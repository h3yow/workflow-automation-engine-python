"""单次运行内部的并发步骤调度器。

职责：
- 依赖感知：只有全部前置 SUCCEEDED 的步骤才会被派发；
- 有界并发：同时 RUNNING 的步骤数不超过锁定的 max_concurrency，满额排队；
- 失败收敛：任一步骤 FAILED 后不再派发新步骤，已在跑的步骤自然跑完，
  最终所有步骤都落在 succeeded/failed/skipped 终态，并返回根因步骤。
- 引擎侧故障收敛：派发/启动执行单元本身出错（启动器抛错、资源不足开不出
  新执行单元等，属于引擎故障而非业务动作失败）时，按与业务失败一致的规则
  收敛——被派发的步骤记 FAILED（error 以 engine_dispatch_failure 前缀可
  识别），失败闸门立即关闭，已在跑的分支自然跑完，其余未开始步骤 SKIPPED
  且 failed_by_step 可回溯到该步骤，不残留 pending/ready 中间态。

终态收敛规则（与串行实现的结论口径一致）：
- 根因步骤 FAILED（error 可追溯到该步骤；引擎派发/启动失败时 error 带
  engine_dispatch_failure 前缀，与业务失败可区分）；
- 依赖链上尚未开始的下游 SKIPPED，failed_by_step 取其第一个
  FAILED/SKIPPED 的直接前置，可逐级回溯到根因；
- 失败发生时已经在跑的其它分支：让其自然跑完，成功仍记 SUCCEEDED
  （失败前的成功不回退），自身也失败则记 FAILED；
- 已排队/仍 pending、与根因无依赖关系的步骤不再派发，统一 SKIPPED，
  failed_by_step 直接记根因步骤（断裂点明确）。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any, Protocol

from .executor import StepExecutor
from .models import (
    STEP_TERMINAL_STATUSES,
    EngineDispatchError,
    RunState,
    StepStatus,
    WorkflowDef,
)

logger = logging.getLogger("workflow_engine")

StepResult = Any


class StepHandle(Protocol):
    """已启动执行单元的句柄：只需支持等待其结束。"""

    def join(self) -> None: ...


class StepLauncher(Protocol):
    """执行单元启动器（派发接缝）：把一个步骤真正“开”起来。

    抽成接缝是为了隔离引擎自身在派发/启动环节的故障（如资源不足无法
    创建新执行单元、启动器抛错），也便于测试中人为注入该类故障。
    """

    def launch(
        self,
        *,
        run_id: str,
        step_id: str,
        target: Callable[[str], None],
    ) -> StepHandle: ...


class ThreadStepLauncher:
    """默认启动器：为每个步骤创建并启动一个守护线程作为执行单元。"""

    def launch(
        self,
        *,
        run_id: str,
        step_id: str,
        target: Callable[[str], None],
    ) -> threading.Thread:
        thread = threading.Thread(
            target=target,
            args=(step_id,),
            name=f"workflow-step-{run_id}-{step_id}",
            daemon=True,
        )
        thread.start()
        return thread


def topological_order(definition: WorkflowDef) -> list[str]:
    """稳定拓扑序（同层按定义声明顺序）。"""
    remaining_deps = {
        step.id: set(step.depends_on) for step in definition.steps
    }
    dependents: dict[str, list[str]] = {step.id: [] for step in definition.steps}
    for step in definition.steps:
        for dep in step.depends_on:
            dependents[dep].append(step.id)

    ready = [s.id for s in definition.steps if not remaining_deps[s.id]]
    order: list[str] = []
    while ready:
        step_id = ready.pop(0)
        order.append(step_id)
        for downstream in dependents[step_id]:
            remaining_deps[downstream].discard(step_id)
            if not remaining_deps[downstream]:
                ready.append(downstream)
    return order


def converge_remaining_steps(
    state: RunState,
    order: list[str],
    root: str,
) -> None:
    """按拓扑序把所有未决步骤收敛为 SKIPPED，failed_by_step 可回溯到根因。

    已在终态（SUCCEEDED/FAILED/SKIPPED）的步骤保持不动——成功不回退、
    失败不重标。业务失败收敛与引擎侧故障收敛共用本函数，规则一致。
    """
    for step_id in order:
        step_state = state.steps[step_id]
        if step_state.status in STEP_TERMINAL_STATUSES:
            continue
        # PENDING/READY/RUNNING 统一收敛（RUNNING 理论上已 join 完）
        step_def = state.definition.get_step(step_id)
        assert step_def is not None
        blocker = None
        for dep in step_def.depends_on:
            dep_status = state.steps[dep].status
            if dep_status in (StepStatus.FAILED, StepStatus.SKIPPED):
                blocker = dep
                break
        failed_by = blocker or root
        step_state.status = StepStatus.SKIPPED
        step_state.failed_by_step = failed_by
        logger.info(
            "run_id=%s step_id=%s reason=step_skipped_on_convergence "
            "failed_by_step=%s",
            state.run_id, step_id, failed_by,
        )


def force_terminate_after_engine_crash(
    state: RunState,
    time_func: Callable[[], float],
    exc: BaseException,
) -> str | None:
    """引擎兜底：调度器自身异常逃逸后，仍保证每个步骤落到明确终态。

    优先复用已登记的 FAILED 步骤作为根因；否则取拓扑序中第一个未决步骤，
    将其判为引擎派发故障（error 带 engine_dispatch_failure 前缀），其余
    未决步骤按统一规则 SKIPPED。返回根因步骤 id（全部已终态时为 None）。
    """
    order = topological_order(state.definition)
    root: str | None = next(
        (sid for sid in order if state.steps[sid].status is StepStatus.FAILED),
        None,
    )
    if root is None:
        for step_id in order:
            step_state = state.steps[step_id]
            if step_state.status not in STEP_TERMINAL_STATUSES:
                step_state.status = StepStatus.FAILED
                step_state.error = (
                    f"{EngineDispatchError.reason}: engine scheduler crashed "
                    f"before step was started "
                    f"({type(exc).__name__}: {exc})"
                )
                step_state.finished_at = time_func()
                logger.error(
                    "run_id=%s step_id=%s reason=engine_crash_root detail=%s",
                    state.run_id, step_id, exc,
                )
                root = step_id
                break
    if root is not None:
        converge_remaining_steps(state, order, root)
    return root


class ConcurrentScheduler:
    """一次运行一个调度器实例（非复用）。"""

    def __init__(
        self,
        *,
        state: RunState,
        executor: StepExecutor,
        results: dict[str, StepResult],
        time_func: Callable[[], float],
        step_launcher: StepLauncher | None = None,
    ) -> None:
        self._state = state
        self._definition = state.definition
        self._max_concurrency = state.max_concurrency
        self._executor = executor
        self._results = results
        self._time = time_func
        self._launcher = step_launcher or ThreadStepLauncher()

        self._cond = threading.Condition()
        self._ready: list[str] = []          # 等待空位的步骤（按拓扑序排列）
        self._active = 0                     # 当前 RUNNING 步骤数
        self._remaining: dict[str, set[str]] = {}
        self._dependents: dict[str, list[str]] = {}
        self._failed_root: str | None = None
        self._order: list[str] = []
        self._order_index: dict[str, int] = {}

    # ------------------------------------------------------------------ #
    # 对外入口
    # ------------------------------------------------------------------ #

    def run(self) -> str | None:
        """执行整次运行；返回根因失败步骤 id（全部成功时返回 None）。

        任何引擎自身的意外（含派发/启动失败之外的调度故障）都不会逃逸出
        本方法：先让已在跑的执行单元自然结束，再兜底收敛全部未决步骤，
        保证每个步骤都落在明确终态。
        """
        worker_threads: list[StepHandle] = []
        try:
            self._run_loop(worker_threads)
        except Exception as exc:
            # 调度循环本身出岔子（理论上启动失败已在 _dispatch 内被消化，
            # 这里覆盖一切引擎侧意外，确保不向编排层泄漏悬空运行）：
            # 先等已启动的执行单元跑完，再兜底收敛。
            logger.exception(
                "run_id=%s reason=scheduler_crash", self._state.run_id
            )
            for thread in worker_threads:
                thread.join()
            root = force_terminate_after_engine_crash(
                self._state, self._time, exc
            )
            self._failed_root = root
            return root

        # 调度循环结束时所有 worker 都已登记；join 保证其状态写入对本线程可见
        for thread in worker_threads:
            thread.join()
        if self._failed_root is not None:
            converge_remaining_steps(
                self._state, self._order, self._failed_root
            )
        return self._failed_root

    def _run_loop(self, worker_threads: list[StepHandle]) -> str | None:
        """调度主循环：依赖感知派发 + 有界并发 + 失败闸门。"""
        definition = self._definition
        run_id = self._state.run_id
        order = topological_order(definition)
        self._order = order
        self._order_index = {step_id: i for i, step_id in enumerate(order)}

        self._remaining = {
            step.id: set(step.depends_on) for step in definition.steps
        }
        self._dependents = {step.id: [] for step in definition.steps}
        for step in definition.steps:
            for dep in step.depends_on:
                self._dependents[dep].append(step.id)

        # 无前置的步骤首批入队
        for step_id in order:
            if not self._remaining[step_id]:
                self._enqueue(step_id)

        with self._cond:
            while True:
                # 失败闸门未落下时，尽量用满并发名额派发
                if self._failed_root is None:
                    self._dispatch_locked(worker_threads, run_id)
                # 全部在跑步骤已结束：成功且队列空 -> 完成；已失败 -> 收尾收敛
                if self._active == 0 and (
                    self._failed_root is not None or not self._ready
                ):
                    break
                # 否则等待任一步骤完成的通知（Condition.wait 也容忍虚假唤醒）
                self._cond.wait()
        return self._failed_root

    # ------------------------------------------------------------------ #
    # 调度
    # ------------------------------------------------------------------ #

    def _dispatch_locked(
        self,
        worker_threads: list[StepHandle],
        run_id: str,
    ) -> None:
        """在条件锁内：按空位与失败闸门派发尽可能多的就绪步骤。

        执行单元的创建/启动经 StepLauncher 接缝完成；启动器自身抛错
        （资源不足、无法创建执行单元等引擎侧故障）时，不允许逃逸出调度
        循环——立即把该步骤判为 FAILED（error 带 engine_dispatch_failure
        前缀），并落下失败闸门停止继续派发，后续按统一规则收敛。
        """
        while self._ready and self._active < self._max_concurrency:
            if self._failed_root is not None:
                return
            step_id = self._ready.pop(0)
            self._active += 1
            try:
                handle = self._launcher.launch(
                    run_id=run_id,
                    step_id=step_id,
                    target=self._run_step,
                )
            except Exception as exc:  # noqa: BLE001 - 启动器可能抛任意引擎侧故障，统一收敛
                self._record_launch_failure_locked(step_id, exc)
                return
            worker_threads.append(handle)
            logger.info(
                "run_id=%s step_id=%s reason=step_dispatched active=%d limit=%d",
                run_id, step_id, self._active, self._max_concurrency,
            )

    def _record_launch_failure_locked(
        self, failed_id: str, exc: Exception
    ) -> None:
        """引擎在派发/启动执行单元时出错：名额回滚、该步骤记根因失败。

        与业务步骤失败使用同一套闸门与收敛规则，区别仅在 error 前缀
        （engine_dispatch_failure），使故障点可识别、可与业务失败区分。
        """
        self._active -= 1
        step_state = self._state.steps[failed_id]
        step_state.status = StepStatus.FAILED
        step_state.error = (
            f"{EngineDispatchError.reason}: could not launch execution unit "
            f"({type(exc).__name__}: {exc})"
        )
        step_state.finished_at = self._time()
        self._record_failure_locked(failed_id)
        logger.error(
            "run_id=%s step_id=%s reason=engine_dispatch_failed detail=%s",
            self._state.run_id, failed_id, exc,
        )

    def _enqueue(self, step_id: str) -> None:
        """标记 READY 并按拓扑顺序插入等待队列。"""
        step_state = self._state.steps[step_id]
        step_state.status = StepStatus.READY
        self._ready.append(step_id)
        self._ready.sort(key=lambda sid: self._order_index[sid])

    def _run_step(self, step_id: str) -> None:
        """工作线程：执行单个步骤（含重试），完成后推进依赖图并唤醒调度。"""
        run_id = self._state.run_id
        definition = self._definition
        step_def = definition.get_step(step_id)
        assert step_def is not None
        step_state = self._state.steps[step_id]

        inputs = {
            dep: self._results[dep]
            for dep in step_def.depends_on
        }

        try:
            status, result = self._executor.execute_step(
                run_id, definition, step_def, step_state, inputs
            )
        except Exception as exc:  # 执行器之外的意外：收敛为该步骤失败，不留悬空
            logger.exception(
                "run_id=%s step_id=%s reason=step_execution_crashed",
                run_id, step_id,
            )
            status = StepStatus.FAILED
            result = None
            step_state.status = StepStatus.FAILED
            step_state.error = f"fatal_step_failure: unexpected {type(exc).__name__}: {exc}"
            step_state.finished_at = self._time()

        with self._cond:
            self._active -= 1
            if status is StepStatus.SUCCEEDED:
                # results 只在本线程完成时写入一次；下游被唤醒时读取
                self._results[step_id] = result
                self._promote_locked(step_id)
            else:
                self._record_failure_locked(step_id)
            self._cond.notify_all()

    def _promote_locked(self, completed_id: str) -> None:
        """成功步骤的下游若已满足全部前置，加入就绪队列。"""
        for downstream_id in self._dependents[completed_id]:
            remaining = self._remaining[downstream_id]
            remaining.discard(completed_id)
            if not remaining and self._state.steps[downstream_id].status in (
                StepStatus.PENDING,
            ):
                self._enqueue(downstream_id)

    def _record_failure_locked(self, failed_id: str) -> None:
        """登记失败；根因取所有失败步骤中拓扑序最靠前者，保证并发下可重复。

        失败派发闸门只看“是否已出现失败”（首个失败即刻关闭），
        根因归属则按拓扑序确定性选择，多个步骤同时失败时结论仍稳定。
        """
        if self._failed_root is None:
            self._failed_root = failed_id
        elif self._order_index[failed_id] < self._order_index[self._failed_root]:
            self._failed_root = failed_id
        if self._failed_root == failed_id:
            logger.error(
                "run_id=%s step_id=%s reason=run_failure_root",
                self._state.run_id, failed_id,
            )
