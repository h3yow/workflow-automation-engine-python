"""单次运行内部的并发步骤调度器。

职责：
- 依赖感知：只有全部前置 SUCCEEDED 的步骤才会被派发；
- 有界并发：同时 RUNNING 的步骤数不超过锁定的 max_concurrency，满额排队；
- 失败收敛：任一步骤 FAILED 后不再派发新步骤，已在跑的步骤自然跑完，
  最终所有步骤都落在 succeeded/failed/skipped 终态，并返回根因步骤；
- 引擎故障收敛：派发/启动执行单元环节自身出错（准备单元抛错、资源不足
  无法再开新单元等）与业务步骤失败走同一收敛口径——失败闸门关闭、
  在跑步骤自然跑完、其余步骤 SKIPPED（failed_by_step 不允许为空），
  运行整体 FAILED 且 RunState.error 指到可识别的出错位置。

终态收敛规则（与串行实现的结论口径一致）：
- 业务失败：根因步骤 FAILED（error 可追溯到该步骤）；
- 引擎故障：没有业务根因步骤，被跳过步骤的断裂来源统一记
  ENGINE_FAILURE_SOURCE（"engine:scheduler"），具体出错位置由
  RunState.error 的 reason/location 给出；当时正准备启动的步骤同样
  SKIPPED（它没有机会执行，不算业务失败）；
- 依赖链上尚未开始的下游 SKIPPED，failed_by_step 取其第一个
  FAILED/SKIPPED 的直接前置，可逐级回溯到断裂点；
- 失败发生时已经在跑的其它分支：让其自然跑完，成功仍记 SUCCEEDED
  （失败前的成功不回退），自身也失败则记 FAILED；
- 已排队/仍 pending、与断裂点无依赖关系的步骤不再派发，统一 SKIPPED，
  failed_by_step 直接记断裂来源（业务根因步骤或 engine:scheduler）。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from .executor import StepExecutor
from .models import (
    ENGINE_FAILURE_SOURCE,
    STEP_TERMINAL_STATUSES,
    EngineDispatchError,
    RunState,
    StepStatus,
    WorkflowDef,
)

logger = logging.getLogger("workflow_engine")

StepResult = Any


class StepUnitLauncher(Protocol):
    """执行单元启动器：把一个工作函数启动为独立执行单元并返回其句柄。

    抽成协议是为了把“派发/启动执行单元”这一引擎侧环节与业务动作执行
    区分开：该环节自身抛错（资源不足、无法再开新单元等）属于引擎故障，
    必须能被注入复现并按引擎故障口径收敛。
    """

    def launch(
        self,
        *,
        name: str,
        target: Callable[..., Any],
        args: tuple[Any, ...],
    ) -> threading.Thread:
        """启动执行单元；启动失败必须抛异常（单元未运行，无名额需回收）。"""
        ...


class ThreadStepUnitLauncher:
    """默认实现：以守护线程承载每个步骤执行单元。"""

    def launch(
        self,
        *,
        name: str,
        target: Callable[..., Any],
        args: tuple[Any, ...],
    ) -> threading.Thread:
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        try:
            thread.start()
        except Exception as exc:
            # start 失败意味着执行单元从未运行，调用方不会占活跃名额。
            step_id = args[0] if args else None
            raise EngineDispatchError(
                f"failed to start step execution unit: "
                f"{type(exc).__name__}: {exc}",
                location="step_unit_launch",
                step_id=step_id if isinstance(step_id, str) else None,
            ) from exc
        return thread


@dataclass(frozen=True)
class SchedulerOutcome:
    """调度结论：业务失败根因与引擎故障互斥地至多一个非空。"""

    failed_root: str | None = None
    engine_failure: EngineDispatchError | None = None


class ConcurrentScheduler:
    """一次运行一个调度器实例（非复用）。"""

    def __init__(
        self,
        *,
        state: RunState,
        executor: StepExecutor,
        results: dict[str, StepResult],
        time_func: Callable[[], float],
        unit_launcher: StepUnitLauncher | None = None,
    ) -> None:
        self._state = state
        self._definition = state.definition
        self._max_concurrency = state.max_concurrency
        self._executor = executor
        self._results = results
        self._time = time_func
        self._unit_launcher = unit_launcher or ThreadStepUnitLauncher()

        self._cond = threading.Condition()
        self._ready: list[str] = []          # 等待空位的步骤（按拓扑序排列）
        self._active = 0                     # 当前 RUNNING 步骤数
        self._remaining: dict[str, set[str]] = {}
        self._dependents: dict[str, list[str]] = {}
        self._failed_root: str | None = None
        self._engine_failure: EngineDispatchError | None = None
        self._order: list[str] = []
        self._order_index: dict[str, int] = {}
        # 已登记的 worker 句柄：兜底收敛需要访问（join 防止悬空）
        self._worker_threads: list[threading.Thread] = []

    # ------------------------------------------------------------------ #
    # 对外入口
    # ------------------------------------------------------------------ #

    def run(self) -> SchedulerOutcome:
        """执行整次运行；返回调度结论（业务根因 / 引擎故障），保证终态收敛。

        正常返回时所有步骤必然落在 succeeded/failed/skipped 终态；
        即便调度循环自身意外出错，也会走兜底收敛，绝不让异常带着
        停在 pending/ready/running 的步骤漏出调度器。
        """
        definition = self._definition
        run_id = self._state.run_id
        order = self._topological_order(definition)
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

        try:
            with self._cond:
                while True:
                    # 失败闸门（业务失败或引擎故障）未落下时，尽量用满名额派发
                    if self._failed_root is None and self._engine_failure is None:
                        self._dispatch_locked(run_id)
                    # 全部在跑步骤已结束：成功且队列空 -> 完成；已失败 -> 收尾收敛
                    if self._active == 0 and (
                        self._failed_root is not None
                        or self._engine_failure is not None
                        or not self._ready
                    ):
                        break
                    # 否则等待任一步骤完成的通知（Condition.wait 也容忍虚假唤醒）
                    self._cond.wait()

            # 调度循环结束时所有 worker 都已登记；join 保证其状态写入对本线程可见
            for thread in self._worker_threads:
                thread.join()
        except Exception:
            # 兜底：调度线程自身在任何环节意外出错，也先让在跑步骤收尾，
            # 再把所有中间态步骤收敛，绝不让异常带悬空步骤漏出调度器。
            logger.exception("run_id=%s reason=scheduler_crashed", run_id)
            self._emergency_converge()
            if self._engine_failure is None:
                self._engine_failure = EngineDispatchError(
                    "scheduler crashed unexpectedly",
                    location="scheduler_loop",
                )
            return SchedulerOutcome(
                failed_root=self._failed_root,
                engine_failure=self._engine_failure,
            )

        self._converge(order)
        return SchedulerOutcome(
            failed_root=self._failed_root,
            engine_failure=self._engine_failure,
        )

    # ------------------------------------------------------------------ #
    # 调度
    # ------------------------------------------------------------------ #

    def _dispatch_locked(self, run_id: str) -> None:
        """在条件锁内：按空位与失败闸门派发尽可能多的就绪步骤。

        执行单元启动失败（引擎侧故障）按引擎故障收敛：失败闸门立即落下，
        活跃名额不增加（单元从未运行，无名额需要回收），当时目标步骤仍在
        就绪队列中，随后由收敛阶段统一判 SKIPPED。
        """
        while self._ready and self._active < self._max_concurrency:
            step_id = self._ready.pop(0)
            try:
                thread = self._unit_launcher.launch(
                    name=f"workflow-step-{run_id}-{step_id}",
                    target=self._run_step,
                    args=(step_id,),
                )
            except Exception as exc:  # noqa: BLE001 - 启动器任何异常都按引擎故障收敛
                failure = exc if isinstance(exc, EngineDispatchError) else (
                    EngineDispatchError(
                        f"failed to launch step execution unit: "
                        f"{type(exc).__name__}: {exc}",
                        location="step_unit_launch",
                        step_id=step_id,
                    )
                )
                self._record_engine_failure_locked(failure, target_step=step_id)
                # 本批派发中止；闸门已落下，外层不会再进入派发
                return
            self._active += 1
            self._worker_threads.append(thread)
            logger.info(
                "run_id=%s step_id=%s reason=step_dispatched active=%d limit=%d",
                run_id, step_id, self._active, self._max_concurrency,
            )

    def _enqueue(self, step_id: str) -> None:
        """标记 READY 并按拓扑顺序插入等待队列。"""
        step_state = self._state.steps[step_id]
        step_state.status = StepStatus.READY
        self._ready.append(step_id)
        self._ready.sort(key=self._topo_index)

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
            try:
                status, result = self._executor.execute_step(
                    run_id, definition, step_def, step_state, inputs
                )
            except Exception as exc:
                # 执行器之外的意外：收敛为该步骤失败，不留悬空
                logger.exception(
                    "run_id=%s step_id=%s reason=step_execution_crashed",
                    run_id, step_id,
                )
                status = StepStatus.FAILED
                result = None
                step_state.status = StepStatus.FAILED
                step_state.error = (
                    f"fatal_step_failure: unexpected {type(exc).__name__}: {exc}"
                )
                step_state.finished_at = self._time()

            with self._cond:
                if status is StepStatus.SUCCEEDED:
                    # results 只在本线程完成时写入一次；下游被唤醒时读取
                    self._results[step_id] = result
                    self._promote_locked(step_id)
                else:
                    self._record_failure_locked(step_id)
        finally:
            # 无论上面是否再出意外，名额回收与唤醒必须发生，否则调度会永久挂起
            with self._cond:
                self._active -= 1
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
        if self._failed_root is None or self._order_index[failed_id] < self._order_index[self._failed_root]:
            self._failed_root = failed_id
        if self._failed_root == failed_id:
            logger.error(
                "run_id=%s step_id=%s reason=run_failure_root",
                self._state.run_id, failed_id,
            )

    def _record_engine_failure_locked(
        self,
        failure: EngineDispatchError,
        *,
        target_step: str | None,
    ) -> None:
        """登记引擎侧派发/启动故障：落下失败闸门，不再启动任何新执行单元。

        引擎故障与业务失败互相独立记录；同一运行中首个故障被保留。
        """
        self._engine_failure = failure
        logger.error(
            "run_id=%s reason=run_engine_failure location=%s target_step=%s "
            "detail=%s",
            self._state.run_id, failure.location,
            target_step if target_step is not None else "-", failure.message,
        )

    # ------------------------------------------------------------------ #
    # 终态收敛
    # ------------------------------------------------------------------ #

    def _converge(self, order: list[str]) -> None:
        """所有在跑步骤结束后：按拓扑序给未决步骤确定终态，不留中间态。

        断裂来源：有业务根因步骤时指向该步骤；纯引擎故障时为
        ENGINE_FAILURE_SOURCE（"engine:scheduler"）。每个被跳过步骤都必须
        落到一个非空的 failed_by_step，追溯链不断。
        """
        fallback_source = self._failure_source()
        for step_id in order:
            step_state = self._state.steps[step_id]
            if step_state.status in STEP_TERMINAL_STATUSES:
                continue
            # PENDING/READY/RUNNING（理论上 RUNNING 已 join 完）统一收敛
            step_def = self._definition.get_step(step_id)
            assert step_def is not None
            blocker = None
            for dep in step_def.depends_on:
                dep_status = self._state.steps[dep].status
                if dep_status in (StepStatus.FAILED, StepStatus.SKIPPED):
                    blocker = dep
                    break
            failed_by = blocker or fallback_source
            assert failed_by is not None
            step_state.status = StepStatus.SKIPPED
            step_state.failed_by_step = failed_by
            logger.info(
                "run_id=%s step_id=%s reason=step_skipped_on_convergence "
                "failed_by_step=%s",
                self._state.run_id, step_id, failed_by,
            )

    def _failure_source(self) -> str:
        """收敛时的断裂来源：业务根因步骤优先，否则引擎故障来源。"""
        if self._failed_root is not None:
            return self._failed_root
        return ENGINE_FAILURE_SOURCE

    def _emergency_converge(self) -> None:
        """调度线程自身意外时的兜底收敛：绝不留下非终态步骤。

        尽力 join 已登记 worker（它们的 try/finally 保证自身名额回收与
        唤醒不会丢），随后把所有仍处中间态的步骤判为 SKIPPED，
        failed_by_step 指向业务根因或引擎故障来源。
        """
        for thread in self._worker_threads:
            try:
                thread.join()
            except Exception:
                logger.exception(
                    "run_id=%s reason=emergency_join_failed thread=%s",
                    self._state.run_id, thread.name,
                )
        for step_id in self._order or [
            step.id for step in self._definition.steps
        ]:
            step_state = self._state.steps[step_id]
            if step_state.status in STEP_TERMINAL_STATUSES:
                continue
            step_def = self._definition.get_step(step_id)
            blocker = None
            if step_def is not None:
                for dep in step_def.depends_on:
                    dep_state = self._state.steps.get(dep)
                    if dep_state is not None and dep_state.status in (
                        StepStatus.FAILED, StepStatus.SKIPPED,
                    ):
                        blocker = dep
                        break
            step_state.status = StepStatus.SKIPPED
            step_state.failed_by_step = blocker or ENGINE_FAILURE_SOURCE
            logger.error(
                "run_id=%s step_id=%s reason=step_skipped_emergency "
                "failed_by_step=%s",
                self._state.run_id, step_id, step_state.failed_by_step,
            )

    # ------------------------------------------------------------------ #
    # 拓扑序（稳定：同层按定义声明顺序）
    # ------------------------------------------------------------------ #

    @staticmethod
    def _topological_order(definition: WorkflowDef) -> list[str]:
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

    def _topo_index(self, step_id: str) -> int:
        return self._order_index[step_id]
