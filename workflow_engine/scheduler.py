"""单次运行内部的并发步骤调度器。

职责：
- 依赖感知：只有全部前置 SUCCEEDED 的步骤才会被派发；
- 有界并发：同时 RUNNING 的步骤数不超过锁定的 max_concurrency，满额排队；
- 失败收敛：任一步骤 FAILED 后不再派发新步骤，已在跑的步骤自然跑完，
  最终所有步骤都落在 succeeded/failed/skipped 终态，并返回根因步骤。

终态收敛规则（与串行实现的结论口径一致）：
- 根因步骤 FAILED（error 可追溯到该步骤）；
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
from typing import Any

from .executor import StepExecutor
from .models import (
    RunState,
    StepStatus,
    WorkflowDef,
)

logger = logging.getLogger("workflow_engine")

StepResult = Any


class ConcurrentScheduler:
    """一次运行一个调度器实例（非复用）。"""

    def __init__(
        self,
        *,
        state: RunState,
        executor: StepExecutor,
        results: dict[str, StepResult],
        time_func: Callable[[], float],
    ) -> None:
        self._state = state
        self._definition = state.definition
        self._max_concurrency = state.max_concurrency
        self._executor = executor
        self._results = results
        self._time = time_func

        self._cond = threading.Condition()
        self._ready: list[str] = []          # 等待空位的步骤（按拓扑序排列）
        self._active = 0                     # 当前 RUNNING 步骤数
        self._remaining: dict[str, set[str]] = {}
        self._dependents: dict[str, list[str]] = {}
        self._failed_root: str | None = None
        self._order_index: dict[str, int] = {}

    # ------------------------------------------------------------------ #
    # 对外入口
    # ------------------------------------------------------------------ #

    def run(self) -> str | None:
        """执行整次运行；返回根因失败步骤 id（全部成功时返回 None）。"""
        definition = self._definition
        run_id = self._state.run_id
        order = self._topological_order(definition)
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

        worker_threads: list[threading.Thread] = []
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

        # 调度循环结束时所有 worker 都已登记；join 保证其状态写入对本线程可见
        for thread in worker_threads:
            thread.join()

        self._converge(order)
        return self._failed_root

    # ------------------------------------------------------------------ #
    # 调度
    # ------------------------------------------------------------------ #

    def _dispatch_locked(
        self,
        worker_threads: list[threading.Thread],
        run_id: str,
    ) -> None:
        """在条件锁内：按空位与失败闸门派发尽可能多的就绪步骤。"""
        while self._ready and self._active < self._max_concurrency:
            step_id = self._ready.pop(0)
            self._active += 1
            thread = threading.Thread(
                target=self._run_step,
                args=(step_id,),
                name=f"workflow-step-{run_id}-{step_id}",
                daemon=True,
            )
            worker_threads.append(thread)
            logger.info(
                "run_id=%s step_id=%s reason=step_dispatched active=%d limit=%d",
                run_id, step_id, self._active, self._max_concurrency,
            )
            thread.start()

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

    # ------------------------------------------------------------------ #
    # 终态收敛
    # ------------------------------------------------------------------ #

    def _converge(self, order: list[str]) -> None:
        """所有在跑步骤结束后：按拓扑序给未决步骤确定终态，不留中间态。"""
        root = self._failed_root
        for step_id in order:
            step_state = self._state.steps[step_id]
            if step_state.status in (
                StepStatus.SUCCEEDED,
                StepStatus.FAILED,
                StepStatus.SKIPPED,
            ):
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
            failed_by = blocker or root
            assert failed_by is not None
            step_state.status = StepStatus.SKIPPED
            step_state.failed_by_step = failed_by
            logger.info(
                "run_id=%s step_id=%s reason=step_skipped_on_convergence "
                "failed_by_step=%s",
                self._state.run_id, step_id, failed_by,
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
