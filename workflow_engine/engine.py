"""工作流编排引擎：调度、幂等触发、失败传播、定义快照绑定。"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from .definitions import DefinitionRegistry
from .executor import ActionRegistry, SleepProtocol, StepExecutor
from .models import (
    DuplicateTriggerError,
    RunNotFoundError,
    RunState,
    RunStatus,
    StepDef,
    StepState,
    StepStatus,
    WorkflowDef,
    new_run_id,
)

logger = logging.getLogger("workflow_engine")


class WorkflowEngine:
    """本地工作流自动化执行引擎。

    并发模型与保证：
    - 每个运行绑定 *触发时* 的定义快照，之后发布的新定义不影响该运行；
      新定义只对之后触发的运行生效。
    - 同一 run_id 的重复/并发触发，在全局注册锁内原子完成
      “检查 + 登记”，保证只有一个被接受，其余抛 DuplicateTriggerError，
      任何步骤副作用都不会因重复触发而执行两次。
    - 每个运行在独立后台线程内按依赖顺序调度步骤；不同运行天然并发。
      单个运行内步骤按拓扑序推进，只有全部前置 SUCCEEDED 才执行该步骤。
    - 步骤失败（不可恢复或重试耗尽）后，该步骤标记 FAILED，其全部
      传递下游标记 SKIPPED（并记录失败来源步骤）；运行整体标记 FAILED。
    """

    def __init__(
        self,
        definitions: DefinitionRegistry,
        actions: ActionRegistry,
        sleep_protocol: SleepProtocol | None = None,
        time_func: Callable[[], float] | None = None,
        run_asynchronously: bool = True,
    ) -> None:
        self._definitions = definitions
        self._executor = StepExecutor(
            actions, sleep_protocol=sleep_protocol, time_func=time_func
        )
        self._time = time_func or time.time
        # run_asynchronously=False 时触发线程内直接跑完（便于测试）
        self._async = run_asynchronously
        self._registry_lock = threading.Lock()
        self._runs: dict[str, RunState] = {}
        self._run_threads: dict[str, threading.Thread] = {}
        self._done_events: dict[str, threading.Event] = {}
        self._results: dict[str, dict[str, Any]] = {}
        self._shutdown = False

    @property
    def definitions(self) -> DefinitionRegistry:
        return self._definitions

    # ------------------------------------------------------------------ #
    # 触发 / 查询
    # ------------------------------------------------------------------ #

    def trigger(
        self,
        run_id: str | None = None,
        *,
        definition: WorkflowDef | None = None,
        wait: bool = False,
    ) -> RunState:
        """触发一次新运行。

        - run_id 重复（含并发）时抛 DuplicateTriggerError。
        - definition 为 None 时使用注册表当前版本。
        - wait=True 时阻塞至运行结束。
        """
        run_id = run_id or new_run_id()
        snapshot = definition or self._definitions.current()

        # 原子“检查 + 登记”，杜绝并发重复触发
        with self._registry_lock:
            if self._shutdown:
                raise RuntimeError("engine has been shut down")
            if run_id in self._runs:
                existing = self._runs[run_id]
                logger.warning(
                    "run_id=%s reason=duplicate_trigger_rejected "
                    "existing_status=%s existing_def_version=%d",
                    run_id, existing.status.value, existing.definition.version,
                )
                raise DuplicateTriggerError(run_id)
            state = RunState.new(run_id, snapshot, self._time())
            self._runs[run_id] = state
            self._done_events[run_id] = threading.Event()
            self._results[run_id] = {}
            logger.info(
                "run_id=%s reason=run_triggered def_name=%s def_version=%d steps=%d",
                run_id, snapshot.name, snapshot.version, len(snapshot.steps),
            )
            if self._async and not wait:
                thread = threading.Thread(
                    target=self._run_pipeline,
                    args=(run_id,),
                    name=f"workflow-run-{run_id}",
                    daemon=True,
                )
                self._run_threads[run_id] = thread
                thread.start()

        if not self._async or wait:
            self._run_pipeline(run_id)
        return state

    def get_run(self, run_id: str) -> RunState:
        """查询运行状态；不存在抛 RunNotFoundError。"""
        with self._registry_lock:
            state = self._runs.get(run_id)
        if state is None:
            raise RunNotFoundError(run_id)
        return state

    def wait_until_done(self, run_id: str, timeout: float | None = None) -> RunState:
        """阻塞等待运行进入终态。"""
        with self._registry_lock:
            event = self._done_events.get(run_id)
            state = self._runs.get(run_id)
        if state is None or event is None:
            raise RunNotFoundError(run_id)
        event.wait(timeout=timeout)
        return state

    def shutdown(self, timeout: float | None = None) -> None:
        """等待全部在途运行结束并释放资源。"""
        with self._registry_lock:
            self._shutdown = True
            threads = list(self._run_threads.values())
        for thread in threads:
            thread.join(timeout=timeout)

    # ------------------------------------------------------------------ #
    # 流水线执行（每个运行一个线程）
    # ------------------------------------------------------------------ #

    def _run_pipeline(self, run_id: str) -> None:
        state = self.get_run(run_id)
        definition = state.definition  # 快照：运行期间定义变更与本运行无关
        try:
            # Kahn 拓扑序；定义已校验无环
            order = self._topological_order(definition)
            results = self._results[run_id]
            for step_id in order:
                step_state = state.steps[step_id]
                step_def = definition.get_step(step_id)
                assert step_def is not None

                # 失败传播：若任一前置 FAILED/SKIPPED，则本步骤 SKIPPED
                blocker = self._first_blocker(definition, step_def, state)
                if blocker is not None:
                    self._mark_skipped(step_state, blocker, run_id)
                    continue

                inputs = {
                    dep: results[dep]
                    for dep in step_def.depends_on
                    if dep in results
                }
                status, result = self._executor.execute_step(
                    run_id, definition, step_def, step_state, inputs
                )
                if status is StepStatus.SUCCEEDED:
                    results[step_id] = result
                else:
                    self._finalize_failure(run_id, state, step_id, order)
                    return

            state.status = RunStatus.SUCCEEDED
            state.finished_at = self._time()
            logger.info(
                "run_id=%s reason=run_succeeded def_version=%d",
                run_id, definition.version,
            )
        except Exception:
            logger.exception("run_id=%s reason=run_crashed", run_id)
            state.status = RunStatus.FAILED
            state.finished_at = self._time()
        finally:
            state.finished_at = state.finished_at or self._time()
            with self._registry_lock:
                event = self._done_events[run_id]
            event.set()
            logger.info(
                "run_id=%s reason=run_finished final_status=%s",
                run_id, state.status.value,
            )

    def _finalize_failure(
        self,
        run_id: str,
        state: RunState,
        failed_step_id: str,
        order: list[str],
    ) -> None:
        """失败步骤之后的所有未完成步骤标记 SKIPPED，并收敛运行终态。"""
        failed_index = order.index(failed_step_id)
        for downstream_id in order[failed_index + 1:]:
            downstream_state = state.steps[downstream_id]
            if downstream_state.status not in (
                StepStatus.SUCCEEDED,
                StepStatus.FAILED,
                StepStatus.SKIPPED,
            ):
                self._mark_skipped(downstream_state, failed_step_id, run_id)
        state.status = RunStatus.FAILED
        state.finished_at = self._time()
        logger.error(
            "run_id=%s reason=run_failed failed_step_id=%s",
            run_id, failed_step_id,
        )

    @staticmethod
    def _mark_skipped(
        step_state: StepState, failed_by_step: str, run_id: str
    ) -> None:
        step_state.status = StepStatus.SKIPPED
        step_state.failed_by_step = failed_by_step
        logger.info(
            "run_id=%s step_id=%s reason=step_skipped failed_by_step=%s",
            run_id, step_state.step_id, failed_by_step,
        )

    @staticmethod
    def _first_blocker(
        definition: WorkflowDef,
        step_def: StepDef,
        state: RunState,
    ) -> str | None:
        """返回导致本步骤不可推进的前置步骤 id（前置失败/被跳过）。"""
        for dep in step_def.depends_on:
            dep_status = state.steps[dep].status
            if dep_status in (StepStatus.FAILED, StepStatus.SKIPPED):
                return dep
        return None

    @staticmethod
    def _topological_order(definition: WorkflowDef) -> list[str]:
        """Kahn 算法；同层按定义顺序，保证输出稳定可预期。"""
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
        # 定义已通过无环校验，这里必然覆盖全部步骤
        return order
