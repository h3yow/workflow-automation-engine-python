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
    DEFAULT_MAX_CONCURRENCY,
    ENGINE_FAILURE_SOURCE,
    DuplicateTriggerError,
    EngineDispatchError,
    RunNotFoundError,
    RunState,
    RunStatus,
    StepStatus,
    WorkflowDef,
    new_run_id,
)
from .scheduler import (
    ConcurrentScheduler,
    SchedulerOutcome,
    StepUnitLauncher,
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
    - 每个运行在独立后台线程内调度步骤；不同运行天然并发。
      单个运行内由 ConcurrentScheduler 按依赖关系做有界并发：
      无依赖关系的步骤可同时执行，RUNNING 数量不超过锁定的上限，
      满额则排队等空位。
    - 步骤失败（不可恢复或重试耗尽）后：该步骤 FAILED 且成为根因，
      不再派发任何新步骤；在跑的其它分支自然跑完；最终所有未决步骤
      收敛为 SKIPPED（failed_by_step 可回溯到根因），运行整体 FAILED。
    - 引擎自身在派发/启动执行单元环节出错（准备单元抛错、资源不足
      无法再开新单元等）：与业务失败走同一收敛口径——在跑步骤自然
      跑完，已成功步骤不回退，其余步骤 SKIPPED（failed_by_step 不为空，
      引擎侧断裂点为 "engine:scheduler"，具体位置记在 RunState.error）。
    """

    def __init__(
        self,
        definitions: DefinitionRegistry,
        actions: ActionRegistry,
        sleep_protocol: SleepProtocol | None = None,
        time_func: Callable[[], float] | None = None,
        run_asynchronously: bool = True,
        default_max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        unit_launcher: StepUnitLauncher | None = None,
    ) -> None:
        if isinstance(default_max_concurrency, bool) or not isinstance(
            default_max_concurrency, int
        ) or default_max_concurrency < 1:
            raise ValueError(
                "default_max_concurrency must be an integer >= 1, got "
                f"{default_max_concurrency!r}"
            )
        self._definitions = definitions
        self._executor = StepExecutor(
            actions, sleep_protocol=sleep_protocol, time_func=time_func
        )
        self._time = time_func or time.time
        # run_asynchronously=False 时触发线程内直接跑完（便于测试）
        self._async = run_asynchronously
        # 定义未显式配置并发上限（如历史旧版本）时使用的兼容默认值
        self._default_max_concurrency = default_max_concurrency
        # 执行单元启动器：默认以守护线程承载，可注入故障替身做引擎故障演练
        self._unit_launcher = unit_launcher
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
        # 触发瞬间锁定并发度：定义版本优先；旧定义未配置时用兼容默认值。
        # 之后发布新版本（哪怕并发度不同）不影响本次运行。
        locked_concurrency = (
            snapshot.max_concurrency
            if snapshot.max_concurrency is not None
            else self._default_max_concurrency
        )

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
            state = RunState.new(
                run_id, snapshot, self._time(), max_concurrency=locked_concurrency
            )
            self._runs[run_id] = state
            self._done_events[run_id] = threading.Event()
            self._results[run_id] = {}
            logger.info(
                "run_id=%s reason=run_triggered def_name=%s def_version=%d "
                "steps=%d max_concurrency=%d",
                run_id, snapshot.name, snapshot.version, len(snapshot.steps),
                locked_concurrency,
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
    # 流水线执行（每个运行一个调度线程，内部步骤有界并发）
    # ------------------------------------------------------------------ #

    def _run_pipeline(self, run_id: str) -> None:
        state = self.get_run(run_id)
        try:
            results = self._results[run_id]
            scheduler = ConcurrentScheduler(
                state=state,
                executor=self._executor,
                results=results,
                time_func=self._time,
                unit_launcher=self._unit_launcher,
            )
            outcome = scheduler.run()
            self._apply_outcome(state, outcome)
        except Exception as exc:
            # 最后防线：即便调度器本身意外漏出异常，也不能留下非终态步骤，
            # 且失败原因必须指到可识别位置。
            logger.exception("run_id=%s reason=run_crashed", run_id)
            self._force_engine_failure(
                state,
                EngineDispatchError(
                    f"run pipeline crashed: {type(exc).__name__}: {exc}",
                    location="run_pipeline",
                ),
            )
        finally:
            state.finished_at = state.finished_at or self._time()
            with self._registry_lock:
                event = self._done_events[run_id]
            event.set()
            logger.info(
                "run_id=%s reason=run_finished final_status=%s",
                run_id, state.status.value,
            )

    def _apply_outcome(self, state: RunState, outcome: SchedulerOutcome) -> None:
        """把调度结论落到运行状态；引擎故障优先于业务根因判定运行失败原因。"""
        run_id = state.run_id

        if outcome.engine_failure is not None:
            failure = outcome.engine_failure
            target = failure.step_id
            state.status = RunStatus.FAILED
            state.finished_at = self._time()
            state.error = (
                f"{failure.reason} at {failure.location}"
                + (f" while starting step {target}" if target else "")
                + f": {failure.message}"
            )
            logger.error(
                "run_id=%s reason=run_failed cause=engine location=%s "
                "target_step=%s failed_step_id=%s def_version=%d "
                "max_concurrency=%d detail=%s",
                run_id, failure.location, target or "-",
                outcome.failed_root or "-", state.definition.version,
                state.max_concurrency, failure.message,
            )
        elif outcome.failed_root is not None:
            root = outcome.failed_root
            state.status = RunStatus.FAILED
            state.finished_at = self._time()
            root_error = state.steps[root].error
            state.error = (
                f"step_failure root_step={root}"
                + (f": {root_error}" if root_error else "")
            )
            logger.error(
                "run_id=%s reason=run_failed cause=step failed_step_id=%s "
                "def_version=%d max_concurrency=%d",
                run_id, root, state.definition.version, state.max_concurrency,
            )
        else:
            state.status = RunStatus.SUCCEEDED
            state.finished_at = self._time()
            logger.info(
                "run_id=%s reason=run_succeeded def_version=%d "
                "max_concurrency=%d",
                run_id, state.definition.version, state.max_concurrency,
            )

    def _force_engine_failure(
        self, state: RunState, failure: EngineDispatchError
    ) -> None:
        """异常兜底：把运行判失败并把任何非终态步骤收敛为 SKIPPED。"""
        for step_state in state.steps.values():
            if step_state.status in (
                StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.SKIPPED,
            ):
                continue
            step_def = state.definition.get_step(step_state.step_id)
            blocker = None
            if step_def is not None:
                for dep in step_def.depends_on:
                    dep_state = state.steps.get(dep)
                    if dep_state is not None and dep_state.status in (
                        StepStatus.FAILED, StepStatus.SKIPPED,
                    ):
                        blocker = dep
                        break
            step_state.status = StepStatus.SKIPPED
            step_state.failed_by_step = blocker or ENGINE_FAILURE_SOURCE
            logger.error(
                "run_id=%s step_id=%s reason=step_skipped_crash_net "
                "failed_by_step=%s",
                state.run_id, step_state.step_id, step_state.failed_by_step,
            )
        state.status = RunStatus.FAILED
        state.error = (
            f"{failure.reason} at {failure.location}: {failure.message}"
        )
