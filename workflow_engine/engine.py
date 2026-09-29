"""工作流编排引擎：调度、幂等触发、失败传播、定义快照绑定。"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .definitions import DefinitionRegistry
from .executor import ActionRegistry, SleepProtocol, StepExecutor
from .models import (
    STEP_TERMINAL_STATUSES,
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
      新定义只对之后触发的运行生效。并发上限同样随定义版本走，并在触发
      瞬间解析锁定到 RunState.max_concurrency（未配置时使用
      DEFAULT_MAX_CONCURRENCY，保证旧版本向后兼容）。
    - 同一 run_id 的重复/并发触发，在全局注册锁内原子完成
      “检查 + 登记”，保证只有一个被接受，其余抛 DuplicateTriggerError，
      任何步骤副作用都不会因重复触发而执行两次。
    - 每个运行在独立后台线程内调度步骤；不同运行天然并发。
      **单个运行内部也并发**：彼此无依赖关系的步骤可同时执行，同时在跑
      的步骤数不超过该运行锁定的并发上限，满了就等待空位；有依赖关系的
      步骤必须等全部前置 SUCCEEDED 后才开始。同层候选按定义稳定顺序
      （Kahn 拓扑序）填充空位。
    - 任一步骤失败（不可恢复或重试耗尽）后：该步骤 FAILED，运行以 FAILED
      收尾且不再开新步骤；失败前已 SUCCEEDED 的步骤保持成功不回退；
      失败时已在跑的无关分支允许跑到终态（规则一致：成功即 SUCCEEDED，
      自身失败即 FAILED）；最终仍未开始/未完成的步骤一律 SKIPPED，
      failed_by_step 指向阻断链上首个未成功的前置，可一路回溯到根因步骤。
      收敛结束时不存在 PENDING/READY/RUNNING 的悬空步骤。
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
                "run_id=%s reason=run_triggered def_name=%s def_version=%d steps=%d "
                "max_concurrency=%d",
                run_id, snapshot.name, snapshot.version, len(snapshot.steps),
                state.max_concurrency,
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
    # 流水线执行（每个运行一个调度线程，步骤在线程池内并发）
    # ------------------------------------------------------------------ #

    def _run_pipeline(self, run_id: str) -> None:
        state = self.get_run(run_id)
        definition = state.definition  # 快照：运行期间定义变更与本运行无关
        max_concurrency = state.max_concurrency  # 触发时锁定，运行期间不变
        condition = threading.Condition()
        results = self._results[run_id]
        order = self._topological_order(definition)

        # 调度账（仅在 condition 锁内访问）：
        # remaining_deps[step] = 尚未成功的前置数量
        # dispatched = 已提交给线程池的步骤；in_flight = 已提交但未到终态的步骤
        remaining_deps = {
            step.id: len(step.depends_on) for step in definition.steps
        }
        dependents = self._build_dependents(definition)
        dispatched: set[str] = set()
        in_flight: set[str] = set()
        failure_seen = False  # 失败屏障：一旦置位，调度器不再新开步骤
        crashed = False

        pool = ThreadPoolExecutor(
            max_workers=max_concurrency,
            thread_name_prefix=f"wf-step-{run_id}",
        )

        def _ready_undispatched() -> list[str]:
            """稳定顺序（拓扑序）返回依赖已全部成功、尚未提交的步骤候选。

            注意：不在此判断并发空位。调用方必须先按当前 in_flight 快照算出
            空位数量，再从候选头部截取——否则若一边提交一边对正在增长的
            in_flight 求长度，会突破并发上限。
            """
            return [
                sid
                for sid in order
                if sid not in dispatched and remaining_deps[sid] == 0
            ]

        def _worker(step_id: str) -> None:
            """在池线程内执行单个步骤，结束后回写调度账并唤醒调度线程。"""
            nonlocal failure_seen
            step_state = state.steps[step_id]
            step_def = definition.get_step(step_id)
            assert step_def is not None
            try:
                # 前置成功的产物必然已写入（依赖计数归零保证）
                inputs = {
                    dep: results[dep]
                    for dep in step_def.depends_on
                    if dep in results
                }
                status, result = self._executor.execute_step(
                    run_id, definition, step_def, step_state, inputs
                )
            except Exception as exc:  # 兜底：动作/执行器自身崩溃也要落到终态
                logger.exception(
                    "run_id=%s step_id=%s reason=step_execution_crashed",
                    run_id, step_id,
                )
                step_state.status = StepStatus.FAILED
                step_state.error = f"unexpected {type(exc).__name__}: {exc}"
                step_state.finished_at = self._time()
                status = StepStatus.FAILED
                result = None

            with condition:
                in_flight.discard(step_id)
                if status is StepStatus.SUCCEEDED:
                    results[step_id] = result
                    for downstream in dependents[step_id]:
                        remaining_deps[downstream] -= 1
                else:
                    failure_seen = True
                condition.notify_all()

        try:
            with condition:
                while True:
                    # 1) 按当前快照一次性算出空位数，再从稳定候选里取头部，
                    #    提交恰好 min(空位数, 候选数) 个，绝不超过并发上限。
                    #    失败屏障开启后空位数按 0 处理，不再新开任何步骤。
                    slots = (
                        max(0, max_concurrency - len(in_flight))
                        if not failure_seen
                        else 0
                    )
                    candidates = _ready_undispatched()[:slots]
                    for step_id in candidates:
                        dispatched.add(step_id)
                        in_flight.add(step_id)
                        pool.submit(_worker, step_id)

                    # 2) 终止判定：所有已提交步骤都结束，且没有可再新开的步骤
                    if not in_flight:
                        break
                    # 在跑或等待空位/依赖：阻塞到某个 worker 结束被唤醒
                    condition.wait()

            # 3) 收敛：所有在跑分支均已到终态，未决步骤统一 SKIPPED。
            #    在锁外做状态标记（标记过程不依赖调度账）。
            if failure_seen:
                root_failed = self._root_failed_step(state, order)
                self._converge_skip_remaining(
                    run_id, state, definition, order, root_failed
                )
                state.status = RunStatus.FAILED
                logger.error(
                    "run_id=%s reason=run_failed max_concurrency=%d "
                    "failed_steps=%s",
                    run_id, max_concurrency,
                    sorted(
                        sid for sid, s in state.steps.items()
                        if s.status is StepStatus.FAILED
                    ),
                )
            else:
                state.status = RunStatus.SUCCEEDED
                logger.info(
                    "run_id=%s reason=run_succeeded def_version=%d "
                    "max_concurrency=%d",
                    run_id, definition.version, max_concurrency,
                )
            state.finished_at = self._time()
        except Exception:
            # 理论不可达：worker 已自行兜底；这里再保证不留悬空步骤
            logger.exception("run_id=%s reason=run_crashed", run_id)
            crashed = True
        finally:
            pool.shutdown(wait=True)
            if crashed:  # 调度器自身崩溃：把一切未决步骤强制收敛到 SKIPPED
                root_failed = self._root_failed_step(state, order)
                self._converge_skip_remaining(
                    run_id, state, definition, order, root_failed
                )
                state.status = RunStatus.FAILED
                state.finished_at = self._time()
            with self._registry_lock:
                event = self._done_events[run_id]
            event.set()
            logger.info(
                "run_id=%s reason=run_finished final_status=%s",
                run_id, state.status.value,
            )

    def _converge_skip_remaining(
        self,
        run_id: str,
        state: RunState,
        definition: WorkflowDef,
        order: list[str],
        root_failed: str,
    ) -> None:
        """把所有未到终态的步骤标记 SKIPPED，沿阻断链记录失败来源。

        按拓扑序处理，failed_by_step 优先指向阻断链上“首个未成功的前置”，
        继续向上即可追溯到真正 FAILED 的根因步骤；若前置都已成功（纯粹因
        失败屏障而未启动的独立分支），直接指向 root_failed 根因步骤。
        """
        for step_id in order:
            step_state = state.steps[step_id]
            if step_state.status in STEP_TERMINAL_STATUSES:
                continue  # 失败前已成功 / 自身失败 / 已收敛：一律保持不动
            step_def = definition.get_step(step_id)
            assert step_def is not None
            blocker = self._first_blocker(step_def, state)
            self._mark_skipped(
                step_state, blocker or root_failed, run_id
            )

    @staticmethod
    def _root_failed_step(state: RunState, order: list[str]) -> str:
        """根因失败步骤：拓扑序（稳定、确定）上最早的 FAILED 步骤。"""
        for step_id in order:
            if state.steps[step_id].status is StepStatus.FAILED:
                return step_id
        return order[0]  # 理论不可达：仅当无任何 FAILED 却进入失败收敛时兜底

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
        step_def: StepDef,
        state: RunState,
    ) -> str | None:
        """返回阻断本步骤的前置步骤 id：优先返回失败/被跳过的前置。

        若所有前置都已成功则返回 None；若仅存在尚未到终态的前置（收敛阶段
        的中间情形），返回第一个未成功的前置，使阻断链连续可追溯。
        """
        for dep in step_def.depends_on:
            dep_status = state.steps[dep].status
            if dep_status in (StepStatus.FAILED, StepStatus.SKIPPED):
                return dep
        for dep in step_def.depends_on:
            if state.steps[dep].status is not StepStatus.SUCCEEDED:
                return dep
        return None

    @staticmethod
    def _build_dependents(definition: WorkflowDef) -> dict[str, list[str]]:
        """步骤 id -> 直接依赖它的下游步骤 id 列表（保持定义出现顺序）。"""
        dependents: dict[str, list[str]] = {
            step.id: [] for step in definition.steps
        }
        for step in definition.steps:
            for dep in step.depends_on:
                dependents[dep].append(step.id)
        return dependents

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
