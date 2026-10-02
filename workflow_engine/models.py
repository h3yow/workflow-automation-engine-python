"""核心领域模型：状态枚举、异常类型与数据结构。"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from typing import Any


# 运行内步骤并发的默认上限：定义未显式配置时使用
DEFAULT_MAX_CONCURRENCY = 4


class StepStatus(str, enum.Enum):
    """单步骤的生命周期状态。"""

    PENDING = "pending"        # 尚未开始，等待前置步骤
    READY = "ready"            # 前置全部成功，可以执行
    RUNNING = "running"        # 正在执行副作用
    SUCCEEDED = "succeeded"    # 执行成功（终态）
    FAILED = "failed"          # 执行失败，重试耗尽或不可恢复（终态）
    SKIPPED = "skipped"        # 因上游失败被跳过（终态）


class RunStatus(str, enum.Enum):
    """一次工作流运行的总体状态。"""

    RUNNING = "running"        # 运行中
    SUCCEEDED = "succeeded"    # 全部步骤成功（终态）
    FAILED = "failed"          # 存在步骤失败（终态）


# 步骤终态集合
STEP_TERMINAL_STATUSES = frozenset(
    {StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.SKIPPED}
)


class WorkflowError(Exception):
    """引擎异常基类。"""

    reason: str = "workflow_error"


class InvalidDefinitionError(WorkflowError):
    """工作流定义非法（缺字段 / 自相矛盾）。"""

    reason = "invalid_definition"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class DuplicateTriggerError(WorkflowError):
    """同一运行被并发或重复触发，拒绝结果（可与其它拒绝原因区分）。"""

    reason = "duplicate_trigger"

    def __init__(self, run_id: str) -> None:
        super().__init__(f"run {run_id} has already been triggered")
        self.run_id = run_id


class InvalidConcurrencyError(InvalidDefinitionError):
    """并发上限配置非法（如小于 1）；发布定义时即拒绝。

    单独的 reason 码，可与其它 invalid_definition 校验失败区分；
    仍是 InvalidDefinitionError 的子类，保持既有捕获逻辑兼容。
    """

    reason = "invalid_concurrency"

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RunNotFoundError(WorkflowError):
    """查询了不存在的运行。"""

    reason = "run_not_found"

    def __init__(self, run_id: str) -> None:
        super().__init__(f"run {run_id} not found")
        self.run_id = run_id


class RecoverableStepError(WorkflowError):
    """可恢复失败：引擎应按退避策略自动重试。"""

    reason = "recoverable_step_failure"

    def __init__(self, step_id: str, message: str) -> None:
        super().__init__(f"step {step_id}: {message}")
        self.step_id = step_id
        self.message = message


class FatalStepError(WorkflowError):
    """不可恢复失败：立即终止该步骤及其下游。"""

    reason = "fatal_step_failure"

    def __init__(self, step_id: str, message: str) -> None:
        super().__init__(f"step {step_id}: {message}")
        self.step_id = step_id
        self.message = message


class EngineDispatchError(WorkflowError):
    """引擎自身在派发/启动执行单元环节出错（非业务动作失败）。

    例如准备执行单元时抛错、系统资源不足无法再启动新的执行单元等。
    location 给出可识别的出错位置（环节名），step_id 为当时正准备启动
    的步骤（可能为 None：出错时没有具体步骤）。
    """

    reason = "engine_dispatch_failure"

    def __init__(
        self,
        message: str,
        *,
        location: str,
        step_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.location = location
        self.step_id = step_id


# 引擎自身出错时，被跳过步骤 failed_by_step 使用的稳定来源标识。
# 它不指向任何业务步骤，表示“断裂来自引擎调度/启动环节”；
# 具体出错位置由 RunState.error 中的 reason/location 给出。
ENGINE_FAILURE_SOURCE = "engine:scheduler"


@dataclass(frozen=True)
class RetryPolicy:
    """重试与退避策略。

    max_attempts: 最大尝试总次数（含首次执行），必须 >= 1。
    base_delay_seconds: 首次重试退避基数。
    backoff_multiplier: 退避乘数，第 n 次重试等待 base * multiplier ** n。
    max_delay_seconds: 单次退避上限。
    """

    max_attempts: int = 3
    base_delay_seconds: float = 0.1
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 30.0

    def delay_for_attempt(self, failed_attempt: int) -> float:
        """第 failed_attempt 次尝试失败后，下一次尝试前的等待秒数。

        例如 failed_attempt=1 -> base*mult**0；failed_attempt=2 -> base*mult**1。
        """
        if failed_attempt < 1:
            return 0.0
        raw = self.base_delay_seconds * self.backoff_multiplier ** (failed_attempt - 1)
        return min(raw, self.max_delay_seconds)


@dataclass(frozen=True)
class StepDef:
    """步骤定义（属于某个定义版本，运行期间不可变）。"""

    id: str
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    max_attempts: int | None = None  # 覆盖定义级重试次数；None 用默认值


@dataclass(frozen=True)
class WorkflowDef:
    """工作流定义的一个不可变版本快照。"""

    name: str
    version: int
    steps: tuple[StepDef, ...]
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    # 单次运行内部步骤并发上限；None 表示用引擎/默认兼容值。
    # 随版本走：每个版本可不同，触发运行时快照锁定。
    max_concurrency: int | None = None

    def step_ids(self) -> frozenset[str]:
        return frozenset(step.id for step in self.steps)

    def get_step(self, step_id: str) -> StepDef | None:
        for step in self.steps:
            if step.id == step_id:
                return step
        return None


@dataclass
class StepState:
    """运行时单步骤状态。"""

    step_id: str
    status: StepStatus = StepStatus.PENDING
    attempts: int = 0
    error: str | None = None          # 失败原因（可追溯到具体步骤）
    failed_by_step: str | None = None # 若被跳过，记录触发失败的上游步骤
    started_at: float | None = None
    finished_at: float | None = None


@dataclass
class RunState:
    """一次运行的完整状态，绑定其启动时的定义快照。"""

    run_id: str
    definition: WorkflowDef
    status: RunStatus = RunStatus.RUNNING
    steps: dict[str, StepState] = field(default_factory=dict)
    started_at: float | None = None
    finished_at: float | None = None
    # 运行整体失败的可追溯原因：业务失败时指向根因步骤；
    # 引擎自身调度/启动出错时记录出错环节与位置。成功运行恒为 None。
    error: str | None = None
    # 触发时锁定的实际并发上限（定义未配置时取引擎默认值）
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY

    @staticmethod
    def new(
        run_id: str,
        definition: WorkflowDef,
        now: float,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    ) -> "RunState":
        steps = {step.id: StepState(step_id=step.id) for step in definition.steps}
        return RunState(
            run_id=run_id,
            definition=definition,
            steps=steps,
            started_at=now,
            max_concurrency=max_concurrency,
        )


def new_run_id() -> str:
    """生成运行标识。"""
    return f"run-{uuid.uuid4().hex[:12]}"
