"""核心领域模型：状态枚举、异常类型与数据结构。"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field


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

# 运行内部并发度的向后兼容默认值：
# 未显式配置 max_concurrency 的定义版本（含旧版本）按此值调度。
DEFAULT_MAX_CONCURRENCY = 4


class WorkflowError(Exception):
    """引擎异常基类。"""

    reason: str = "workflow_error"


class InvalidDefinitionError(WorkflowError):
    """工作流定义非法（缺字段 / 自相矛盾）。"""

    reason = "invalid_definition"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class InvalidConcurrencyError(InvalidDefinitionError):
    """并发上限配置非法（如小于 1）。

    继承自 InvalidDefinitionError（仍属于发布期定义校验失败，HTTP 422），
    但 reason 可与其它校验失败明确区分。
    """

    reason = "invalid_concurrency"


class DuplicateTriggerError(WorkflowError):
    """同一运行被并发或重复触发，拒绝结果（可与其它拒绝原因区分）。"""

    reason = "duplicate_trigger"

    def __init__(self, run_id: str) -> None:
        super().__init__(f"run {run_id} has already been triggered")
        self.run_id = run_id


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
    # 运行内部并发上限；None 表示该版本未显式配置，触发时回落到
    # 向后兼容默认值 DEFAULT_MAX_CONCURRENCY。不同版本可配置不同值。
    max_concurrency: int | None = None

    def effective_max_concurrency(self) -> int:
        """触发时实际生效的并发上限（锁定到运行快照上）。"""
        if self.max_concurrency is None:
            return DEFAULT_MAX_CONCURRENCY
        return self.max_concurrency

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
    # 触发瞬间从定义快照解析出的并发上限，运行期间锁定；
    # 之后再发布新版本（即便并发度不同）也不影响本次运行。
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    started_at: float | None = None
    finished_at: float | None = None

    @staticmethod
    def new(run_id: str, definition: WorkflowDef, now: float) -> "RunState":
        steps = {step.id: StepState(step_id=step.id) for step in definition.steps}
        return RunState(
            run_id=run_id,
            definition=definition,
            steps=steps,
            max_concurrency=definition.effective_max_concurrency(),
            started_at=now,
        )


def new_run_id() -> str:
    """生成运行标识。"""
    return f"run-{uuid.uuid4().hex[:12]}"
