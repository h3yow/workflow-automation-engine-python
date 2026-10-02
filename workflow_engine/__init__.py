"""本地工作流自动化执行引擎。

对外暴露：
- 模型与异常：models
- 定义构建/校验/版本注册表：definitions
- 动作注册与步骤执行器（重试/退避）：executor
- 编排引擎（幂等触发、依赖调度、失败传播、定义快照）：engine
"""

from .definitions import DefinitionRegistry, build_definition, validate_definition
from .engine import WorkflowEngine
from .executor import ActionRegistry, StepExecutor
from .models import (
    DEFAULT_MAX_CONCURRENCY,
    EngineDispatchError,
    FatalStepError,
    InvalidConcurrencyError,
    InvalidDefinitionError,
    RecoverableStepError,
    RetryPolicy,
    RunState,
    RunStatus,
    StepDef,
    StepState,
    StepStatus,
    WorkflowDef,
)

__all__ = [
    "DEFAULT_MAX_CONCURRENCY",
    "ActionRegistry",
    "DefinitionRegistry",
    "EngineDispatchError",
    "FatalStepError",
    "InvalidConcurrencyError",
    "InvalidDefinitionError",
    "RecoverableStepError",
    "RetryPolicy",
    "RunState",
    "RunStatus",
    "StepDef",
    "StepExecutor",
    "StepState",
    "StepStatus",
    "WorkflowDef",
    "WorkflowEngine",
    "build_definition",
    "validate_definition",
]
