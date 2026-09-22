"""步骤动作注册表与单步骤执行（含重试 / 退避）。"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from .models import (
    FatalStepError,
    RecoverableStepError,
    RetryPolicy,
    StepDef,
    StepStatus,
    StepState,
    WorkflowDef,
)

logger = logging.getLogger("workflow_engine")

# 动作签名：接收步骤上下文，返回副作用结果
StepAction = Callable[[dict[str, Any]], Any]


class StepContext:
    """传给动作的上下文：运行标识、步骤标识、上游产物等。

    以普通 dict 形式传给动作（键：run_id/step_id/attempt/inputs），
    也保留属性访问，便于日志与测试断言。
    """

    def __init__(
        self,
        run_id: str,
        step_id: str,
        attempt: int,
        inputs: dict[str, Any],
    ) -> None:
        self.run_id = run_id
        self.step_id = step_id
        self.attempt = attempt
        self.inputs = inputs

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "step_id": self.step_id,
            "attempt": self.attempt,
            "inputs": self.inputs,
        }


class ActionRegistry:
    """步骤标识 -> 动作函数 的注册表。"""

    def __init__(self) -> None:
        self._actions: dict[str, StepAction] = {}

    def register(self, step_id: str, action: StepAction) -> None:
        self._actions[step_id] = action

    def get(self, step_id: str) -> StepAction:
        if step_id not in self._actions:
            raise KeyError(f"no action registered for step {step_id!r}")
        return self._actions[step_id]


class SleepProtocol:
    """可替换的退避等待，便于测试中不真实睡眠。"""

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class _NoSleep(SleepProtocol):
    def sleep(self, seconds: float) -> None:  # 测试替身：立即返回
        return None


StepResult = Any  # 动作返回值，作为下游输入透传


class StepExecutor:
    """按重试策略执行单个步骤，直接写回 StepState。

    判定规则：
    - 动作正常返回 -> SUCCEEDED。
    - 抛 RecoverableStepError -> 未超过该步骤最大尝试次数则按指数退避重试；
      超过则 FAILED（error 记录最后一次原因，attempts 记录真实尝试次数）。
    - 抛 FatalStepError -> 不重试，直接 FAILED。
    - 抛其它异常 -> 包装为 FatalStepError（不可恢复）。
    """

    def __init__(
        self,
        actions: ActionRegistry,
        sleep_protocol: SleepProtocol | None = None,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        self._actions = actions
        self._sleep = sleep_protocol or SleepProtocol()
        self._time = time_func or time.time

    def _max_attempts(self, definition: WorkflowDef, step_def: StepDef) -> int:
        return (
            step_def.max_attempts
            if step_def.max_attempts is not None
            else definition.retry_policy.max_attempts
        )

    def execute_step(
        self,
        run_id: str,
        definition: WorkflowDef,
        step_def: StepDef,
        state: StepState,
        inputs: dict[str, Any],
    ) -> tuple[StepStatus, StepResult]:
        """执行步骤直到成功或最终失败，返回 (终态状态, 动作返回值)。"""
        policy = definition.retry_policy
        max_attempts = self._max_attempts(definition, step_def)
        action = self._actions.get(step_def.id)

        state.status = StepStatus.RUNNING
        state.started_at = self._time()

        while True:
            state.attempts += 1
            ctx = StepContext(
                run_id=run_id,
                step_id=step_def.id,
                attempt=state.attempts,
                inputs=inputs,
            )
            logger.info(
                "run_id=%s step_id=%s attempt=%d/%d reason=execute_action",
                run_id, step_def.id, state.attempts, max_attempts,
            )
            try:
                result = action(ctx.as_dict())
            except FatalStepError as exc:
                return self._fail(
                    state,
                    f"{exc.reason}: {exc.message}",
                    fatal=True,
                    run_id=run_id,
                ), None
            except RecoverableStepError as exc:
                state.error = f"{exc.reason}: {exc.message}"
                logger.warning(
                    "run_id=%s step_id=%s attempt=%d reason=recoverable_failure detail=%s",
                    run_id, step_def.id, state.attempts, exc.message,
                )
                if state.attempts >= max_attempts:
                    return self._fail(
                        state,
                        f"retries exhausted ({state.attempts}/{max_attempts}): "
                        f"{state.error}",
                        fatal=False,
                        run_id=run_id,
                    ), None
                delay = policy.delay_for_attempt(state.attempts)
                logger.info(
                    "run_id=%s step_id=%s attempt=%d reason=backoff_sleep "
                    "delay_seconds=%.3f",
                    run_id, step_def.id, state.attempts, delay,
                )
                self._sleep.sleep(delay)
                continue
            except Exception as exc:  # 未声明异常一律视为不可恢复
                wrapped = f"unexpected {type(exc).__name__}: {exc}"
                logger.exception(
                    "run_id=%s step_id=%s attempt=%d reason=unexpected_failure",
                    run_id, step_def.id, state.attempts,
                )
                return self._fail(
                    state, f"fatal_step_failure: {wrapped}",
                    fatal=True, run_id=run_id,
                ), None
            else:
                state.status = StepStatus.SUCCEEDED
                state.error = None
                state.finished_at = self._time()
                logger.info(
                    "run_id=%s step_id=%s attempts=%d reason=step_succeeded",
                    run_id, step_def.id, state.attempts,
                )
                return StepStatus.SUCCEEDED, result

    def _fail(
        self,
        state: StepState,
        error: str,
        *,
        fatal: bool,
        run_id: str,
    ) -> StepStatus:
        state.status = StepStatus.FAILED
        state.error = error
        state.finished_at = self._time()
        kind = "fatal_failure" if fatal else "retries_exhausted"
        logger.error(
            "run_id=%s step_id=%s attempts=%d reason=step_failed kind=%s detail=%s",
            run_id, state.step_id, state.attempts, kind, error,
        )
        return StepStatus.FAILED
