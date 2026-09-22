"""场景二：步骤失败与重试。

覆盖：
- 可恢复失败按指数退避自动重试，达到上限后成功（副作用可多次执行）。
- 一直可恢复失败 -> 重试次数封顶后 FAILED，下游 SKIPPED，失败原因可追溯。
- 不可恢复失败 -> 不重试直接 FAILED 并传播下游。
- 任何失败运行整体 FAILED，下游任何步骤都不会被标记为 SUCCEEDED。
"""

from __future__ import annotations

import logging

import pytest

from workflow_engine.executor import SleepProtocol
from workflow_engine.models import (
    FatalStepError,
    RecoverableStepError,
    RetryPolicy,
    RunStatus,
    StepStatus,
)

logger = logging.getLogger("workflow_engine.test")


def _publish(registry):
    from workflow_engine.definitions import build_definition

    registry.put(
        build_definition(
            "payment-flow",
            1,
            [
                {"id": "create_order"},
                {"id": "call_gateway", "depends_on": ["create_order"]},
                {"id": "send_receipt", "depends_on": ["call_gateway"]},
            ],
        )
    )


class RecordingSleep(SleepProtocol):
    def __init__(self):
        self.calls: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.calls.append(seconds)


def _log_steps(state):
    for record in state.steps.values():
        logger.info(
            "run_id=%s step_id=%s status=%s attempts=%s error=%s "
            "failed_by_step=%s reason=final_step_verdict",
            state.run_id,
            record.step_id,
            record.status.value,
            record.attempts,
            record.error,
            record.failed_by_step,
        )


def test_recoverable_failure_retries_with_backoff_and_succeeds(make_engine):
    policy = RetryPolicy(
        max_attempts=4,
        base_delay_seconds=0.1,
        backoff_multiplier=2.0,
    )
    sleep_recorder = RecordingSleep()
    engine, registry, actions = make_engine(asynchronous=False)
    _publish(registry)

    attempts = {"n": 0}

    def flaky_gateway(ctx):
        attempts["n"] += 1
        logger.info(
            "run_id=%s step_id=%s attempt=%d reason=simulated_attempt",
            ctx["run_id"], ctx["step_id"], attempts["n"],
        )
        if attempts["n"] < 3:
            raise RecoverableStepError(
                ctx["step_id"], f"gateway timeout #{attempts['n']}"
            )
        return "charged"

    actions.register("create_order", lambda ctx: "order-1")
    actions.register("call_gateway", flaky_gateway)
    actions.register("send_receipt", lambda ctx: "receipt")

    # 注入自定义策略：直接替换引擎内执行器的退避记录器
    engine._executor._sleep = sleep_recorder  # type: ignore[attr-defined]

    # 用自定义重试策略重新构造定义版本（步骤级覆盖走默认策略，这里覆盖整个定义）
    from workflow_engine.definitions import build_definition

    custom_def = build_definition(
        "payment-flow", 1,
        [
            {"id": "create_order"},
            {"id": "call_gateway", "depends_on": ["create_order"]},
            {"id": "send_receipt", "depends_on": ["call_gateway"]},
        ],
        retry_policy=policy,
    )

    state = engine.trigger("run-retry-ok", definition=custom_def)
    _log_steps(state)

    assert state.status is RunStatus.SUCCEEDED
    gw = state.steps["call_gateway"]
    assert gw.status is StepStatus.SUCCEEDED
    assert gw.attempts == 3  # 首调 + 2 次重试
    # 指数退避：0.1 * 2^0, 0.1 * 2^1
    assert sleep_recorder.calls == pytest.approx([0.1, 0.2])


def test_retries_exhausted_marks_failed_and_skips_downstream(make_engine):
    engine, registry, actions = make_engine(asynchronous=False)
    _publish(registry)

    counter = {"n": 0}

    def always_bad(ctx):
        counter["n"] += 1
        raise RecoverableStepError(ctx["step_id"], "persistent timeout")

    actions.register("create_order", lambda ctx: "order-2")
    actions.register("call_gateway", always_bad)
    actions.register(
        "send_receipt",
        lambda ctx: pytest.fail("downstream side effect must not run"),
    )

    from workflow_engine.definitions import build_definition

    custom_def = build_definition(
        "payment-flow", 1,
        [
            {"id": "create_order"},
            {"id": "call_gateway", "depends_on": ["create_order"]},
            {"id": "send_receipt", "depends_on": ["call_gateway"]},
        ],
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
    )

    state = engine.trigger("run-retry-exhausted", definition=custom_def)
    _log_steps(state)

    assert state.status is RunStatus.FAILED
    gw = state.steps["call_gateway"]
    assert gw.status is StepStatus.FAILED
    assert gw.attempts == 3  # 重试次数有上限
    assert "retries exhausted" in (gw.error or "")
    assert "persistent timeout" in (gw.error or "")  # 原因可追溯到步骤

    receipt = state.steps["send_receipt"]
    assert receipt.status is StepStatus.SKIPPED  # 可区分的终止状态
    assert receipt.failed_by_step == "call_gateway"
    assert counter["n"] == 3


def test_fatal_failure_does_not_retry_and_propagates(make_engine):
    engine, registry, actions = make_engine(asynchronous=False)
    _publish(registry)

    counter = {"n": 0}

    def fatal_gateway(ctx):
        counter["n"] += 1
        raise FatalStepError(ctx["step_id"], "card declined")

    actions.register("create_order", lambda ctx: "order-3")
    actions.register("call_gateway", fatal_gateway)
    actions.register(
        "send_receipt",
        lambda ctx: pytest.fail("downstream must never execute"),
    )

    state = engine.trigger("run-fatal-1")
    _log_steps(state)

    assert state.status is RunStatus.FAILED
    gw = state.steps["call_gateway"]
    assert gw.status is StepStatus.FAILED
    assert gw.attempts == 1  # 不可恢复 -> 不重试
    assert "fatal_step_failure" in (gw.error or "")
    assert "card declined" in (gw.error or "")
    assert state.steps["send_receipt"].status is StepStatus.SKIPPED
    assert state.steps["send_receipt"].failed_by_step == "call_gateway"
    assert counter["n"] == 1
