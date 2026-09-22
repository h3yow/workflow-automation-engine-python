"""Scenario 1: concurrent / repeated triggers must not duplicate side effects.

Only one trigger may execute the run; all concurrent/repeated triggers get a
distinguishable ``already_running`` rejection.
"""

from __future__ import annotations

import threading

import pytest

from workflow_engine import (
    RejectReason,
    RetryPolicy,
    StepDef,
    StepExecutor,
    TriggerRejected,
    WorkflowDef,
    WorkflowEngine,
)
from workflow_engine.models import RunStatus, StepStatus

from ._helpers import assert_run_status, assert_side_effect_count, assert_step_status


def _engine_with_slow_step(run_id: str):
    """A single-step workflow whose handler blocks until released."""
    side_effects: list[str] = []
    started = threading.Event()
    release = threading.Event()

    def slow_handler() -> str:
        side_effects.append("slow")
        started.set()
        release.wait(timeout=2.0)
        return "done"

    executor = StepExecutor(sleep=lambda _delay: None)
    executor.register("slow", slow_handler)
    engine = WorkflowEngine(executor=executor)
    engine.register_definition(WorkflowDef(
        name="parallel-wf", version=1,
        steps=(StepDef(id="s1", handler="slow",
                       retry_policy=RetryPolicy(max_attempts=1)),),
    ))
    engine.create_run(run_id, "parallel-wf")
    return engine, side_effects, started, release


def test_concurrent_duplicate_triggers_execute_side_effect_once() -> None:
    run_id = "run-concurrent-1"
    engine, side_effects, started, release = _engine_with_slow_step(run_id)

    accepted: list = []
    rejected: list[RejectReason] = []

    def fire() -> None:
        try:
            accepted.append(engine.trigger(run_id))
        except TriggerRejected as exc:
            rejected.append(exc.reason)

    threads = [threading.Thread(target=fire, name=f"trigger-{i}") for i in range(8)]
    for t in threads:
        t.start()
    # Wait until the one winning trigger is genuinely executing the handler.
    assert started.wait(timeout=2.0), "the single accepted trigger never started the step"

    # A trigger issued while the run is RUNNING blocks on the per-run lock and
    # must observe the terminal state afterwards -> rejected, never re-run.
    while_running = threading.Thread(target=fire, name="trigger-while-running")
    while_running.start()

    # Release the in-flight side effect; the winning trigger completes and the
    # blocked triggers observe the terminal state and are rejected.
    release.set()
    for t in threads:
        t.join(timeout=2.0)
    while_running.join(timeout=2.0)

    record = engine.get_run(run_id)
    assert_run_status(record, RunStatus.SUCCEEDED,
                      basis="exactly_one_trigger_ran_to_completion")
    assert_step_status(record, "s1", StepStatus.SUCCEEDED,
                       basis="single_side_effect_completed")
    assert len(accepted) == 1, f"expected exactly 1 accepted trigger, got {len(accepted)}"
    # 7 losers among the 8 racing threads + 1 caller blocked while RUNNING = 8.
    assert len(rejected) == 8, f"expected 8 concurrent rejects, got {len(rejected)}"
    assert all(reason == RejectReason.ALREADY_RUNNING for reason in rejected), rejected
    assert_side_effect_count(len(side_effects), 1,
                             basis="side_effect_executions_under_concurrent_triggers")

    # Post-completion re-trigger is also rejected (not silently swallowed) and
    # must never run the handler again.
    with pytest.raises(TriggerRejected) as after:
        engine.trigger(run_id)
    assert after.value.reason == RejectReason.ALREADY_RUNNING
    assert_side_effect_count(len(side_effects), 1,
                             basis="side_effect_executions_after_completion_retrigger")
    assert record.trigger_count == 10  # 8 threads + 1 blocked while running + 1 explicit
