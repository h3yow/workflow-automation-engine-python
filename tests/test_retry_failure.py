"""Scenario 2: recoverable failures retry with backoff (bounded); unrecoverable
failures terminate the failed step and skip every downstream step.
"""

from __future__ import annotations

import pytest

from workflow_engine import (
    RetryPolicy,
    StepDef,
    StepExecutor,
    WorkflowDef,
    WorkflowEngine,
)
from workflow_engine.executor import RetryableStepError
from workflow_engine.models import RunStatus, StepStatus

from ._helpers import assert_run_status, assert_side_effect_count, assert_step_status


# --- recoverable failure: bounded exponential-backoff retry -------------------


def test_recoverable_failure_retries_with_backoff_and_succeeds() -> None:
    run_id = "run-retry-recover"
    calls: dict[str, int] = {}
    sleeps: list[float] = []

    def flaky() -> str:
        calls["flaky"] = calls.get("flaky", 0) + 1
        if calls["flaky"] <= 2:
            raise RetryableStepError(f"transient #{calls['flaky']}")
        return "recovered"

    executor = StepExecutor(sleep=sleeps.append)
    executor.register("flaky", flaky)
    engine = WorkflowEngine(executor=executor)
    engine.register_definition(WorkflowDef(
        name="retry-wf", version=1,
        steps=(
            StepDef(
                id="flaky-step",
                handler="flaky",
                retry_policy=RetryPolicy(
                    max_attempts=4, base_delay_seconds=0.1, max_delay_seconds=0.5,
                ),
            ),
        ),
    ))
    engine.create_run(run_id, "retry-wf")
    record = engine.trigger(run_id)

    assert_step_status(record, "flaky-step", StepStatus.SUCCEEDED,
                       basis="recoverable_failure_succeeded_within_budget")
    assert_side_effect_count(calls["flaky"], 3,
                             basis="handler_invocations_until_recovered")
    # Backoff before attempt 2 and 3: 0.1, 0.2 (exponential, capped at 0.5).
    assert_side_effect_count(sleeps, [0.1, 0.2], basis="exponential_backoff_schedule")
    assert_run_status(record, RunStatus.SUCCEEDED,
                      basis="all_steps_succeeded_after_retries")
    assert record.steps["flaky-step"].attempts == 3
    assert record.steps["flaky-step"].output == "recovered"


def test_recoverable_failure_exhausting_attempts_fails_terminal() -> None:
    run_id = "run-retry-exhausted"
    calls = {"n": 0}
    sleeps: list[float] = []

    def always_transient() -> None:
        calls["n"] += 1
        raise RetryableStepError("still broken")

    executor = StepExecutor(sleep=sleeps.append)
    executor.register("bad", always_transient)
    engine = WorkflowEngine(executor=executor)
    engine.register_definition(WorkflowDef(
        name="retry-wf", version=1,
        steps=(StepDef(
            id="exhausted-step", handler="bad",
            retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.01),
        ),),
    ))
    engine.create_run(run_id, "retry-wf")
    record = engine.trigger(run_id)

    assert_step_status(record, "exhausted-step", StepStatus.FAILED,
                       basis="retries_exhausted_is_terminal_failure")
    assert_side_effect_count(calls["n"], 3, basis="bounded_by_max_attempts")
    assert_side_effect_count(len(sleeps), 2, basis="backoff_only_between_attempts")
    assert_run_status(record, RunStatus.FAILED,
                      basis="run_failed_when_retries_exhausted")
    assert "still broken" in record.steps["exhausted-step"].error


# --- unrecoverable failure: failed step + skipped downstream ------------------


def test_unrecoverable_failure_marks_failed_and_skips_downstream() -> None:
    run_id = "run-fatal-1"
    effects: list[str] = []

    def ok(step_id: str):
        def _handler() -> str:
            effects.append(step_id)
            return f"{step_id}-out"
        return _handler

    def fatal() -> None:
        effects.append("fatal")
        raise ValueError("permanent damage")

    executor = StepExecutor(sleep=lambda _d: None)
    executor.register("a", ok("a"))
    executor.register("fatal", fatal)
    executor.register("c", ok("c"))
    executor.register("d1", ok("d1"))
    executor.register("d2", ok("d2"))
    engine = WorkflowEngine(executor=executor)
    # DAG: a -> fatal -> d1 -> d2 ; a -> c (independent branch)
    engine.register_definition(WorkflowDef(
        name="fatal-wf", version=1,
        steps=(
            StepDef(id="a", handler="a"),
            StepDef(id="fatal", depends_on=("a",), handler="fatal"),
            StepDef(id="c", depends_on=("a",), handler="c"),
            StepDef(id="d1", depends_on=("fatal",), handler="d1"),
            StepDef(id="d2", depends_on=("d1", "c"), handler="d2"),
        ),
    ))
    engine.create_run(run_id, "fatal-wf")
    record = engine.trigger(run_id)

    assert_run_status(record, RunStatus.FAILED,
                      basis="unrecoverable_step_makes_run_fail")
    assert_step_status(record, "a", StepStatus.SUCCEEDED,
                       basis="upstream_of_failure_ran_before_it")
    assert_step_status(record, "fatal", StepStatus.FAILED,
                       basis="non_retryable_error_is_terminal_without_retries")
    assert "permanent damage" in record.steps["fatal"].error
    assert record.steps["fatal"].attempts == 1  # non-retryable: retries not consumed
    # Direct and transitive downstream steps are SKIPPED with traceable cause.
    assert_step_status(record, "d1", StepStatus.SKIPPED,
                       basis="direct_downstream_of_failed_step")
    assert "upstream_failure" in record.steps["d1"].error
    assert "'fatal'" in record.steps["d1"].error
    assert_step_status(record, "d2", StepStatus.SKIPPED,
                       basis="transitive_downstream_never_succeeds")
    assert "upstream_failure" in record.steps["d2"].error
    # Independent branch c, not started when the abort happened, is skipped too
    # but with a distinguishable basis — and never marked succeeded.
    assert_step_status(record, "c", StepStatus.SKIPPED,
                       basis="independent_unstarted_branch_aborted_not_succeeded")
    assert "run_aborted" in record.steps["c"].error
    assert_side_effect_count(effects, ["a", "fatal"],
                             basis="downstream_and_independent_side_effects_never_run")

    # No branch may be reported as successful when the run failed: the only
    # SUCCEEDED step is the upstream 'a'; success on the failed branch itself
    # would constitute a false success.
    succeeded = [sid for sid, st in record.steps.items() if st.status == StepStatus.SUCCEEDED]
    assert succeeded == ["a"], succeeded
