"""Scenario 3: definition changes while a run is in flight.

* the in-flight run completes against the immutable snapshot captured at its
  trigger time (old step set / old retry policy);
* runs triggered afterwards bind to the newest definition;
* missing/self-contradictory definitions are rejected with distinguishable
  reasons.
"""

from __future__ import annotations

import threading

import pytest

from workflow_engine import (
    DefinitionError,
    RejectReason,
    RetryPolicy,
    StepDef,
    StepExecutor,
    TriggerRejected,
    WorkflowDef,
    WorkflowEngine,
)
from workflow_engine.executor import RetryableStepError
from workflow_engine.models import RunStatus, StepStatus

from ._helpers import assert_run_status, assert_side_effect_count, assert_step_status, log_line


def _v1() -> WorkflowDef:
    # Two steps; the second one is retryable but configured for 1 attempt
    # only in v1 -> it fails terminally under v1 semantics.
    return WorkflowDef(
        name="evolving-wf", version=1,
        steps=(
            StepDef(id="prepare", handler="prepare",
                    retry_policy=RetryPolicy(max_attempts=1)),
            StepDef(id="finish-v1", handler="finish",
                    retry_policy=RetryPolicy(max_attempts=1)),
        ),
    )


def _v2() -> WorkflowDef:
    # v2 renames the second step and grants it 3 attempts; the recoverable
    # failure now succeeds.
    return WorkflowDef(
        name="evolving-wf", version=2,
        steps=(
            StepDef(id="prepare", handler="prepare",
                    retry_policy=RetryPolicy(max_attempts=1)),
            StepDef(id="finish-v2", handler="finish",
                    retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.0)),
        ),
    )


def test_inflight_run_keeps_startup_definition_new_run_uses_new_version() -> None:
    prepare_started = threading.Event()
    release_prepare = threading.Event()
    effects: list[str] = []
    finish_attempts = {"n": 0}

    def prepare() -> str:
        effects.append("prepare")
        prepare_started.set()
        release_prepare.wait(timeout=2.0)
        return "prepared"

    def finish() -> str:
        finish_attempts["n"] += 1
        effects.append(f"finish#{finish_attempts['n']}")
        if finish_attempts["n"] < 3:
            raise RetryableStepError("transient until attempt 3")
        return "finished"

    executor = StepExecutor(sleep=lambda _d: None)
    executor.register("prepare", prepare)
    executor.register("finish", finish)
    engine = WorkflowEngine(executor=executor)
    engine.register_definition(_v1())

    # Create + trigger the first run; it blocks inside the 'prepare' handler.
    old_run_id = "run-version-old"
    engine.create_run(old_run_id, "evolving-wf")
    outcome: dict[str, object] = {}

    def run_old() -> None:
        try:
            outcome["record"] = engine.trigger(old_run_id)
        except BaseException as exc:  # pragma: no cover - surface thread errors
            outcome["error"] = exc

    old_thread = threading.Thread(target=run_old, name="old-run")
    old_thread.start()
    assert prepare_started.wait(timeout=2.0), "old run never entered its v1 step"

    # --- definition change WHILE the old run is executing ---------------------
    engine.register_definition(_v2())
    log_line(old_run_id, "-", "definition_changed_to_v2_while_run_in_flight", new_version=2)

    # New run created after the change binds to v2 (but do not trigger it until
    # the old run is released, because the executor's handler gate is shared).
    new_run_id = "run-version-new"
    engine.create_run(new_run_id, "evolving-wf")

    # Release the old run; it must finish on v1: 'finish-v1' fails terminally
    # (1 attempt despite the now-active v2 retry budget of 3).
    release_prepare.set()
    old_thread.join(timeout=2.0)
    assert "error" not in outcome, outcome.get("error")
    old_record = engine.get_run(old_run_id)

    log_line(old_run_id, "-", "snapshot_version", expected=1,
             actual=old_record.definition.version)
    assert old_record.definition.version == 1, "in-flight run must keep its start-time version"
    assert_run_status(old_record, RunStatus.FAILED,
                      basis="old_run_executed_v1_retry_budget_then_failed")
    assert_step_status(old_record, "prepare", StepStatus.SUCCEEDED,
                       basis="v1_step_present_and_executed")
    assert_step_status(old_record, "finish-v1", StepStatus.FAILED,
                       basis="v1_only_step_used_old_max_attempts_1")
    assert old_record.steps["finish-v1"].attempts == 1
    assert "finish-v2" not in old_record.steps, "old run must not gain v2 steps"

    # Reset shared handler state, then trigger the new run -> runs v2 fully.
    prepare_started.clear()
    release_prepare.set()  # do not block the second execution
    finish_attempts["n"] = 0
    before = list(effects)
    new_record = engine.trigger(new_run_id)

    log_line(new_run_id, "-", "snapshot_version", expected=2,
             actual=new_record.definition.version)
    assert new_record.definition.version == 2, "new run must use the newest version"
    assert_run_status(new_record, RunStatus.SUCCEEDED,
                      basis="new_run_executed_v2_and_recovered_with_retries")
    assert_step_status(new_record, "finish-v2", StepStatus.SUCCEEDED,
                       basis="v2_step_and_v2_retry_budget_used")
    assert new_record.steps["finish-v2"].attempts == 3
    assert "finish-v1" not in new_record.steps
    new_effects = effects[len(before):]
    assert_side_effect_count(
        new_effects.count("finish#1") + new_effects.count("finish#2")
        + new_effects.count("finish#3"), 3,
        basis="new_run_side_effects_follow_v2_retry_policy",
    )


# --- definition rejection: distinguishable reasons ----------------------------


@pytest.mark.parametrize(
    ("steps", "expected_reason"),
    [
        ((), "empty_workflow"),
        ((StepDef(id="", handler="h"),), "missing_step_id"),
        ((StepDef(id="a", handler="h"), StepDef(id="a", handler="h")), "duplicate_step_id"),
        ((StepDef(id="a", depends_on=("ghost",), handler="h"),), "unknown_dependency"),
        (
            (StepDef(id="a", depends_on=("b",), handler="h"),
             StepDef(id="b", depends_on=("a",), handler="h")),
            "cyclic_dependency",
        ),
        ((StepDef(id="a", handler=""),), "missing_handler"),
        ((StepDef(id="a", handler="h", retry_policy=RetryPolicy(max_attempts=0)),),
         "invalid_retry_policy"),
    ],
)
def test_bad_definitions_are_rejected_with_distinguishable_reasons(
    steps: tuple[StepDef, ...], expected_reason: str,
) -> None:
    engine = WorkflowEngine(executor=StepExecutor())
    definition = WorkflowDef(name="bad-wf", version=1, steps=tuple(steps))
    with pytest.raises(DefinitionError) as exc_info:
        engine.register_definition(definition)
    log_line("-", "-", "bad_definition_rejected",
             expected=expected_reason, actual=exc_info.value.reason)
    assert exc_info.value.reason == expected_reason


def test_same_version_with_different_content_is_rejected() -> None:
    engine = WorkflowEngine(executor=StepExecutor())
    engine.register_definition(WorkflowDef(
        name="locked-wf", version=1,
        steps=(StepDef(id="a", handler="h"),),
    ))
    with pytest.raises(DefinitionError) as exc_info:
        engine.register_definition(WorkflowDef(
            name="locked-wf", version=1,
            steps=(StepDef(id="a", handler="h"), StepDef(id="b", handler="h")),
        ))
    assert exc_info.value.reason == "version_conflict"


def test_trigger_unknown_workflow_and_missing_run_are_distinguishable() -> None:
    engine = WorkflowEngine(executor=StepExecutor())
    engine.create_run("orphan-run", "never-registered")
    with pytest.raises(TriggerRejected) as wf_missing:
        engine.trigger("orphan-run")
    assert wf_missing.value.reason == RejectReason.WORKFLOW_NOT_FOUND

    with pytest.raises(TriggerRejected) as run_missing:
        engine.trigger("does-not-exist")
    assert run_missing.value.reason == RejectReason.RUN_NOT_FOUND
