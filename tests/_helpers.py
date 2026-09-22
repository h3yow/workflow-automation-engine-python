"""Shared helpers for engine scenario tests.

Every assertion helper prints the run id, the relevant step ids and the
*basis* of the decision, as required by the test-logging rules.
"""

from __future__ import annotations

import logging
from typing import Any

from workflow_engine.models import RunRecord, RunStatus, StepStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)
log = logging.getLogger("tests")


def log_line(run_id: str, step_id: str, basis: str, **extra: Any) -> None:
    extras = " ".join(f"{k}={v}" for k, v in extra.items())
    log.info("ASSERT run_id=%s step_id=%s basis=%s %s", run_id, step_id, basis, extras)


def assert_step_status(
    record: RunRecord,
    step_id: str,
    expected: StepStatus,
    basis: str,
) -> None:
    actual = record.steps[step_id].status
    log_line(
        record.run_id, step_id, basis,
        expected=expected.value, actual=actual.value,
        attempts=record.steps[step_id].attempts,
    )
    assert actual == expected, f"run={record.run_id} step={step_id}: {actual} != {expected}"


def assert_run_status(record: RunRecord, expected: RunStatus, basis: str) -> None:
    summary = {sid: st.status.value for sid, st in record.steps.items()}
    log_line(record.run_id, "-", basis, expected=expected.value,
             actual=record.status.value, steps=summary)
    assert record.status == expected, f"run={record.run_id}: {record.status} != {expected}"


def assert_side_effect_count(observed: Any, expected: Any, basis: str) -> None:
    log.info("ASSERT run_id=- step_id=- basis=%s expected=%s observed=%s",
             basis, expected, observed)
    assert observed == expected, f"{basis}: {observed} != {expected}"
