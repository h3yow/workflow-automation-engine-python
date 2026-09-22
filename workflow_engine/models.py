"""Core type definitions for the workflow automation engine.

Only enums / immutable data shapes + error types live here; behaviour is
implemented in the other modules of the package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"          # terminal, unrecoverable (retries exhausted or non-retryable)
    SKIPPED = "skipped"        # terminal, downstream of an upstream failure


class RunStatus(str, Enum):
    PENDING = "pending"        # created, not yet started
    RUNNING = "running"
    SUCCEEDED = "succeeded"    # terminal: every branch succeeded
    FAILED = "failed"          # terminal: at least one step failed / skipped


class RejectReason(str, Enum):
    """Distinguishable reasons a request is rejected."""

    ALREADY_RUNNING = "already_running"     # duplicate / concurrent trigger of a run
    RUN_NOT_FOUND = "run_not_found"
    RUN_ALREADY_EXISTS = "run_already_exists"
    WORKFLOW_NOT_FOUND = "workflow_not_found"
    VERSION_CONFLICT = "version_conflict"
    BAD_DEFINITION = "bad_definition"


# --- Definition side (immutable snapshots) ---------------------------------


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1            # total attempts including the first one
    base_delay_seconds: float = 0.0  # exponential backoff base
    max_delay_seconds: float = 0.0   # backoff cap

    def delay_for_attempt(self, attempt: int) -> float:
        """Backoff delay before *attempt* (attempt 1 is the first try -> 0).

        Exponential: base * 2 ** (attempt - 2), capped at ``max_delay``.
        """
        if attempt <= 1 or self.base_delay_seconds <= 0:
            return 0.0
        raw = self.base_delay_seconds * (2 ** (attempt - 2))
        if self.max_delay_seconds > 0:
            return min(raw, self.max_delay_seconds)
        return raw


@dataclass(frozen=True)
class StepDef:
    id: str
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    # Name of the registered handler that performs the side effect.
    handler: str = ""


@dataclass(frozen=True)
class WorkflowDef:
    name: str
    version: int
    steps: tuple[StepDef, ...] = field(default_factory=tuple)

    def step(self, step_id: str) -> StepDef | None:
        for step in self.steps:
            if step.id == step_id:
                return step
        return None


# --- Runtime side ------------------------------------------------------------


@dataclass(frozen=True)
class StepResult:
    """Outcome of one step execution (possibly several attempts)."""

    success: bool
    retryable: bool = False
    error: str | None = None
    output: object = None
    attempts: int = 1


@dataclass
class StepRuntime:
    step_id: str
    status: StepStatus = StepStatus.PENDING
    attempts: int = 0
    error: str | None = None
    output: object = None


@dataclass
class RunRecord:
    run_id: str
    workflow_name: str = ""
    definition: WorkflowDef | None = None   # immutable snapshot bound on trigger
    status: RunStatus = RunStatus.PENDING
    steps: dict[str, StepRuntime] = field(default_factory=dict)
    trigger_count: int = 0           # how many times trigger was attempted


# --- Errors ------------------------------------------------------------------


class EngineError(Exception):
    """Base class for distinguishable engine errors."""


class DefinitionError(EngineError):
    """The workflow definition is missing information or self-contradictory."""

    def __init__(self, reason: str, detail: str | None = None) -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


class TriggerRejected(EngineError):
    """A trigger request was rejected (e.g. duplicate/concurrent trigger)."""

    def __init__(self, reason: RejectReason, run_id: str, detail: str | None = None) -> None:
        super().__init__(f"{reason.value} run_id={run_id} {detail or ''}".strip())
        self.reason = reason
        self.run_id = run_id
        self.detail = detail
