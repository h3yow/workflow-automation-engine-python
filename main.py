"""FastAPI web service exposing the local workflow engine."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from workflow_engine import (
    DefinitionError,
    RejectReason,
    RetryPolicy,
    StepDef,
    TriggerRejected,
    WorkflowDef,
    WorkflowEngine,
)
from workflow_engine.models import RunRecord

app = FastAPI(title="workflow-automation-engine", version="0.1.0")


# --- demo handlers so the service is exercisable out of the box ---------------


def build_engine() -> WorkflowEngine:
    """Construct an engine with the demo side-effect handlers registered."""
    from workflow_engine.executor import RetryableStepError

    new_engine = WorkflowEngine()
    flaky_state: dict[str, int] = {}

    def demo_ok() -> dict[str, str]:
        return {"result": "ok"}

    def demo_flaky() -> dict[str, str]:
        # Succeeds on the 2nd attempt; first attempts are retryable.
        n = flaky_state.get("demo_flaky", 0) + 1
        flaky_state["demo_flaky"] = n
        if n < 2:
            raise RetryableStepError(f"transient failure on attempt {n}")
        return {"result": "recovered", "attempts": n}

    def demo_fatal() -> None:
        raise ValueError("permanent failure: cannot proceed")

    for name, fn in (("demo_ok", demo_ok), ("demo_flaky", demo_flaky), ("demo_fatal", demo_fatal)):
        new_engine.executor.register(name, fn)
    return new_engine


# A single process-local engine instance backs the service.
engine = build_engine()


# --- request / response models ----------------------------------------------


class RetryPolicyIn(BaseModel):
    max_attempts: int = Field(default=1, ge=1)
    base_delay_seconds: float = Field(default=0.0, ge=0)
    max_delay_seconds: float = Field(default=0.0, ge=0)


class StepIn(BaseModel):
    id: str
    depends_on: list[str] = Field(default_factory=list)
    handler: str
    retry: RetryPolicyIn = Field(default_factory=RetryPolicyIn)


class DefinitionIn(BaseModel):
    name: str
    version: int = Field(ge=1)
    steps: list[StepIn]


class CreateRunIn(BaseModel):
    workflow_name: str


# --- serialization ------------------------------------------------------------


def _step_out(runtime) -> dict[str, Any]:
    return {
        "step_id": runtime.step_id,
        "status": runtime.status.value,
        "attempts": runtime.attempts,
        "error": runtime.error,
        "output": runtime.output,
    }


def run_out(record: RunRecord) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "workflow_name": record.workflow_name,
        "status": record.status.value,
        "definition_version": record.definition.version if record.definition else None,
        "trigger_count": record.trigger_count,
        "steps": [_step_out(runtime) for runtime in record.steps.values()],
    }


# --- error handling ------------------------------------------------------------


@app.exception_handler(DefinitionError)
def handle_definition_error(_request, exc: DefinitionError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "rejected": True,
            "reason": RejectReason.BAD_DEFINITION.value,
            "detail_reason": exc.reason,
            "detail": exc.detail,
        },
    )


@app.exception_handler(TriggerRejected)
def handle_trigger_rejected(_request, exc: TriggerRejected) -> JSONResponse:
    status_code = 404 if exc.reason == RejectReason.RUN_NOT_FOUND else 409
    return JSONResponse(
        status_code=status_code,
        content={
            "rejected": True,
            "reason": exc.reason.value,
            "run_id": exc.run_id,
            "detail": exc.detail,
        },
    )


# --- endpoints ----------------------------------------------------------------


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "workflow-automation-engine", "status": "ok"}


@app.post("/workflows", status_code=201)
def register_workflow(payload: DefinitionIn) -> dict[str, Any]:
    definition = WorkflowDef(
        name=payload.name,
        version=payload.version,
        steps=tuple(
            StepDef(
                id=step.id,
                depends_on=tuple(step.depends_on),
                handler=step.handler,
                retry_policy=RetryPolicy(
                    max_attempts=step.retry.max_attempts,
                    base_delay_seconds=step.retry.base_delay_seconds,
                    max_delay_seconds=step.retry.max_delay_seconds,
                ),
            )
            for step in payload.steps
        ),
    )
    engine.register_definition(definition)
    return {"accepted": True, "workflow_name": payload.name, "version": payload.version}


@app.post("/runs/{run_id}", status_code=201)
def create_run(run_id: str, payload: CreateRunIn) -> dict[str, Any]:
    record = engine.create_run(run_id, payload.workflow_name)
    return run_out(record)


@app.post("/runs/{run_id}/trigger")
def trigger_run(run_id: str) -> dict[str, Any]:
    record = engine.trigger(run_id)
    return {"accepted": True, "run": run_out(record)}


@app.get("/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    return run_out(engine.get_run(run_id))

