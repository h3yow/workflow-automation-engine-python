"""FastAPI HTTP 层：定义管理、触发运行、查询状态。"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .definitions import DefinitionRegistry, build_definition
from .engine import WorkflowEngine
from .executor import ActionRegistry
from .models import (
    DuplicateTriggerError,
    InvalidConcurrencyError,
    InvalidDefinitionError,
    RetryPolicy,
    RunNotFoundError,
    RunState,
)


class StepDefIn(BaseModel):
    id: str
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int | None = None


class RetryPolicyIn(BaseModel):
    max_attempts: int | None = None
    base_delay_seconds: float | None = None
    backoff_multiplier: float | None = None
    max_delay_seconds: float | None = None


class DefinitionIn(BaseModel):
    name: str
    version: int
    steps: list[StepDefIn]
    retry_policy: RetryPolicyIn | None = None
    max_concurrency: int | None = None


class TriggerIn(BaseModel):
    run_id: str | None = None


def _default_engine() -> WorkflowEngine:
    """默认装配：空注册表 + 空动作表（演示用；动作由使用方注册）。"""
    registry = DefinitionRegistry()
    actions = ActionRegistry()
    return WorkflowEngine(registry, actions)


def _serialize_run(state: RunState) -> dict[str, Any]:
    return {
        "run_id": state.run_id,
        "status": state.status.value,
        "failed_step_id": state.failed_step_id,
        "definition_version": state.definition.version,
        "max_concurrency": state.max_concurrency,
        "started_at": state.started_at,
        "finished_at": state.finished_at,
        "steps": [
            {
                "step_id": step.step_id,
                "status": step.status.value,
                "attempts": step.attempts,
                "error": step.error,
                "failed_by_step": step.failed_by_step,
            }
            for step in state.steps.values()
        ],
    }


def create_app(
    engine: WorkflowEngine | None = None,
    definitions: DefinitionRegistry | None = None,
) -> FastAPI:
    """构造 FastAPI 应用，并装配可区分的错误响应。"""
    app = FastAPI(title="Workflow Automation Engine", version="0.1.0")

    if engine is None:
        engine = _default_engine()
    app.state.engine = engine
    registry = definitions or engine.definitions

    def _error(status_code: int, reason: str, message: str) -> JSONResponse:
        return JSONResponse(
            status_code=status_code,
            content={"error": {"reason": reason, "message": message}},
        )

    @app.post("/definitions")
    def publish_definition(payload: DefinitionIn) -> dict[str, Any]:
        policy = RetryPolicy()
        if payload.retry_policy is not None:
            data = payload.retry_policy.model_dump(exclude_none=True)
            policy = RetryPolicy(**data)
        definition = build_definition(
            name=payload.name,
            version=payload.version,
            raw_steps=[step.model_dump() for step in payload.steps],
            retry_policy=policy,
            max_concurrency=payload.max_concurrency,
        )
        try:
            registry.put(definition)
        except InvalidConcurrencyError as exc:
            # 422 + reason=invalid_concurrency：并发上限配置非法，可与其它校验区分
            return _error(422, InvalidConcurrencyError.reason, exc.message)
        except InvalidDefinitionError as exc:
            # 422 + reason=invalid_definition：缺信息 / 自相矛盾的定义
            return _error(422, InvalidDefinitionError.reason, exc.message)
        return {
            "name": definition.name,
            "version": definition.version,
            "steps": len(definition.steps),
            "max_concurrency": definition.max_concurrency,
        }

    @app.post("/runs")
    def trigger_run(payload: TriggerIn) -> Any:
        try:
            state = engine.trigger(payload.run_id)
        except DuplicateTriggerError as exc:
            # 409 + reason=duplicate_trigger：可区分的拒绝结果
            return _error(409, DuplicateTriggerError.reason, str(exc))
        except InvalidDefinitionError as exc:
            return _error(422, InvalidDefinitionError.reason, exc.message)
        return JSONResponse(status_code=202, content=_serialize_run(state))

    @app.get("/runs/{run_id}")
    def get_run(run_id: str) -> Any:
        try:
            state = engine.get_run(run_id)
        except RunNotFoundError as exc:
            return _error(404, RunNotFoundError.reason, str(exc))
        return _serialize_run(state)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
