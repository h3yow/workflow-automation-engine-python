"""HTTP 层测试：可区分的拒绝结果与端到端状态查询。"""

from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from workflow_engine.api import create_app
from workflow_engine.definitions import DefinitionRegistry, build_definition

logger = logging.getLogger("workflow_engine.test")


def _build_client(*, asynchronous: bool = False):
    from workflow_engine.executor import ActionRegistry
    from workflow_engine.executor import SleepProtocol
    from workflow_engine.engine import WorkflowEngine

    class InstantSleep(SleepProtocol):
        def sleep(self, seconds: float) -> None:
            return None

    registry = DefinitionRegistry()
    actions = ActionRegistry()
    engine = WorkflowEngine(
        registry, actions, sleep_protocol=InstantSleep(),
        run_asynchronously=asynchronous,
    )
    actions.register("a", lambda ctx: "A")
    actions.register("b", lambda ctx: "B")
    app = create_app(engine, registry)
    return TestClient(app), registry


def _publish_version(client, version: int, step_ids: list[dict]):
    resp = client.post(
        "/definitions",
        json={"name": "wf", "version": version, "steps": step_ids},
    )
    return resp


def test_publish_trigger_and_query_happy_path():
    client, _ = _build_client()
    resp = _publish_version(
        client, 1, [{"id": "a"}, {"id": "b", "depends_on": ["a"]}]
    )
    assert resp.status_code == 200

    resp = client.post("/runs", json={"run_id": "run-api-1"})
    assert resp.status_code == 202
    body = resp.json()
    assert body["run_id"] == "run-api-1"

    # 同步引擎：触发时已跑完
    resp = client.get("/runs/run-api-1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "succeeded"
    assert body["definition_version"] == 1
    statuses = {s["step_id"]: s["status"] for s in body["steps"]}
    assert statuses == {"a": "succeeded", "b": "succeeded"}
    logger.info(
        "run_id=%s reason=api_happy_path statuses=%s",
        body["run_id"], statuses,
    )


def test_duplicate_trigger_returns_distinguishable_409():
    client, _ = _build_client()
    _publish_version(client, 1, [{"id": "a"}])
    first = client.post("/runs", json={"run_id": "dup"})
    assert first.status_code == 202
    second = client.post("/runs", json={"run_id": "dup"})
    assert second.status_code == 409
    error = second.json()["error"]
    assert error["reason"] == "duplicate_trigger"
    logger.info("run_id=dup reason=api_duplicate_rejected http_status=409")


def test_invalid_definition_returns_distinguishable_422():
    client, _ = _build_client()
    resp = _publish_version(
        client, 1, [{"id": "a", "depends_on": ["ghost"]}]
    )
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["reason"] == "invalid_definition"
    assert "unknown step" in error["message"]
    logger.info("reason=api_invalid_definition_rejected http_status=422")


def test_invalid_concurrency_returns_its_own_reason_422():
    client, _ = _build_client()
    resp = client.post(
        "/definitions",
        json={
            "name": "wf", "version": 1,
            "steps": [{"id": "a"}], "max_concurrency": 0,
        },
    )
    assert resp.status_code == 422
    error = resp.json()["error"]
    # 独立 reason，与其它 invalid_definition 校验失败可区分
    assert error["reason"] == "invalid_concurrency"
    assert "max_concurrency" in error["message"]
    logger.info("reason=api_invalid_concurrency_rejected http_status=422")


def test_run_response_reports_locked_max_concurrency():
    client, _ = _build_client()
    client.post(
        "/definitions",
        json={
            "name": "wf", "version": 1,
            "steps": [{"id": "a"}], "max_concurrency": 3,
        },
    )
    resp = client.post("/runs", json={"run_id": "run-c"})
    assert resp.status_code == 202
    assert resp.json()["max_concurrency"] == 3


def test_run_not_found_returns_404():
    client, _ = _build_client()
    resp = client.get("/runs/missing")
    assert resp.status_code == 404
    assert resp.json()["error"]["reason"] == "run_not_found"
