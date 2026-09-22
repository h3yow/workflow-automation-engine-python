"""End-to-end HTTP smoke tests through the FastAPI layer.

The three required scenarios are covered directly against the engine in the
dedicated test modules; here we assert that the web surface exposes the same
distinguishable rejection semantics.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

import main


def _make_client() -> TestClient:
    # Fresh engine (with demo handlers) per test to isolate side-effect state.
    main.engine = main.build_engine()
    return TestClient(main.app)


def _demo_workflow(version: int = 1, fatal: bool = False) -> dict:
    return {
        "name": "api-wf",
        "version": version,
        "steps": [
            {"id": "a", "handler": "demo_ok"},
            {"id": "b", "depends_on": ["a"],
             "handler": "demo_fatal" if fatal else "demo_ok",
             "retry": {"max_attempts": 2, "base_delay_seconds": 0}},
        ],
    }


def test_happy_path_and_duplicate_trigger_rejected() -> None:
    client = _make_client()
    assert client.post("/workflows", json=_demo_workflow()).status_code == 201
    assert client.post("/runs/r1", json={"workflow_name": "api-wf"}).status_code == 201

    first = client.post("/runs/r1/trigger")
    assert first.status_code == 200
    assert first.json()["accepted"] is True
    assert first.json()["run"]["status"] == "succeeded"

    dup = client.post("/runs/r1/trigger")
    assert dup.status_code == 409
    body = dup.json()
    assert body["rejected"] is True
    assert body["reason"] == "already_running"

    missing = client.post("/runs/nope/trigger")
    assert missing.status_code == 404
    assert missing.json()["reason"] == "run_not_found"


def test_bad_definition_rejected_with_reason() -> None:
    client = _make_client()
    bad = {
        "name": "broken", "version": 1,
        "steps": [{"id": "a", "depends_on": ["ghost"], "handler": "demo_ok"}],
    }
    resp = client.post("/workflows", json=bad)
    assert resp.status_code == 422
    assert resp.json()["reason"] == "bad_definition"
    assert resp.json()["detail_reason"] == "unknown_dependency"


def test_failed_run_skips_downstream_via_http() -> None:
    client = _make_client()
    assert client.post("/workflows", json=_demo_workflow(fatal=True)).status_code == 201
    assert client.post("/runs/rf", json={"workflow_name": "api-wf"}).status_code == 201
    resp = client.post("/runs/rf/trigger")
    assert resp.status_code == 200
    run = resp.json()["run"]
    assert run["status"] == "failed"
    statuses = {step["step_id"]: step["status"] for step in run["steps"]}
    assert statuses == {"a": "succeeded", "b": "failed"}
    failed = next(step for step in run["steps"] if step["step_id"] == "b")
    assert "permanent failure" in failed["error"]
