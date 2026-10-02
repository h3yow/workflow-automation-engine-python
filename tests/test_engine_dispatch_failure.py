"""场景四：引擎自身在派发/启动执行单元环节出错时的终态收敛。

与业务步骤失败（动作抛错）区分：本场景中业务动作本身没问题，是引擎侧
“准备启动后续执行单元”这一步出错（模拟准备抛错、系统资源不足无法再开
新单元）。要求与业务失败一致的收敛口径：

- 运行整体 FAILED，RunState.error 指到可识别的位置（reason/location/目标步骤）；
- 每个步骤都落在明确、可区分的终态（succeeded/failed/skipped），
  不再有 pending/ready/running 的“一直没开始”中间态；
- 出错前已成功的步骤保持成功，不回退、不重标；
- 没机会跑起来的步骤 SKIPPED，failed_by_step 非空，可回溯到断裂点
  （引擎侧断裂来源为 "engine:scheduler"），追溯链条不断；
- 出错时可能已经有别的分支在跑：自然跑完，规则一致；
- 同样定义 + 同样故障反复触发，最终结论稳定一致；
- 失败闸门：启动环节出错后，不再启动任何新的执行单元。
"""

from __future__ import annotations

import logging
import threading
import time

import pytest

from workflow_engine.definitions import build_definition
from workflow_engine.models import (
    ENGINE_FAILURE_SOURCE,
    RunStatus,
    StepStatus,
)

from .conftest import FaultyUnitLauncher

logger = logging.getLogger("workflow_engine.test")

TERMINAL = {StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.SKIPPED}


def _log_verdict(state, tag: str) -> None:
    logger.info(
        "run_id=%s suite=engine_dispatch_failure case=%s status=%s error=%s "
        "reason=engine_failure_run_verdict",
        state.run_id, tag, state.status.value, state.error,
    )
    for s in state.steps.values():
        logger.info(
            "run_id=%s step_id=%s status=%s attempts=%d failed_by_step=%s "
            "error=%s reason=engine_failure_step_verdict",
            state.run_id, s.step_id, s.status.value, s.attempts,
            s.failed_by_step, s.error,
        )


def _assert_run_failure_traceable(state, *, location: str, target: str) -> None:
    """运行整体失败且原因可识别。"""
    assert state.status is RunStatus.FAILED
    assert state.error is not None
    assert "engine_dispatch_failure" in state.error
    assert location in state.error
    assert target in state.error
    assert state.finished_at is not None


def _assert_every_step_terminal(state) -> None:
    """任何步骤都必须落在明确终态；被跳过者必须带来源，追溯链不空。"""
    known = set(state.steps)
    for sid, step_state in state.steps.items():
        assert step_state.status in TERMINAL, (
            f"{sid} left in non-terminal state {step_state.status}"
        )
        if step_state.status is StepStatus.SKIPPED:
            assert step_state.failed_by_step, (
                f"{sid} skipped without traceable source"
            )
            # 来源要么是真实步骤（可在本运行中找到），要么是引擎断裂来源
            assert (
                step_state.failed_by_step in known
                or step_state.failed_by_step == ENGINE_FAILURE_SOURCE
            ), f"{sid} has dangling source {step_state.failed_by_step}"


def test_dispatch_failure_fails_run_and_converges_every_step(make_engine):
    """线性 DAG：root 成功后，启动 b 的执行单元失败 -> 全收敛、可追溯。"""
    launcher = FaultyUnitLauncher(fail_steps={"b"})
    engine, registry, actions = make_engine(
        asynchronous=False, unit_launcher=launcher
    )
    registry.put(build_definition("eng-fail", 1, [
        {"id": "a"},
        {"id": "b", "depends_on": ["a"]},
        {"id": "c", "depends_on": ["a"]},
        {"id": "d", "depends_on": ["b"]},
    ], max_concurrency=1))

    def must_not_run(ctx):
        pytest.fail(f"step {ctx['step_id']} must never execute")

    actions.register("a", lambda ctx: "A")
    actions.register("b", must_not_run)
    actions.register("c", must_not_run)
    actions.register("d", must_not_run)

    state = engine.trigger("run-eng-fail-linear")
    _log_verdict(state, "linear_dispatch_failure")

    # 运行失败、原因指到可识别位置（派发/启动环节、目标步骤 b）
    _assert_run_failure_traceable(state, location="step_unit_launch", target="b")

    statuses = {sid: s.status for sid, s in state.steps.items()}
    # 出错前已成功的步骤保持成功，不回退
    assert statuses["a"] is StepStatus.SUCCEEDED
    # 没机会跑的步骤全部 SKIPPED（不是 pending/ready，也不是业务 failed）
    assert statuses["b"] is StepStatus.SKIPPED
    assert statuses["c"] is StepStatus.SKIPPED
    assert statuses["d"] is StepStatus.SKIPPED
    # 追溯链：d 的直接前置 b 已被跳过 -> 指 b；可继续回溯到 engine:scheduler
    assert state.steps["d"].failed_by_step == "b"
    assert state.steps["b"].failed_by_step == ENGINE_FAILURE_SOURCE
    assert state.steps["c"].failed_by_step == ENGINE_FAILURE_SOURCE
    # 没跑起来的步骤没有任何尝试
    for sid in ("b", "c", "d"):
        assert state.steps[sid].attempts == 0

    _assert_every_step_terminal(state)


def test_dispatch_failure_running_branches_get_consistent_verdict(make_engine):
    """启动环节出错时已有别的分支在跑：让其自然跑完并保持成功，边界一致。"""
    launcher = FaultyUnitLauncher(fail_steps={"blocked"})
    engine, registry, actions = make_engine(
        asynchronous=True, unit_launcher=launcher
    )
    # 注意声明顺序：root 完成后按稳定拓扑序派发，running 先占名额进入在跑，
    # 随后 blocked 启动失败（闸门关闭），idle 不再被派发。
    registry.put(build_definition("eng-fail", 1, [
        {"id": "root"},
        {"id": "running", "depends_on": ["root"]},
        {"id": "blocked", "depends_on": ["root"]},
        {"id": "down", "depends_on": ["blocked"]},
        {"id": "idle", "depends_on": ["root"]},
    ], max_concurrency=3))

    root_done = threading.Event()
    running_started = threading.Event()
    release_running = threading.Event()

    actions.register(
        "root", lambda ctx: root_done.set() or "R"
    )

    def running_action(ctx):
        # 拖过 blocked 启动失败被收敛的时刻，证明“在跑分支”不被打断
        running_started.set()
        assert release_running.wait(timeout=5)
        time.sleep(0.05)
        return "OK"

    actions.register("running", running_action)
    actions.register(
        "blocked", lambda ctx: pytest.fail("blocked action must never run")
    )
    actions.register(
        "down", lambda ctx: pytest.fail("down action must never run")
    )
    actions.register(
        "idle", lambda ctx: pytest.fail("idle action must never run")
    )

    engine.trigger("run-eng-fail-branches")
    assert root_done.wait(timeout=2)
    assert running_started.wait(timeout=2), "running branch did not start"
    # 等调度线程登记 blocked 的启动失败（失败闸门已落），再放行在跑分支，
    # 避免“在跑分支等放行 / 收敛等在跑分支结束”的相互等待。
    deadline = time.monotonic() + 2
    while "blocked" not in launcher.failed and time.monotonic() < deadline:
        time.sleep(0.01)
    assert "blocked" in launcher.failed
    release_running.set()
    final = engine.wait_until_done("run-eng-fail-branches", timeout=5)
    _log_verdict(final, "running_branch_consistent_verdict")

    _assert_run_failure_traceable(
        final, location="step_unit_launch", target="blocked"
    )
    statuses = {sid: s.status for sid, s in final.steps.items()}
    # 出错前的 root 与出错时已在跑、自然跑完的分支保持成功
    assert statuses["root"] is StepStatus.SUCCEEDED
    assert statuses["running"] is StepStatus.SUCCEEDED
    # 启动失败的目标、其下游、以及未派发的无关节点都 SKIPPED
    assert statuses["blocked"] is StepStatus.SKIPPED
    assert statuses["down"] is StepStatus.SKIPPED
    assert statuses["idle"] is StepStatus.SKIPPED
    # 追溯链不断
    assert final.steps["blocked"].failed_by_step == ENGINE_FAILURE_SOURCE
    assert final.steps["down"].failed_by_step == "blocked"
    assert final.steps["idle"].failed_by_step == ENGINE_FAILURE_SOURCE
    _assert_every_step_terminal(final)


def test_dispatch_failure_verdict_is_repeatable(make_engine):
    """同样定义 + 同样故障反复触发：运行结论与每步终态稳定一致。"""

    def _one_verdict(run_id: str):
        launcher = FaultyUnitLauncher(fail_steps={"b"})
        engine, registry, actions = make_engine(
            asynchronous=False, unit_launcher=launcher
        )
        registry.put(build_definition("eng-rep", 1, [
            {"id": "a"},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c", "depends_on": ["b"]},
        ], max_concurrency=1))
        actions.register("a", lambda ctx: "A")
        actions.register("b", lambda ctx: pytest.fail("b must not run"))
        actions.register("c", lambda ctx: pytest.fail("c must not run"))
        state = engine.trigger(run_id)
        steps = tuple(sorted(
            (sid, s.status, s.failed_by_step)
            for sid, s in state.steps.items()
        ))
        engine.shutdown()
        return state.status, state.error, steps, tuple(launcher.failed)

    verdicts = [_one_verdict(f"run-eng-rep-{i}") for i in range(5)]
    assert len(set(verdicts)) == 1
    status, error, steps, failed_at = verdicts[0]
    assert status is RunStatus.FAILED
    assert error is not None and "engine_dispatch_failure" in error
    step_map = {sid: (st, by) for sid, st, by in steps}
    assert step_map["a"] == (StepStatus.SUCCEEDED, None)
    assert step_map["b"] == (StepStatus.SKIPPED, ENGINE_FAILURE_SOURCE)
    assert step_map["c"] == (StepStatus.SKIPPED, "b")
    assert failed_at == ("b",)
    logger.info(
        "suite=engine_dispatch_failure case=repeatable reason=repeatable_verdict "
        "samples=%d", 5
    )


def test_raw_system_error_at_launch_is_wrapped_and_converged(make_engine):
    """系统层原始异常（如资源不足）在启动处抛出：统一包装并收敛。"""
    launcher = FaultyUnitLauncher(fail_steps={"b"}, raw=True)
    engine, registry, actions = make_engine(
        asynchronous=False, unit_launcher=launcher
    )
    registry.put(build_definition("eng-raw", 1, [
        {"id": "a"}, {"id": "b", "depends_on": ["a"]},
    ], max_concurrency=1))
    actions.register("a", lambda ctx: "A")
    actions.register("b", lambda ctx: pytest.fail("b must not run"))

    state = engine.trigger("run-eng-raw")
    _log_verdict(state, "raw_system_error_wrapped")

    _assert_run_failure_traceable(
        state, location="step_unit_launch", target="b"
    )
    # 原始系统异常信息不丢
    assert "RuntimeError" in (state.error or "")
    assert "resource exhaustion" in (state.error or "")
    assert state.steps["a"].status is StepStatus.SUCCEEDED
    assert state.steps["b"].status is StepStatus.SKIPPED
    assert state.steps["b"].failed_by_step == ENGINE_FAILURE_SOURCE


def test_no_new_units_start_after_engine_dispatch_failure(make_engine):
    """失败边界：启动环节出错后，就绪队列里的后续步骤不再被派发。"""
    # 并发 2：root 完成后 b/c 同时就绪；b 启动失败时 c 尚未被尝试派发
    launcher = FaultyUnitLauncher(fail_steps={"b"})
    engine, registry, actions = make_engine(
        asynchronous=False, unit_launcher=launcher
    )
    registry.put(build_definition("eng-gate", 1, [
        {"id": "a"},
        {"id": "b", "depends_on": ["a"]},
        {"id": "c", "depends_on": ["a"]},
    ], max_concurrency=2))
    actions.register("a", lambda ctx: "A")
    actions.register("b", lambda ctx: pytest.fail("b must not run"))
    actions.register("c", lambda ctx: pytest.fail("c must not run"))

    state = engine.trigger("run-eng-gate")
    _log_verdict(state, "dispatch_gate_closed")

    _assert_run_failure_traceable(
        state, location="step_unit_launch", target="b"
    )
    # 闸门在 b 失败后立即关闭：c 根本没进入启动环节
    assert launcher.attempted == ["a", "b"]
    assert launcher.failed == ["b"]
    assert state.steps["c"].status is StepStatus.SKIPPED
    assert state.steps["c"].attempts == 0
    assert state.steps["c"].failed_by_step == ENGINE_FAILURE_SOURCE
