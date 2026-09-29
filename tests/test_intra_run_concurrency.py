"""场景四：一次运行内部的步骤并发调度。

覆盖：
- 互不依赖的步骤确实同时执行（并行而非串行）；
- 同时在跑的步骤数不超过定义版本配置的并发上限，满了排队等空位；
- 有依赖关系的步骤严格等待全部前置成功后才开始（菱形 DAG）；
- 某步失败后运行以 failed 收尾、不再开新步骤；失败前已成功步骤保持
  succeeded；失败时在跑的无关分支跑到规则一致的终态；未启动/未决步骤
  全部 skipped 且 failed_by_step 可沿链回溯到根因；不存在悬空中间态；
- 并发上限随定义版本走：旧版本用向后兼容默认值，新版本可不同；
- 在途运行锁定启动时的并发设置，之后发布新版本不影响它；
- 非法并发配置（< 1）发布期被拒绝，reason=invalid_concurrency，
  可与其它定义校验失败区分；
- 相同定义 + 相同动作重复触发，最终成败结论与各步骤终态一致（确定性）。
"""

from __future__ import annotations

import logging
import threading

import pytest

from workflow_engine.definitions import build_definition
from workflow_engine.models import (
    DEFAULT_MAX_CONCURRENCY,
    FatalStepError,
    InvalidConcurrencyError,
    InvalidDefinitionError,
    RunStatus,
    STEP_TERMINAL_STATUSES,
    StepStatus,
)

logger = logging.getLogger("workflow_engine.test")


def _wait_for(predicate, timeout: float = 2.0, interval: float = 0.005) -> None:
    """轮询等待断言成立，避免在并发现象上依赖脆弱的固定 sleep。"""
    deadline_elapsed = 0.0
    while deadline_elapsed < timeout:
        if predicate():
            return
        threading.Event().wait(interval)
        deadline_elapsed += interval
    raise AssertionError("condition not met within timeout")


def _statuses(state) -> dict[str, str]:
    return {sid: s.status.value for sid, s in state.steps.items()}


# --------------------------------------------------------------------- #
# 1. 互不依赖的步骤确实并行
# --------------------------------------------------------------------- #


def test_independent_steps_run_in_parallel(make_engine):
    engine, registry, actions = make_engine(asynchronous=True)
    registry.put(
        build_definition(
            "parallel", 1, [{"id": "a"}, {"id": "b"}], max_concurrency=2
        )
    )

    started: list[str] = []
    start_lock = threading.Lock()
    release = threading.Event()

    def make_action(step_id):
        def action(ctx):
            with start_lock:
                started.append(step_id)
            release.wait(2)   # 阻塞住，让测试能观测到两个步骤同时在跑
            return step_id
        return action

    actions.register("a", make_action("a"))
    actions.register("b", make_action("b"))

    state = engine.trigger("run-par-1")

    # 观测到两个步骤同时处于 running，即证明它们并行（串行不可能出现）
    _wait_for(
        lambda: {s.status for s in state.steps.values()}
        == {StepStatus.RUNNING}
    )
    logger.info(
        "run_id=%s statuses=%s reason=independent_steps_overlap",
        state.run_id, _statuses(state),
    )
    with start_lock:
        assert sorted(started) == ["a", "b"]

    release.set()
    final = engine.wait_until_done("run-par-1", timeout=5)
    assert final.status is RunStatus.SUCCEEDED
    assert all(
        s.status is StepStatus.SUCCEEDED for s in final.steps.values()
    )


# --------------------------------------------------------------------- #
# 2. 并发度不超过上限，满了排队，空出后补位
# --------------------------------------------------------------------- #


def test_inflight_never_exceeds_concurrency_cap_and_queue_drains(make_engine):
    cap = 2
    engine, registry, actions = make_engine(asynchronous=True)
    registry.put(
        build_definition(
            "capped",
            1,
            [{"id": f"s{i}"} for i in range(5)],
            max_concurrency=cap,
        )
    )

    lock = threading.Lock()
    inflight = {"active": 0, "peak": 0, "starts": []}
    release = threading.Event()

    def make_action(step_id):
        def action(ctx):
            with lock:
                inflight["active"] += 1
                inflight["peak"] = max(inflight["peak"], inflight["active"])
                inflight["starts"].append(step_id)
            release.wait(5)
            with lock:
                inflight["active"] -= 1
            return step_id
        return action

    for i in range(5):
        actions.register(f"s{i}", make_action(f"s{i}"))

    state = engine.trigger("run-cap-1")
    _wait_for(lambda: inflight["peak"] == cap)
    # 确保此刻稳定停在 cap 个在跑（没有提前放行，多余步骤必然排队）
    threading.Event().wait(0.05)

    # 恰好 cap 个在跑，其余仍在排队（从未开始）
    running = {
        sid for sid, s in state.steps.items()
        if s.status is StepStatus.RUNNING
    }
    queued = {
        sid for sid, s in state.steps.items()
        if s.status is StepStatus.PENDING
    }
    assert len(running) == cap
    assert len(queued) == 3
    with lock:
        assert inflight["peak"] == cap
    logger.info(
        "run_id=%s running=%s queued=%s peak=%d reason=cap_enforced",
        state.run_id, sorted(running), sorted(queued), inflight["peak"],
    )

    release.set()
    final = engine.wait_until_done("run-cap-1", timeout=5)
    assert final.status is RunStatus.SUCCEEDED
    with lock:
        assert inflight["peak"] == cap  # 整个生命周期都没有突破上限
        assert sorted(inflight["starts"]) == [f"s{i}" for i in range(5)]
    assert final.max_concurrency == cap


# --------------------------------------------------------------------- #
# 3. 依赖严格等待（菱形 DAG）：汇聚点必须等所有前置成功
# --------------------------------------------------------------------- #


def test_diamond_dependencies_wait_for_all_upstreams(make_engine):
    engine, registry, actions = make_engine(asynchronous=True)
    # top -> (left, right) -> join
    registry.put(
        build_definition(
            "diamond",
            1,
            [
                {"id": "top"},
                {"id": "left", "depends_on": ["top"]},
                {"id": "right", "depends_on": ["top"]},
                {"id": "join", "depends_on": ["left", "right"]},
            ],
            max_concurrency=4,
        )
    )

    order: list[str] = []
    lock = threading.Lock()
    top_done = threading.Event()
    release_branches = threading.Event()

    def top(ctx):
        with lock: order.append("top")
        top_done.set()
        return "top"

    def left(ctx):
        with lock: order.append("left")
        release_branches.wait(2)  # 与 right 同时阻塞，证明两分支并行
        return "left"

    def right(ctx):
        with lock: order.append("right")
        release_branches.wait(2)
        return "right"

    def join(ctx):
        with lock: order.append("join")
        # inputs 必须包含两个已成功前置的产物
        assert ctx["inputs"] == {"left": "left", "right": "right"}
        return "join"

    actions.register("top", top)
    actions.register("left", left)
    actions.register("right", right)
    actions.register("join", join)

    state = engine.trigger("run-diamond-1")
    assert top_done.wait(2)
    # 汇聚前置 left/right 必须同时在跑（串行调度不可能出现这一观测）
    _wait_for(
        lambda: state.steps["left"].status is StepStatus.RUNNING
        and state.steps["right"].status is StepStatus.RUNNING
    )
    # 两个前置都未成功时，join 必须严格等待
    assert state.steps["join"].status is StepStatus.PENDING
    logger.info(
        "run_id=%s reason=branches_parallel_and_join_waits partial=%s",
        state.run_id, _statuses(state),
    )

    release_branches.set()
    final = engine.wait_until_done("run-diamond-1", timeout=5)
    assert final.status is RunStatus.SUCCEEDED
    with lock:
        assert order.index("top") < order.index("left")
        assert order.index("top") < order.index("right")
        assert order.index("join") > order.index("left")
        assert order.index("join") > order.index("right")
    assert final.steps["join"].status is StepStatus.SUCCEEDED


# --------------------------------------------------------------------- #
# 4. 失败收敛：终态可区分、可追溯、无悬空；在跑无关分支跑到终态
# --------------------------------------------------------------------- #


def test_failure_converges_to_distinguishable_traceable_terminal_states(
    make_engine,
):
    engine, registry, actions = make_engine(asynchronous=True)
    # a(失败)  b(独立慢分支)  c<-a  d<-c  e(独立，因上限2始终排队未启动)
    registry.put(
        build_definition(
            "fail-converge",
            1,
            [
                {"id": "a"},
                {"id": "b"},
                {"id": "c", "depends_on": ["a"]},
                {"id": "d", "depends_on": ["c"]},
                {"id": "e"},
            ],
            max_concurrency=2,
        )
    )

    release_a = threading.Event()
    release_b = threading.Event()

    def a(ctx):
        release_a.wait(2)
        raise FatalStepError("a", "root boom")

    def b(ctx):
        release_b.wait(2)  # 失败时仍在跑的无关分支
        return "b-result"

    def c(ctx): pytest.fail("c must not run")
    def d(ctx): pytest.fail("d must not run")
    def e(ctx): pytest.fail("e must not run")

    actions.register("a", a)
    actions.register("b", b)
    actions.register("c", c)
    actions.register("d", d)
    actions.register("e", e)

    state = engine.trigger("run-fail-conv-1")
    # a、b 占满 2 个槽；c/d/e 全部未启动
    _wait_for(
        lambda: state.steps["a"].status is StepStatus.RUNNING
        and state.steps["b"].status is StepStatus.RUNNING
    )
    threading.Event().wait(0.05)  # 稳定停在 cap，确认 e 仍未被补位
    assert state.steps["e"].status is StepStatus.PENDING

    release_a.set()  # 触发根因失败，但 b 还在跑
    _wait_for(lambda: state.steps["a"].status is StepStatus.FAILED)
    # 失败屏障：b 仍在跑时，排队的 e 不得补位启动
    assert state.steps["e"].status is StepStatus.PENDING
    assert state.steps["b"].status is StepStatus.RUNNING

    release_b.set()  # 放行在跑的无关分支
    final = engine.wait_until_done("run-fail-conv-1", timeout=5)

    logger.info("run_id=%s reason=failure_convergence final=%s",
                final.run_id, _statuses(final))
    for s in final.steps.values():
        logger.info(
            "run_id=%s step_id=%s status=%s failed_by_step=%s error=%s "
            "reason=final_step_verdict",
            final.run_id, s.step_id, s.status.value,
            s.failed_by_step, s.error,
        )

    # 运行整体失败
    assert final.status is RunStatus.FAILED

    # 根因：失败、原因可追溯
    a_state = final.steps["a"]
    assert a_state.status is StepStatus.FAILED
    assert a_state.attempts == 1
    assert "root boom" in (a_state.error or "")

    # 失败前/失败时在跑的无关分支成功，且不被回退
    assert final.steps["b"].status is StepStatus.SUCCEEDED

    # 未启动的下游/独立步骤全部 skipped，链路可回溯到根因 a
    assert final.steps["c"].status is StepStatus.SKIPPED
    assert final.steps["c"].failed_by_step == "a"
    assert final.steps["d"].status is StepStatus.SKIPPED
    assert final.steps["d"].failed_by_step == "c"  # 沿链：d -> c -> a
    assert final.steps["e"].status is StepStatus.SKIPPED
    assert final.steps["e"].failed_by_step == "a"  # 独立排队步骤直指根因

    # 不存在悬空的中间态
    assert all(
        s.status in STEP_TERMINAL_STATUSES for s in final.steps.values()
    )
    # 三类终态互斥且可区分，本运行同时出现
    present = {s.status for s in final.steps.values()}
    assert present == {
        StepStatus.FAILED, StepStatus.SUCCEEDED, StepStatus.SKIPPED
    }


# --------------------------------------------------------------------- #
# 5. 在途运行锁定启动时并发设置，之后发布新版本不影响它
# --------------------------------------------------------------------- #


def test_inflight_run_pins_concurrency_across_new_version(make_engine):
    engine, registry, actions = make_engine(asynchronous=True)
    # v1：并发度 2，两个互不依赖的慢步骤
    registry.put(
        build_definition(
            "pin", 1, [{"id": "v1a"}, {"id": "v1b"}], max_concurrency=2
        )
    )

    release_v1 = threading.Event()

    def v1_action(step_id):
        def action(ctx):
            release_v1.wait(2)
            return step_id
        return action

    actions.register("v1a", v1_action("v1a"))
    actions.register("v1b", v1_action("v1b"))

    run_v1 = engine.trigger("run-pin-old")
    _wait_for(
        lambda: run_v1.steps["v1a"].status is StepStatus.RUNNING
        and run_v1.steps["v1b"].status is StepStatus.RUNNING
    )
    assert run_v1.definition.version == 1
    assert run_v1.max_concurrency == 2

    # 在途期间发布 v2：并发度改成 8、步骤集合也变化
    registry.put(
        build_definition(
            "pin",
            2,
            [{"id": "v2a"}, {"id": "v2b"}, {"id": "v2c"}],
            max_concurrency=8,
        )
    )
    for sid in ("v2a", "v2b", "v2c"):
        actions.register(sid, lambda ctx, sid=sid: sid)

    # 触发新运行：必须落到 v2 / 并发度 8
    run_v2 = engine.trigger("run-pin-new")
    final_v2 = engine.wait_until_done("run-pin-new", timeout=5)
    assert final_v2.definition.version == 2
    assert final_v2.max_concurrency == 8
    assert final_v2.status is RunStatus.SUCCEEDED

    # v1 在途运行仍锁定并发度 2 与旧步骤集合，不受 v2 影响
    assert run_v1.max_concurrency == 2
    assert set(run_v1.steps) == {"v1a", "v1b"}
    release_v1.set()
    final_v1 = engine.wait_until_done("run-pin-old", timeout=5)
    assert final_v1.status is RunStatus.SUCCEEDED
    assert final_v1.definition.version == 1
    assert final_v1.max_concurrency == 2
    logger.info(
        "run_id=%s def_version=%d pinned_concurrency=%d "
        "reason=inflight_concurrency_pinned",
        final_v1.run_id, final_v1.definition.version,
        final_v1.max_concurrency,
    )


# --------------------------------------------------------------------- #
# 6. 不同版本可配置不同并发度；未配置版本使用向后兼容默认值
# --------------------------------------------------------------------- #


def test_versions_can_set_different_concurrency_and_unset_uses_default(
    make_engine,
):
    engine, registry, actions = make_engine(asynchronous=False)
    # v1 未配置：旧版本按默认并发度
    registry.put(build_definition("ver", 1, [{"id": "a"}]))
    # v2 显式 1
    registry.put(
        build_definition("ver", 2, [{"id": "a"}], max_concurrency=1)
    )
    # v3 显式 16
    registry.put(
        build_definition("ver", 3, [{"id": "a"}], max_concurrency=16)
    )
    actions.register("a", lambda ctx: "ok")

    assert registry.get_version(1).max_concurrency is None
    assert registry.get_version(1).effective_max_concurrency() == (
        DEFAULT_MAX_CONCURRENCY
    )
    assert registry.get_version(2).effective_max_concurrency() == 1
    assert registry.get_version(3).effective_max_concurrency() == 16

    # 已发布的旧版本仍能正常触发，并按默认并发度跑
    r1 = engine.trigger("run-v1", definition=registry.get_version(1))
    r2 = engine.trigger("run-v2", definition=registry.get_version(2))
    r3 = engine.trigger("run-v3", definition=registry.get_version(3))
    assert r1.max_concurrency == DEFAULT_MAX_CONCURRENCY
    assert r2.max_concurrency == 1
    assert r3.max_concurrency == 16
    for r in (r1, r2, r3):
        assert r.status is RunStatus.SUCCEEDED


# --------------------------------------------------------------------- #
# 7. 非法并发配置在发布期被拒绝，reason 可区分
# --------------------------------------------------------------------- #


@pytest.mark.parametrize("bad_value", [0, -1, -100])
def test_invalid_concurrency_rejected_at_publish_with_distinct_reason(
    make_engine, bad_value
):
    engine, registry, _ = make_engine(asynchronous=False)
    with pytest.raises(InvalidConcurrencyError) as exc_info:
        registry.put(
            build_definition(
                "bad", 1, [{"id": "a"}], max_concurrency=bad_value
            )
        )
    # 与其它定义校验失败区分
    assert exc_info.value.reason == "invalid_concurrency"
    assert "max_concurrency" in str(exc_info.value)
    # 仍是定义校验失败的子类（HTTP 层统一 422，仅 reason 更细）
    assert isinstance(exc_info.value, InvalidDefinitionError)
    logger.info(
        "reason=invalid_concurrency_rejected value=%s", bad_value
    )


def test_bool_concurrency_is_rejected_not_silently_treated_as_one(make_engine):
    engine, registry, _ = make_engine(asynchronous=False)
    with pytest.raises(InvalidConcurrencyError) as exc_info:
        registry.put(
            build_definition("bad", 1, [{"id": "a"}], max_concurrency=True)
        )
    assert exc_info.value.reason == "invalid_concurrency"


def test_other_definition_errors_keep_their_own_reason(make_engine):
    engine, registry, _ = make_engine(asynchronous=False)
    with pytest.raises(InvalidDefinitionError) as exc_info:
        registry.put(build_definition("bad", 1, []))  # 空步骤
    assert exc_info.value.reason == "invalid_definition"


# --------------------------------------------------------------------- #
# 8. 相同定义 + 相同动作重复触发，结论确定一致
# --------------------------------------------------------------------- #


def test_repeated_runs_of_same_definition_have_deterministic_verdict():
    def build():
        from workflow_engine.definitions import DefinitionRegistry
        from workflow_engine.executor import ActionRegistry
        from workflow_engine.engine import WorkflowEngine
        from tests.conftest import InstantSleep

        registry = DefinitionRegistry()
        actions = ActionRegistry()
        eng = WorkflowEngine(
            registry, actions, sleep_protocol=InstantSleep(),
            run_asynchronously=False,
        )
        registry.put(
            build_definition(
                "det",
                1,
                [
                    {"id": "a"},
                    {"id": "b"},
                    {"id": "c", "depends_on": ["a"]},
                    {"id": "d", "depends_on": ["c"]},
                    {"id": "e"},
                ],
                max_concurrency=2,
            )
        )
        # 确定性动作：a 成功、b 成功、e 成功；c、d 不会跑到（a 成功才会）
        actions.register("a", lambda ctx: "A")
        actions.register("b", lambda ctx: "B")
        actions.register("c", lambda ctx: "C")
        actions.register("d", lambda ctx: "D")
        actions.register("e", lambda ctx: "E")
        return eng

    signatures = set()
    for i in range(5):
        eng = build()
        state = eng.trigger(f"run-det-{i}")
        sig = (
            state.status.value,
            tuple(
                (sid, s.status.value, s.failed_by_step)
                for sid, s in sorted(state.steps.items())
            ),
        )
        signatures.add(sig)
    assert len(signatures) == 1, signatures
    run_status, step_sig = next(iter(signatures))
    assert run_status == "succeeded"
    assert all(s[1] == "succeeded" for s in step_sig)
    logger.info("reason=deterministic_verdict signature=%s", step_sig)
