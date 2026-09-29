"""运行内部有界并发调度的行为测试。

覆盖：
- 互不依赖的步骤确实并行；
- 同时 RUNNING 数不超过定义配置的上限，满额排队；
- 有依赖的步骤严格等全部前置成功后才开始；
- 失败时所有步骤落到可区分、可追溯的终态（failed/skipped/succeeded），
  失败前的成功不回退，在跑的无关节点规则一致，不残留中间态；
- 并发上限随定义版本走，在途运行锁定其启动时的值；
- max_concurrency < 1 在发布时以独立 reason 拒绝。
"""

from __future__ import annotations

import logging
import threading
import time

import pytest

from workflow_engine.definitions import build_definition
from workflow_engine.models import (
    DEFAULT_MAX_CONCURRENCY,
    FatalStepError,
    InvalidConcurrencyError,
    RunStatus,
    StepStatus,
)

logger = logging.getLogger("workflow_engine.test")


class _ConcurrencyTracker:
    """记录动作执行的活跃数、峰值、起止时刻。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.started: dict[str, float] = {}
        self.finished: dict[str, float] = {}

    def wrap(self, step_id: str, inner, delay: float = 0.0):
        def action(ctx):
            with self.lock:
                self.active += 1
                self.peak = max(self.peak, self.active)
                self.started[step_id] = time.monotonic()
            try:
                if delay:
                    time.sleep(delay)
                return inner(ctx)
            finally:
                with self.lock:
                    self.finished[step_id] = time.monotonic()
                    self.active -= 1
        return action


def _log_verdict(state):
    logger.info(
        "run_id=%s status=%s max_concurrency=%d reason=concurrent_run_verdict",
        state.run_id, state.status.value, state.max_concurrency,
    )
    for s in state.steps.values():
        logger.info(
            "run_id=%s step_id=%s status=%s attempts=%d failed_by_step=%s "
            "error=%s reason=step_verdict",
            state.run_id, s.step_id, s.status.value, s.attempts,
            s.failed_by_step, s.error,
        )


def test_independent_steps_run_in_parallel(make_engine):
    engine, registry, actions = make_engine(asynchronous=False)
    registry.put(build_definition("par", 1, [
        {"id": "a"}, {"id": "b"}, {"id": "c"},
    ], max_concurrency=3))

    tracker = _ConcurrencyTracker()
    # 三个无依赖步骤都阻塞在同一屏障上：只有真正并行才可能一起通过
    barrier = threading.Barrier(3)
    for sid in ("a", "b", "c"):
        actions.register(sid, tracker.wrap(sid, lambda ctx: barrier.wait(5)))

    state = engine.trigger("run-parallel")
    _log_verdict(state)

    assert state.status is RunStatus.SUCCEEDED
    assert tracker.peak == 3, f"expected parallel peak 3, got {tracker.peak}"
    assert all(s.status is StepStatus.SUCCEEDED for s in state.steps.values())


def test_running_steps_never_exceed_configured_limit(make_engine):
    engine, registry, actions = make_engine(asynchronous=False)
    limit = 2
    registry.put(build_definition("limited", 1, [
        {"id": f"s{i}"} for i in range(6)
    ], max_concurrency=limit))

    tracker = _ConcurrencyTracker()
    for i in range(6):
        sid = f"s{i}"
        actions.register(sid, tracker.wrap(sid, lambda ctx: None, delay=0.02))

    state = engine.trigger("run-limited")
    _log_verdict(state)

    assert state.status is RunStatus.SUCCEEDED
    assert tracker.peak <= limit, f"peak {tracker.peak} exceeded limit {limit}"
    assert tracker.peak == limit, "独立步骤应至少把并发名额用满"
    assert state.max_concurrency == limit


def test_dependent_steps_wait_for_all_prerequisites(make_engine):
    engine, registry, actions = make_engine(asynchronous=False)
    registry.put(build_definition("diamond", 1, [
        {"id": "root"},
        {"id": "left", "depends_on": ["root"]},
        {"id": "right", "depends_on": ["root"]},
        {"id": "join", "depends_on": ["left", "right"]},
    ], max_concurrency=4))

    tracker = _ConcurrencyTracker()
    actions.register("root", tracker.wrap("root", lambda ctx: "r", delay=0.02))
    actions.register("left", tracker.wrap("left", lambda ctx: "l", delay=0.05))
    actions.register("right", tracker.wrap("right", lambda ctx: "r2", delay=0.02))
    actions.register("join", tracker.wrap("join", lambda ctx: "j"))

    state = engine.trigger("run-diamond")
    _log_verdict(state)

    assert state.status is RunStatus.SUCCEEDED
    # left/right 在 root 成功后才开始
    assert tracker.started["left"] > tracker.finished["root"]
    assert tracker.started["right"] > tracker.finished["root"]
    # join 必须等两个前置全部成功
    assert tracker.started["join"] >= tracker.finished["left"]
    assert tracker.started["join"] >= tracker.finished["right"]
    # left 与 right 时间窗重叠 => 确实并行
    assert tracker.started["left"] < tracker.finished["right"]
    assert tracker.started["right"] < tracker.finished["left"]


def test_failure_converges_all_steps_to_traceable_terminal_states(make_engine):
    engine, registry, actions = make_engine(asynchronous=True)
    # root -> bad -> downstream（失败链）
    # root -> good（失败时已在跑的无关分支：自然跑完并保持成功）
    # late：依赖 root，用屏障精确卡在“已满额排队”位置（失败后必须 skipped）
    registry.put(build_definition("fail", 1, [
        {"id": "root"},
        {"id": "bad", "depends_on": ["root"]},
        {"id": "downstream", "depends_on": ["bad"]},
        {"id": "good", "depends_on": ["root"]},
        {"id": "late", "depends_on": ["root"]},
    ], max_concurrency=2))

    root_done = threading.Event()
    release_bad = threading.Event()
    bad_started = threading.Event()
    tracker = _ConcurrencyTracker()

    actions.register("root", tracker.wrap(
        "root", lambda ctx: root_done.set() or "r", delay=0.02))

    def bad_action(ctx):
        bad_started.set()
        release_bad.wait(timeout=5)
        raise FatalStepError("bad", "boom")
    actions.register("bad", bad_action)

    def must_not_run(ctx):
        pytest.fail(f"step {ctx['step_id']} must never execute after root failure")
    actions.register("downstream", must_not_run)
    actions.register("late", must_not_run)

    # good 占住第二个名额（root 完成时 bad/good 两个就绪者被派发，late 排队）；
    # good 持续在跑到失败之后，验证“在跑的无关分支给一致终态（保持成功）”
    def good_action(ctx):
        # 先确保 bad 已占满名额，再拖过 bad 失败收敛的时刻
        assert bad_started.wait(timeout=2)
        time.sleep(0.15)
        return "g"
    actions.register("good", tracker.wrap("good", good_action))

    state = engine.trigger("run-fail")
    assert root_done.wait(timeout=2)
    assert bad_started.wait(timeout=2), "bad step did not start"
    release_bad.set()

    final = engine.wait_until_done("run-fail", timeout=5)
    _log_verdict(final)

    statuses = {sid: s.status for sid, s in final.steps.items()}
    # 运行整体失败
    assert final.status is RunStatus.FAILED
    # 失败之前已成功的步骤保持成功，不回退
    assert statuses["root"] is StepStatus.SUCCEEDED
    # 根因可追溯到具体步骤
    assert statuses["bad"] is StepStatus.FAILED
    assert "boom" in (final.steps["bad"].error or "")
    # 失败链下游 SKIPPED，failed_by_step 可回溯到根因
    assert statuses["downstream"] is StepStatus.SKIPPED
    assert final.steps["downstream"].failed_by_step == "bad"
    # 失败时已在跑、与原因无关的分支：规则一致——自然跑完并保持成功
    assert statuses["good"] is StepStatus.SUCCEEDED
    # 已满额排队、尚未派发的步骤也有明确终态，断裂点指向根因步骤
    assert statuses["late"] is StepStatus.SKIPPED
    assert final.steps["late"].failed_by_step == "bad"
    # 不允许任何中间态残留
    for sid, step_state in final.steps.items():
        assert step_state.status in (
            StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.SKIPPED,
        ), f"{sid} left in non-terminal state {step_state.status}"
        if step_state.status is StepStatus.SKIPPED:
            assert step_state.failed_by_step is not None


def test_inflight_run_pins_concurrency_across_new_version(make_engine):
    engine, registry, actions = make_engine(asynchronous=True)
    # v1：并发上限 1（严格串行），a 先阻塞，保证运行在途
    registry.put(build_definition("ver", 1, [
        {"id": "a"}, {"id": "b"},
    ], max_concurrency=1))

    a_started = threading.Event()
    release_a = threading.Event()

    def action_a(ctx):
        a_started.set()
        release_a.wait(timeout=5)
        return "a"
    actions.register("a", action_a)
    actions.register("b", lambda ctx: "b")

    run_v1 = engine.trigger("run-v1")
    assert a_started.wait(timeout=2)
    assert run_v1.max_concurrency == 1

    # 在途期间发布 v2：并发上限改为 4，不应影响 run-v1
    registry.put(build_definition("ver", 2, [
        {"id": "a"}, {"id": "b"},
    ], max_concurrency=4))
    assert registry.current().max_concurrency == 4
    assert run_v1.max_concurrency == 1  # 在途运行锁定启动时的值
    assert run_v1.definition.max_concurrency == 1

    # 定义未配置并发上限的旧版本：触发时按引擎兼容默认值锁定
    registry.put(build_definition("ver", 3, [
        {"id": "a"}, {"id": "b"},
    ]))  # max_concurrency=None
    run_v3 = engine.trigger("run-v3", wait=True)
    assert run_v3.definition.version == 3
    assert run_v3.definition.max_concurrency is None
    assert run_v3.max_concurrency == DEFAULT_MAX_CONCURRENCY

    release_a.set()
    final_v1 = engine.wait_until_done("run-v1", timeout=5)
    _log_verdict(final_v1)
    assert final_v1.status is RunStatus.SUCCEEDED
    assert final_v1.max_concurrency == 1
    assert {s.status for s in final_v1.steps.values()} == {StepStatus.SUCCEEDED}


@pytest.mark.parametrize("bad_value", [0, -1, -99])
def test_invalid_concurrency_rejected_at_publish_with_distinct_reason(
    make_engine, bad_value
):
    engine, registry, _ = make_engine(asynchronous=False)
    with pytest.raises(InvalidConcurrencyError) as exc_info:
        registry.put(build_definition(
            "wf", 1, [{"id": "a"}], max_concurrency=bad_value
        ))
    # 独立 reason，可与其它 invalid_definition 校验失败区分
    assert exc_info.value.reason == "invalid_concurrency"
    assert "max_concurrency" in str(exc_info.value)
    # 同时仍是 InvalidDefinitionError 子类，兼容旧捕获逻辑
    from workflow_engine.models import InvalidDefinitionError
    assert isinstance(exc_info.value, InvalidDefinitionError)
    # 非法定义未进入注册表
    assert registry.get_version(1) is None
    logger.info("reason=invalid_concurrency_rejected value=%d", bad_value)


def test_engine_rejects_invalid_default_concurrency(make_engine):
    from workflow_engine.definitions import DefinitionRegistry
    from workflow_engine.engine import WorkflowEngine
    from workflow_engine.executor import ActionRegistry

    with pytest.raises(ValueError):
        WorkflowEngine(
            DefinitionRegistry(), ActionRegistry(), default_max_concurrency=0
        )


def test_repeated_runs_with_same_definition_and_actions_have_consistent_verdict(
    make_engine
):
    """同样定义、同样动作重复触发：运行成败结论与每步终态必须一致。

    说明：独立排队步骤在失败瞬间是「在跑成功」还是「未派发被跳过」取决于
    真实调度时序，但其 failed_by_step 与根因口径一致；本用例因此采用
    确定时序（坏步骤最先失败、好分支不与失败竞争），断言结论可重复。
    """
    def _verdict(engine, registry, actions, run_id):
        registry.put(build_definition("rep", 1, [
            {"id": "root"},
            {"id": "bad", "depends_on": ["root"]},
            {"id": "tail", "depends_on": ["bad"]},
        ], max_concurrency=1))  # 串行名额：坏步骤失败时 tail 必定尚未开始

        def bad(ctx):
            raise FatalStepError("bad", "always boom")
        actions.register("root", lambda ctx: "r")
        actions.register("bad", bad)
        actions.register("tail", lambda ctx: pytest.fail("tail must not run"))
        state = engine.trigger(run_id, wait=True)
        return (
            state.status,
            tuple(sorted(
                (sid, s.status, s.failed_by_step)
                for sid, s in state.steps.items()
            )),
        )

    verdicts = []
    for i in range(5):
        engine, registry, actions = make_engine(asynchronous=False)
        verdicts.append(_verdict(engine, registry, actions, f"run-rep-{i}"))
        engine.shutdown()

    assert len(set(verdicts)) == 1
    run_status, steps = verdicts[0]
    assert run_status is RunStatus.FAILED
    step_map = {sid: (status, by) for sid, status, by in steps}
    assert step_map["root"] == (StepStatus.SUCCEEDED, None)
    assert step_map["bad"][0] is StepStatus.FAILED
    assert step_map["tail"] == (StepStatus.SKIPPED, "bad")
    logger.info("reason=repeatable_verdict samples=%d verdict=%s", 5, steps)
