"""场景一：同一运行被并发/重复触发时的幂等保证。

预期：
- N 个并发触发中恰好 1 个被接受，其余全部得到可区分的拒绝
  （DuplicateTriggerError.reason == "duplicate_trigger"）。
- 每个步骤的副作用只执行一次（run_id 相同，只存在一个运行）。
"""

from __future__ import annotations

import logging
import threading

import pytest

from workflow_engine.models import DuplicateTriggerError, RunStatus, StepStatus

logger = logging.getLogger("workflow_engine.test")


def _publish_chain(registry, policy=None):
    from workflow_engine.definitions import build_definition

    registry.put(
        build_definition(
            "order-flow",
            1,
            [
                {"id": "validate"},
                {"id": "charge", "depends_on": ["validate"]},
                {"id": "ship", "depends_on": ["charge"]},
            ],
            retry_policy=policy,
        )
    )


def test_concurrent_duplicate_trigger_is_rejected_and_side_effects_run_once(
    make_engine,
):
    engine, registry, actions = make_engine(
        asynchronous=True,
    )
    _publish_chain(registry)

    side_effects: list[str] = []
    effect_lock = threading.Lock()

    def make_action(name):
        def action(ctx):
            with effect_lock:
                side_effects.append(f"{ctx['run_id']}:{name}")
            return name

        return action

    for step_id in ("validate", "charge", "ship"):
        actions.register(step_id, make_action(step_id))

    run_id = "run-concurrent-1"
    thread_count = 16
    accepted: list[str] = []
    rejected: list[str] = []
    results_lock = threading.Lock()
    start_gate = threading.Barrier(thread_count)

    def trigger() -> None:
        start_gate.wait()  # 尽量放大“同时检查 + 登记”的竞争
        try:
            engine.trigger(run_id)
            with results_lock:
                accepted.append(threading.current_thread().name)
        except DuplicateTriggerError as exc:
            assert exc.reason == "duplicate_trigger"
            assert exc.run_id == run_id
            with results_lock:
                rejected.append(threading.current_thread().name)

    threads = [
        threading.Thread(target=trigger, name=f"trigger-{i}")
        for i in range(thread_count)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state = engine.wait_until_done(run_id, timeout=5)

    # 判定依据打印到单测日志：运行标识 + 接受/拒绝数量 + 最终状态
    logger.info(
        "run_id=%s accepted=%d rejected=%d reason=exactly_one_trigger_accepted",
        run_id, len(accepted), len(rejected),
    )
    for record in state.steps.values():
        logger.info(
            "run_id=%s step_id=%s status=%s attempts=%d reason=side_effect_once",
            run_id, record.step_id, record.status.value, record.attempts,
        )

    assert len(accepted) == 1, f"accepted={accepted}"
    assert len(rejected) == thread_count - 1
    assert state.status is RunStatus.SUCCEEDED
    assert all(s.status is StepStatus.SUCCEEDED for s in state.steps.values())

    # 每个步骤副作用恰好一次
    assert sorted(side_effects) == sorted(
        f"{run_id}:{name}" for name in ("validate", "charge", "ship")
    )
    assert len(side_effects) == 3


def test_sequential_duplicate_trigger_after_completion_is_rejected(make_engine):
    engine, registry, actions = make_engine(asynchronous=False)
    _publish_chain(registry)
    actions.register("validate", lambda ctx: "v")
    actions.register("charge", lambda ctx: "c")
    actions.register("ship", lambda ctx: "s")

    first = engine.trigger("run-seq-1")
    assert first.status is RunStatus.SUCCEEDED

    # 完成后用相同 run_id 再次触发，仍须被区分性拒绝
    with pytest.raises(DuplicateTriggerError) as exc_info:
        engine.trigger("run-seq-1")
    assert exc_info.value.reason == "duplicate_trigger"
    logger.info(
        "run_id=%s reason=post_completion_duplicate_rejected",
        "run-seq-1",
    )
