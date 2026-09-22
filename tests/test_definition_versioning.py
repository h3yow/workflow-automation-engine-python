"""场景三：工作流定义在运行期间发生变更。

预期：
- 已触发的在途运行始终按启动时的定义快照执行（步骤集合/依赖/版本不变）。
- 新发布的定义只对之后触发的运行生效。
- 缺必要信息或自相矛盾的定义被拒绝，且原因可区分
  （InvalidDefinitionError.reason == "invalid_definition"）。
"""

from __future__ import annotations

import logging
import threading

import pytest

from workflow_engine.definitions import build_definition
from workflow_engine.models import InvalidDefinitionError, RunStatus, StepStatus

logger = logging.getLogger("workflow_engine.test")


def test_inflight_run_keeps_snapshot_while_new_runs_use_new_version(make_engine):
    engine, registry, actions = make_engine(asynchronous=True)

    # v1：a -> b
    registry.put(
        build_definition(
            "etl", 1, [{"id": "a"}, {"id": "b", "depends_on": ["a"]}]
        )
    )

    # v1 动作：a 先等待放行信号，保证运行在途时可以发布 v2
    a_started = threading.Event()
    release_a = threading.Event()

    def action_a_v1(ctx):
        a_started.set()
        release_a.wait(timeout=5)
        return "a-v1"

    v1_effects: list[str] = []
    actions.register("a", action_a_v1)
    actions.register("b", lambda ctx: v1_effects.append("b-v1") or "b-v1")

    # 触发 v1 运行
    state_v1 = engine.trigger("run-def-old")
    assert a_started.wait(timeout=2), "v1 step a did not start"
    assert state_v1.definition.version == 1

    # ---- 运行在途期间发布 v2：去掉 b，新增 c，并为 c 注册动作 ----
    registry.put(
        build_definition(
            "etl", 2, [{"id": "a"}, {"id": "c", "depends_on": ["a"]}]
        )
    )

    v2_effects: list[str] = []
    actions.register("c", lambda ctx: v2_effects.append("c-v2") or "c-v2")

    # 新运行必须落到 v2
    state_v2 = engine.trigger("run-def-new")

    # 放行 v1 的 a；v2 的 a 共用同一个 release 事件
    release_a.set()

    final_v1 = engine.wait_until_done("run-def-old", timeout=5)
    final_v2 = engine.wait_until_done("run-def-new", timeout=5)

    logger.info(
        "run_id=%s def_version=%d status=%s reason=inflight_snapshot_pinned",
        final_v1.run_id, final_v1.definition.version, final_v1.status.value,
    )
    logger.info(
        "run_id=%s def_version=%d status=%s reason=new_run_uses_new_version",
        final_v2.run_id, final_v2.definition.version, final_v2.status.value,
    )

    # 在途运行仍按 v1 完成：执行 b，且不存在 c 这个步骤
    assert final_v1.status is RunStatus.SUCCEEDED
    assert final_v1.definition.version == 1
    assert set(final_v1.steps) == {"a", "b"}
    assert v1_effects == ["b-v1"]
    assert "c" not in final_v1.steps

    # 新运行按 v2 完成：执行 c，不存在 b
    assert final_v2.status is RunStatus.SUCCEEDED
    assert final_v2.definition.version == 2
    assert set(final_v2.steps) == {"a", "c"}
    assert v2_effects == ["c-v2"]
    assert "b" not in final_v2.steps


@pytest.mark.parametrize(
    "raw_steps, expected_fragment",
    [
        ([], "at least one step"),
        ([{"id": "", "depends_on": []}], "step id"),
        ([{"id": "x"}, {"id": "x"}], "duplicate step id"),
        ([{"id": "x", "depends_on": ["missing"]}], "unknown step"),
        ([{"id": "x", "depends_on": ["x"]}], "cycle"),
    ],
)
def test_invalid_definitions_are_rejected_with_distinguishable_reason(
    make_engine, raw_steps, expected_fragment
):
    engine, registry, _ = make_engine(asynchronous=False)
    with pytest.raises(InvalidDefinitionError) as exc_info:
        registry.put(build_definition("etl", 1, raw_steps))
    assert exc_info.value.reason == "invalid_definition"
    assert expected_fragment in str(exc_info.value)
    logger.info(
        "def_version=1 reason=invalid_definition_rejected detail=%s",
        exc_info.value,
    )


def test_republished_version_is_rejected(make_engine):
    engine, registry, _ = make_engine(asynchronous=False)
    registry.put(build_definition("etl", 1, [{"id": "a"}]))
    with pytest.raises(InvalidDefinitionError) as exc_info:
        registry.put(build_definition("etl", 1, [{"id": "a"}, {"id": "b"}]))
    assert exc_info.value.reason == "invalid_definition"
    assert "already published" in str(exc_info.value)
    # 历史快照保持不变，在途运行依据不受影响
    assert len(registry.get_version(1).steps) == 1
    logger.info("reason=republish_same_version_rejected")
