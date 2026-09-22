"""测试公共夹具：零退避引擎、日志打印 run_id/step_id/reason。"""

from __future__ import annotations

import logging
import threading

import pytest

from workflow_engine.definitions import DefinitionRegistry, build_definition
from workflow_engine.engine import WorkflowEngine
from workflow_engine.executor import ActionRegistry, SleepProtocol
from workflow_engine.models import RetryPolicy


class InstantSleep(SleepProtocol):
    """测试替身：退避立即返回，不真实睡眠。"""

    def sleep(self, seconds: float) -> None:
        return None


@pytest.fixture(autouse=True)
def _engine_logs(caplog):
    """让单测日志中打印运行标识、步骤标识与判定依据（reason=...）。"""
    caplog.set_level(logging.INFO, logger="workflow_engine")
    yield


@pytest.fixture
def make_engine():
    """构造一个绑定注册表/动作表的引擎工厂（默认异步、零退避）。"""

    created: list[WorkflowEngine] = []

    def _factory(
        registry: DefinitionRegistry | None = None,
        actions: ActionRegistry | None = None,
        *,
        asynchronous: bool = True,
    ) -> tuple[WorkflowEngine, DefinitionRegistry, ActionRegistry]:
        registry = registry or DefinitionRegistry()
        actions = actions or ActionRegistry()
        engine = WorkflowEngine(
            definitions=registry,
            actions=actions,
            sleep_protocol=InstantSleep(),
            time_func=None,
            run_asynchronously=asynchronous,
        )
        created.append(engine)
        return engine, registry, actions

    yield _factory

    for engine in created:
        engine.shutdown(timeout=5)
