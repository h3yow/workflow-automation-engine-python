"""测试公共夹具：零退避引擎、日志打印 run_id/step_id/reason。"""

from __future__ import annotations

import logging

import pytest

from workflow_engine.definitions import DefinitionRegistry
from workflow_engine.engine import WorkflowEngine
from workflow_engine.executor import ActionRegistry, SleepProtocol
from workflow_engine.models import EngineDispatchError
from workflow_engine.scheduler import ThreadStepUnitLauncher


class InstantSleep(SleepProtocol):
    """测试替身：退避立即返回，不真实睡眠。"""

    def sleep(self, seconds: float) -> None:
        return None


class FaultyUnitLauncher(ThreadStepUnitLauncher):
    """可注入故障的执行单元启动器：稳定复现引擎派发/启动环节出错。

    - fail_steps：命中步骤的启动请求直接抛 EngineDispatchError（单元不运行）；
    - fail_times：每个命中步骤前 N 次启动报错（默认 1）；
    - raw：抛原始 RuntimeError（模拟系统层异常），由调度器统一包装。
    记录 attempted/failed 两个步骤序列，供断言派发边界。
    """

    def __init__(
        self,
        fail_steps: set[str] | None = None,
        *,
        fail_times: int = 1,
        raw: bool = False,
    ) -> None:
        self._fail_steps = fail_steps or set()
        self._fail_times = fail_times
        self._raw = raw
        self._seen: dict[str, int] = {}
        self.attempted: list[str] = []
        self.failed: list[str] = []

    def launch(self, *, name, target, args):
        step_id = args[0]
        self.attempted.append(step_id)
        seen = self._seen.get(step_id, 0) + 1
        self._seen[step_id] = seen
        if step_id in self._fail_steps and seen <= self._fail_times:
            self.failed.append(step_id)
            if self._raw:
                raise RuntimeError(
                    f"unable to start new execution unit for {step_id} "
                    "(simulated resource exhaustion)"
                )
            raise EngineDispatchError(
                f"simulated dispatch failure for step {step_id}",
                location="step_unit_launch",
                step_id=step_id,
            )
        return super().launch(name=name, target=target, args=args)


@pytest.fixture(autouse=True)
def _engine_logs(caplog):
    """让单测日志中打印运行标识、步骤标识与判定依据（reason=...）。"""
    caplog.set_level(logging.INFO, logger="workflow_engine")
    yield


@pytest.fixture
def make_engine():
    """构造一个绑定注册表/动作表的引擎工厂（默认异步、零退避）。

    unit_launcher 可注入执行单元启动器（如 FaultyUnitLauncher），
    用于复现引擎派发/启动环节自身出错。
    """

    created: list[WorkflowEngine] = []

    def _factory(
        registry: DefinitionRegistry | None = None,
        actions: ActionRegistry | None = None,
        *,
        asynchronous: bool = True,
        unit_launcher=None,
    ) -> tuple[WorkflowEngine, DefinitionRegistry, ActionRegistry]:
        registry = registry or DefinitionRegistry()
        actions = actions or ActionRegistry()
        engine = WorkflowEngine(
            definitions=registry,
            actions=actions,
            sleep_protocol=InstantSleep(),
            time_func=None,
            run_asynchronously=asynchronous,
            unit_launcher=unit_launcher,
        )
        created.append(engine)
        return engine, registry, actions

    yield _factory

    for engine in created:
        engine.shutdown(timeout=5)
