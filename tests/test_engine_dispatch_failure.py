"""场景四：引擎自身在派发/启动执行单元环节出错时的终态收敛。

人为故障注入点：StepLauncher（执行单元启动接缝）在启动某个步骤时抛错，
模拟“引擎准备开新执行单元时抛错 / 资源不足开不出新执行单元”。

断言：
- 运行以 failed 收尾，failed_step_id 指到故障点，根因步骤 error 带
  engine_dispatch_failure 前缀（可与业务步骤失败区分）；
- 出错前已成功的步骤保持 succeeded，不回退、不重标；
- 出错时已经在跑的其它分支自然跑完，成功仍记 succeeded（规则与业务失败一致）；
- 没跑起来的步骤一律 skipped 且 failed_by_step 非空、可逐级回溯到故障步骤，
  不残留 pending/ready 中间态，追溯链条不断；
- 同样定义 + 同样故障反复触发，最终结论稳定一致。
"""

from __future__ import annotations

import logging
import threading
import time

import pytest

from workflow_engine.definitions import build_definition
from workflow_engine.models import RunStatus, StepStatus
from workflow_engine.scheduler import ThreadStepLauncher

logger = logging.getLogger("workflow_engine.test")


class FailingStepLauncher:
    """执行单元启动器测试替身：对指定步骤模拟引擎侧派发/启动失败。

    其余步骤仍交给真实的线程启动器，保证“出错时别的分支正在跑”可发生。
    """

    def __init__(self, failing_step: str, make_error):
        self._inner = ThreadStepLauncher()
        self._failing_step = failing_step
        self._make_error = make_error
        self.failed_attempts = 0

    def launch(self, *, run_id, step_id, target):
        if step_id == self._failing_step:
            self.failed_attempts += 1
            logger.error(
                "run_id=%s step_id=%s reason=simulated_engine_dispatch_failure",
                run_id, step_id,
            )
            raise self._make_error()
        return self._inner.launch(
            run_id=run_id, step_id=step_id, target=target
        )


def _publish(registry):
    # root 先成功；第二波 a/b/... 就绪。
    # a：故障时已在跑的无关分支（自然跑完，成功不回退）；
    # a_tail：依赖 a，故障后不再派发 -> skipped（failed_by 回溯到根因 b）；
    # b：被人为注入启动失败的步骤（引擎侧故障点，不跑业务动作）；
    # slow：故障时还在就绪队列排队 -> skipped。
    registry.put(build_definition("engine-fail", 1, [
        {"id": "root"},
        {"id": "a", "depends_on": ["root"]},
        {"id": "a_tail", "depends_on": ["a"]},
        {"id": "b", "depends_on": ["root"]},
        {"id": "slow", "depends_on": ["root"]},
    ], max_concurrency=2))


def _must_not_run(ctx):
    pytest.fail(f"step {ctx['step_id']} must never execute after engine fault")


def _log_verdict(state):
    logger.info(
        "run_id=%s status=%s failed_step_id=%s reason=engine_fault_run_verdict",
        state.run_id, state.status.value, state.failed_step_id,
    )
    for s in state.steps.values():
        logger.info(
            "run_id=%s step_id=%s status=%s attempts=%d failed_by_step=%s "
            "error=%s reason=step_verdict",
            state.run_id, s.step_id, s.status.value, s.attempts,
            s.failed_by_step, s.error,
        )


def _assert_all_terminal_and_traceable(final):
    """每个步骤必须落在明确终态；被跳过的步骤必须带来源信息。"""
    for sid, step_state in final.steps.items():
        assert step_state.status in (
            StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.SKIPPED,
        ), f"{sid} left in non-terminal state {step_state.status}"
        if step_state.status is StepStatus.SKIPPED:
            assert step_state.failed_by_step is not None, (
                f"{sid} skipped without traceable failed_by_step"
            )
            # 追溯链最终必须能走到一个 FAILED 步骤
            cursor = sid
            seen = set()
            while cursor not in seen:
                seen.add(cursor)
                by = final.steps[cursor].failed_by_step
                if final.steps[cursor].status is StepStatus.FAILED:
                    break
                assert by is not None
                cursor = by
            assert final.steps[cursor].status is StepStatus.FAILED, (
                f"trace chain from {sid} does not reach a FAILED step"
            )


@pytest.mark.parametrize("error_kind", ["launch_error", "resource_exhausted"])
def test_engine_dispatch_failure_converges_all_steps(make_engine, error_kind):
    if error_kind == "launch_error":
        make_error = lambda: RuntimeError("unable to start execution unit")
        expect_detail = "RuntimeError"
    else:
        # 模拟系统资源不足开不出新执行单元（Errno 11；
        # Python 会把该 errno 具体化为 BlockingIOError，属 OSError 子类）
        make_error = lambda: OSError(11, "resource temporarily unavailable")
        expect_detail = "Errno 11"

    launcher = FailingStepLauncher("b", make_error)
    engine, registry, actions = make_engine(
        asynchronous=True, step_launcher=launcher
    )
    _publish(registry)

    a_started = threading.Event()

    actions.register("root", lambda ctx: "r")

    def action_a(ctx):
        a_started.set()
        time.sleep(0.15)  # 拖过 b 启动失败的时刻，成为“故障时在跑的分支”
        return "a"
    actions.register("a", action_a)
    actions.register("a_tail", _must_not_run)
    actions.register("b", _must_not_run)  # 启动即失败，业务动作不应执行
    actions.register("slow", _must_not_run)

    state = engine.trigger(f"run-engine-fail-{error_kind}")
    assert a_started.wait(timeout=2), "branch 'a' did not start in parallel"
    final = engine.wait_until_done(state.run_id, timeout=5)
    _log_verdict(final)

    # 运行整体失败，根因指到可识别的故障点
    assert final.status is RunStatus.FAILED
    assert final.failed_step_id == "b"

    statuses = {sid: s.status for sid, s in final.steps.items()}
    # 出错前已成功的步骤保持成功；故障时在跑的无关分支自然跑完保持成功
    assert statuses["root"] is StepStatus.SUCCEEDED
    assert statuses["a"] is StepStatus.SUCCEEDED
    # 故障步骤：FAILED、未执行业务动作、错误前缀可识别为引擎侧故障
    b = final.steps["b"]
    assert b.status is StepStatus.FAILED
    assert b.attempts == 0
    assert "engine_dispatch_failure" in (b.error or "")
    assert expect_detail in (b.error or "")
    # 没跑起来的步骤：SKIPPED 且 failed_by_step 可回溯到故障点
    assert statuses["a_tail"] is StepStatus.SKIPPED
    assert final.steps["a_tail"].failed_by_step == "b"
    assert statuses["slow"] is StepStatus.SKIPPED
    assert final.steps["slow"].failed_by_step == "b"
    # 启动器只对 b 失败过一次，且失败后不再派发任何新步骤
    assert launcher.failed_attempts == 1
    _assert_all_terminal_and_traceable(final)


def test_engine_dispatch_failure_before_any_step_runs(make_engine):
    """首批步骤（root）启动即失败：全部下游跳过，追溯链不断。"""
    launcher = FailingStepLauncher(
        "root", lambda: RuntimeError("cannot start first execution unit")
    )
    engine, registry, actions = make_engine(
        asynchronous=False, step_launcher=launcher
    )
    _publish(registry)
    for sid in ("root", "a", "a_tail", "b", "slow"):
        actions.register(sid, _must_not_run)

    final = engine.trigger("run-engine-fail-first")
    _log_verdict(final)

    assert final.status is RunStatus.FAILED
    assert final.failed_step_id == "root"
    assert final.steps["root"].status is StepStatus.FAILED
    assert "engine_dispatch_failure" in (final.steps["root"].error or "")
    # 直接下游断裂来源是 root；间接下游可逐级回溯
    assert final.steps["a"].status is StepStatus.SKIPPED
    assert final.steps["a"].failed_by_step == "root"
    assert final.steps["b"].status is StepStatus.SKIPPED
    assert final.steps["b"].failed_by_step == "root"
    assert final.steps["slow"].status is StepStatus.SKIPPED
    assert final.steps["slow"].failed_by_step == "root"
    assert final.steps["a_tail"].status is StepStatus.SKIPPED
    assert final.steps["a_tail"].failed_by_step == "a"  # 逐级回溯到 root
    _assert_all_terminal_and_traceable(final)


def test_repeated_engine_dispatch_failures_have_consistent_verdict(make_engine):
    """同样定义 + 同样故障反复触发：运行结论与每步终态稳定一致。"""

    def _verdict(run_id):
        launcher = FailingStepLauncher(
            "b", lambda: RuntimeError("unable to start execution unit")
        )
        engine, registry, actions = make_engine(
            asynchronous=False, step_launcher=launcher
        )
        _publish(registry)
        actions.register("root", lambda ctx: "r")
        actions.register("a", lambda ctx: "a")
        actions.register("a_tail", _must_not_run)
        actions.register("b", _must_not_run)
        actions.register("slow", _must_not_run)
        state = engine.trigger(run_id)
        _log_verdict(state)
        return (
            state.status,
            state.failed_step_id,
            tuple(sorted(
                (sid, s.status, s.failed_by_step)
                for sid, s in state.steps.items()
            )),
        )

    verdicts = [_verdict(f"run-engine-fail-repeat-{i}") for i in range(3)]
    assert len(set(verdicts)) == 1
    run_status, failed_step, steps = verdicts[0]
    assert run_status is RunStatus.FAILED
    assert failed_step == "b"
    step_map = {sid: (status, by) for sid, status, by in steps}
    assert step_map["root"] == (StepStatus.SUCCEEDED, None)
    assert step_map["a"] == (StepStatus.SUCCEEDED, None)
    assert step_map["b"][0] is StepStatus.FAILED
    assert step_map["a_tail"] == (StepStatus.SKIPPED, "b")
    assert step_map["slow"] == (StepStatus.SKIPPED, "b")
    logger.info("reason=repeatable_engine_fault_verdict samples=%d", 3)


def test_scheduler_unexpected_crash_still_converges(make_engine):
    """调度器自身抛出未预期异常（非启动失败）时，最后防线仍收敛到终态。"""
    from workflow_engine.scheduler import ConcurrentScheduler
    from workflow_engine.models import EngineDispatchError

    engine, registry, actions = make_engine(asynchronous=False)
    _publish(registry)
    for sid in ("root", "a", "a_tail", "b", "slow"):
        actions.register(sid, _must_not_run)

    snapshot = registry.current()
    run_id = "run-scheduler-crash"
    from workflow_engine.models import RunState

    run_state = RunState.new(run_id, snapshot, time.time)
    engine._runs[run_id] = run_state
    engine._done_events[run_id] = threading.Event()
    engine._results[run_id] = {}

    class CrashingScheduler(ConcurrentScheduler):
        def _run_loop(self, worker_threads):
            raise RuntimeError("scheduler internal invariant broken")

    scheduler = CrashingScheduler(
        state=run_state,
        executor=engine._executor,
        results=engine._results[run_id],
        time_func=time.time,
    )
    root = scheduler.run()
    _log_verdict(run_state)

    assert root == "root"  # 拓扑序第一个未决步骤被定为引擎故障根因
    assert run_state.status is not RunStatus.SUCCEEDED
    assert run_state.steps["root"].status is StepStatus.FAILED
    assert EngineDispatchError.reason in (run_state.steps["root"].error or "")
    assert run_state.steps["a"].status is StepStatus.SKIPPED
    assert run_state.steps["a"].failed_by_step == "root"
    assert run_state.steps["a_tail"].failed_by_step == "a"
    _assert_all_terminal_and_traceable(run_state)
