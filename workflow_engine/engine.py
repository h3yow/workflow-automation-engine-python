"""The workflow execution engine: trigger, schedule and advance runs."""

from __future__ import annotations

import copy
import logging
import threading

from .definitions import validate_definition
from .executor import StepExecutor
from .models import (
    DefinitionError,
    RejectReason,
    RunRecord,
    RunStatus,
    StepRuntime,
    StepStatus,
    TriggerRejected,
    WorkflowDef,
)
from .store import RunAlreadyExists, RunStore

logger = logging.getLogger("workflow_engine")


class WorkflowEngine:
    """Schedules DAG steps inside a process, synchronously per run.

    Guarantees:

    * a step starts only after **all** its upstream steps succeeded;
    * the per-run lock makes concurrent/repeated triggers mutually exclusive:
      exactly one trigger executes the run, every other trigger is rejected
      with ``RejectReason.ALREADY_RUNNING`` and no side effect is repeated;
    * a failed step (non-retryable or retries exhausted) terminates as FAILED
      and every still-pending downstream step terminates as SKIPPED; no branch
      is ever marked successful;
    * the definition is snapshotted (deep-copied) at trigger time, so later
      ``register_definition`` calls never affect an in-flight/finished run.
    """

    def __init__(self, store: RunStore | None = None, executor: StepExecutor | None = None) -> None:
        self.store = store if store is not None else RunStore()
        self.executor = executor if executor is not None else StepExecutor()
        self._definitions: dict[str, WorkflowDef] = {}
        self._def_lock = threading.Lock()

    # -- definition management ------------------------------------------------

    def register_definition(self, definition: WorkflowDef) -> None:
        """Register a (typically higher) version of a workflow.

        New triggers bind to the newest version. Re-registering an existing
        version number with different content is rejected (``version_conflict``)
        rather than silently mutating snapshots that older runs may rely on.
        """
        validate_definition(definition)
        with self._def_lock:
            existing = self._definitions.get(definition.name)
            if existing is not None and existing.version == definition.version \
                    and existing != definition:
                raise DefinitionError(
                    "version_conflict",
                    f"workflow {definition.name!r} version {definition.version} "
                    "already exists with different content",
                )
            if existing is None or definition.version >= existing.version:
                self._definitions[definition.name] = definition
                logger.info(
                    "workflow=%s registered version=%d (newest active)",
                    definition.name, definition.version,
                )
            else:
                logger.info(
                    "workflow=%s registered older version=%d; active stays version=%d",
                    definition.name, definition.version, existing.version,
                )

    def current_definition(self, workflow_name: str) -> WorkflowDef | None:
        with self._def_lock:
            return self._definitions.get(workflow_name)

    # -- run lifecycle --------------------------------------------------------

    def create_run(self, run_id: str, workflow_name: str) -> RunRecord:
        """Create a PENDING run. The definition snapshot is bound at trigger."""
        record = RunRecord(run_id=run_id, workflow_name=workflow_name)
        try:
            self.store.create(record)
        except RunAlreadyExists as exc:
            raise TriggerRejected(
                RejectReason.RUN_ALREADY_EXISTS, run_id, "run id already created",
            ) from exc
        logger.info("run_id=%s created for workflow=%s (pending trigger)", run_id, workflow_name)
        return record

    def trigger(self, run_id: str) -> RunRecord:
        """Trigger a run; duplicate/concurrent triggers are distinguishable rejects."""
        record = self.store.get(run_id)
        if record is None:
            raise TriggerRejected(RejectReason.RUN_NOT_FOUND, run_id, "no such run")

        # The per-run RLock is held across arbitration AND the whole execution,
        # so a concurrent trigger observes RUNNING (or a terminal state) instead
        # of racing step state; it blocks here and is then rejected. No step side
        # effect can ever run twice for the same run.
        with self.store.lock(run_id):
            record.trigger_count += 1
            if record.status != RunStatus.PENDING:
                logger.warning(
                    "run_id=%s rejected basis=already_%s trigger_count=%d "
                    "reason=%s -> no side effect executed",
                    run_id, record.status.value, record.trigger_count,
                    RejectReason.ALREADY_RUNNING.value,
                )
                raise TriggerRejected(
                    RejectReason.ALREADY_RUNNING,
                    run_id,
                    f"run is already {record.status.value}; side effects are not repeated",
                )

            workflow_name = record.workflow_name
            definition = self.current_definition(workflow_name)
            if definition is None:
                # Status is still PENDING, so the trigger may be retried later
                # after the workflow has been registered.
                raise TriggerRejected(
                    RejectReason.WORKFLOW_NOT_FOUND, run_id,
                    f"workflow {workflow_name!r} is not registered",
                )

            # Deep-copy snapshot: in-flight run is immune to later definition changes.
            snapshot = copy.deepcopy(definition)
            record.definition = snapshot
            record.status = RunStatus.RUNNING
            record.steps = {
                step.id: StepRuntime(step_id=step.id) for step in snapshot.steps
            }
            logger.info(
                "run_id=%s accepted basis=first_trigger workflow=%s version=%d steps=%d",
                run_id, snapshot.name, snapshot.version, len(snapshot.steps),
            )
            self.store.save(record)

            self._execute_locked(record)
            self.store.save(record)

        logger.info(
            "run_id=%s finished status=%s basis=terminal_state step_summary=%s",
            run_id, record.status.value,
            {sid: st.status.value for sid, st in record.steps.items()},
        )
        return record

    def get_run(self, run_id: str) -> RunRecord:
        record = self.store.get(run_id)
        if record is None:
            raise TriggerRejected(RejectReason.RUN_NOT_FOUND, run_id, "no such run")
        return record

    # -- scheduling -----------------------------------------------------------

    def _execute_locked(self, record: RunRecord) -> None:
        """Drive every ready step until the run reaches a terminal state.

        Called with the run lock held; a concurrent trigger therefore blocks
        here until the run is terminal, then sees FAILED/SUCCEEDED and is
        rejected. No step side effect can run twice for the same run.
        """
        definition = record.definition
        assert definition is not None

        while True:
            ready = [
                step for step in definition.steps
                if record.steps[step.id].status == StepStatus.PENDING
                and all(
                    record.steps[dep].status == StepStatus.SUCCEEDED
                    for dep in step.depends_on
                )
            ]
            if not ready:
                break

            for step in ready:
                runtime = record.steps[step.id]
                runtime.status = StepStatus.RUNNING
                logger.info(
                    "run_id=%s step_id=%s state=running basis=all_dependencies_succeeded deps=%s",
                    record.run_id, step.id, list(step.depends_on),
                )
                self.store.save(record)

                result = self.executor.execute(
                    step.handler, step.retry_policy, record.run_id, step.id,
                )
                runtime.attempts = result.attempts
                if result.success:
                    runtime.status = StepStatus.SUCCEEDED
                    runtime.output = result.output
                    runtime.error = None
                    logger.info(
                        "run_id=%s step_id=%s state=succeeded basis=execution_result attempts=%d",
                        record.run_id, step.id, result.attempts,
                    )
                else:
                    runtime.status = StepStatus.FAILED
                    runtime.error = result.error
                    logger.warning(
                        "run_id=%s step_id=%s state=failed basis=execution_result attempts=%d error=%r",
                        record.run_id, step.id, result.attempts, result.error,
                    )
                    self._abort_remaining(record, step.id)
                    record.status = RunStatus.FAILED
                    self.store.save(record)
                    return
                self.store.save(record)

        # No pending step remains: a DAG with no failures has all SUCCEEDED.
        # (Failure aborts return early above, so here every step is SUCCEEDED.)
        record.status = RunStatus.SUCCEEDED

    def _abort_remaining(self, record: RunRecord, failed_step_id: str) -> None:
        """After a failure, terminate every not-yet-finished step.

        Two distinguishable skip bases:

        * ``upstream_failure`` – the step is (transitively) downstream of the
          failed step;
        * ``run_aborted`` – every other unstarted/independent step.

        No PENDING step is ever left behind, and none is marked SUCCEEDED.
        """
        definition = record.definition
        assert definition is not None
        dependents: dict[str, list[str]] = {step.id: [] for step in definition.steps}
        for step in definition.steps:
            for dep in step.depends_on:
                dependents[dep].append(step.id)

        # Transitive closure downstream of the failed step -> upstream_failure.
        downstream: set[str] = set()
        stack = list(dependents[failed_step_id])
        while stack:
            current = stack.pop()
            if current not in downstream:
                downstream.add(current)
                stack.extend(dependents[current])

        for step in definition.steps:
            runtime = record.steps[step.id]
            if runtime.status not in (StepStatus.PENDING, StepStatus.RUNNING):
                continue
            if step.id in downstream:
                runtime.status = StepStatus.SKIPPED
                runtime.error = (
                    f"skipped: upstream step {failed_step_id!r} failed (basis=upstream_failure)"
                )
                logger.warning(
                    "run_id=%s step_id=%s state=skipped basis=upstream_failure failed_step=%s",
                    record.run_id, step.id, failed_step_id,
                )
            else:
                runtime.status = StepStatus.SKIPPED
                runtime.error = (
                    f"skipped: run aborted after {failed_step_id!r} failed (basis=run_aborted)"
                )
                logger.warning(
                    "run_id=%s step_id=%s state=skipped basis=run_aborted failed_step=%s",
                    record.run_id, step.id, failed_step_id,
                )

