"""Workflow definition validation and immutable snapshots."""

from __future__ import annotations

from .models import DefinitionError, StepDef, WorkflowDef


def validate_definition(definition: WorkflowDef) -> None:
    """Validate a definition or raise ``DefinitionError`` with a reason.

    Distinguishable reasons:

    * ``missing_name``        – the workflow has no name
    * ``empty_workflow``      – the workflow contains zero steps
    * ``missing_step_id``     – a step has an empty id
    * ``duplicate_step_id``   – two steps share the same id
    * ``unknown_dependency``  – a step depends on a step that does not exist
    * ``cyclic_dependency``   – the dependency graph contains a cycle
    * ``missing_handler``     – a step names no handler
    * ``invalid_retry_policy`` – max_attempts / delays are not positive sane
    """
    if not definition.name:
        raise DefinitionError("missing_name", "workflow name must be a non-empty string")

    steps = definition.steps
    if not steps:
        raise DefinitionError("empty_workflow", f"workflow {definition.name!r} has no steps")

    seen: set[str] = set()
    for step in steps:
        if not step.id:
            raise DefinitionError("missing_step_id", f"step in {definition.name!r} has empty id")
        if step.id in seen:
            raise DefinitionError("duplicate_step_id", f"step id {step.id!r} is used more than once")
        seen.add(step.id)
        if not step.handler:
            raise DefinitionError("missing_handler", f"step {step.id!r} must name a handler")
        policy = step.retry_policy
        if policy.max_attempts < 1:
            raise DefinitionError(
                "invalid_retry_policy",
                f"step {step.id!r}: max_attempts must be >= 1, got {policy.max_attempts}",
            )
        if policy.base_delay_seconds < 0 or policy.max_delay_seconds < 0:
            raise DefinitionError(
                "invalid_retry_policy",
                f"step {step.id!r}: backoff delays must not be negative",
            )

    for step in steps:
        unknown = [dep for dep in step.depends_on if dep not in seen]
        if unknown:
            raise DefinitionError(
                "unknown_dependency",
                f"step {step.id!r} depends on unknown step(s): {', '.join(unknown)}",
            )

    _ensure_acyclic(steps)


def _ensure_acyclic(steps: tuple[StepDef, ...]) -> None:
    """Kahn's algorithm: any step left with an in-edge after processing = cycle."""
    ids = {step.id for step in steps}
    indegree = {step_id: 0 for step_id in ids}
    dependents: dict[str, list[str]] = {step_id: [] for step_id in ids}
    for step in steps:
        for dep in step.depends_on:
            indegree[step.id] += 1
            dependents[dep].append(step.id)

    ready = [step_id for step_id, deg in indegree.items() if deg == 0]
    processed = 0
    while ready:
        current = ready.pop()
        processed += 1
        for downstream in dependents[current]:
            indegree[downstream] -= 1
            if indegree[downstream] == 0:
                ready.append(downstream)

    if processed != len(ids):
        stuck = sorted(step_id for step_id, deg in indegree.items() if deg > 0)
        raise DefinitionError(
            "cyclic_dependency",
            f"dependency cycle detected involving step(s): {', '.join(stuck)}",
        )
