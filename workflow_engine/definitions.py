"""工作流定义的构建、校验与版本管理。"""

from __future__ import annotations

import threading

from .models import (
    InvalidDefinitionError,
    RetryPolicy,
    StepDef,
    WorkflowDef,
)


def _validate_retry_policy(policy: RetryPolicy) -> None:
    if policy.max_attempts < 1:
        raise InvalidDefinitionError(
            f"retry_policy.max_attempts must be >= 1, got {policy.max_attempts}"
        )
    if policy.base_delay_seconds < 0:
        raise InvalidDefinitionError("retry_policy.base_delay_seconds must be >= 0")
    if policy.backoff_multiplier < 1:
        raise InvalidDefinitionError(
            "retry_policy.backoff_multiplier must be >= 1"
        )
    if policy.max_delay_seconds < 0:
        raise InvalidDefinitionError("retry_policy.max_delay_seconds must be >= 0")


def validate_definition(definition: WorkflowDef) -> None:
    """校验定义完整性与一致性；非法时抛出 InvalidDefinitionError。

    拒绝场景（每条对应可区分的错误消息）：
    - 名称为空 / 版本号非法
    - 步骤列表为空
    - 步骤 id 为空或重复
    - depends_on 引用了不存在的步骤
    - 依赖图存在环（含自依赖）
    - 步骤级 max_attempts 覆盖值 < 1
    - 重试退避参数非法
    """
    if not definition.name or not definition.name.strip():
        raise InvalidDefinitionError("workflow name must not be empty")
    if definition.version < 1:
        raise InvalidDefinitionError(
            f"definition version must be >= 1, got {definition.version}"
        )

    _validate_retry_policy(definition.retry_policy)

    if not definition.steps:
        raise InvalidDefinitionError("workflow must contain at least one step")

    seen: set[str] = set()
    for step in definition.steps:
        if not step.id or not step.id.strip():
            raise InvalidDefinitionError("step id must not be empty")
        if step.id in seen:
            raise InvalidDefinitionError(f"duplicate step id: {step.id!r}")
        seen.add(step.id)
        if step.max_attempts is not None and step.max_attempts < 1:
            raise InvalidDefinitionError(
                f"step {step.id!r} max_attempts must be >= 1, "
                f"got {step.max_attempts}"
            )

    known = definition.step_ids()
    for step in definition.steps:
        for dep in step.depends_on:
            if dep not in known:
                raise InvalidDefinitionError(
                    f"step {step.id!r} depends on unknown step {dep!r}"
                )

    _ensure_acyclic(definition)


def _ensure_acyclic(definition: WorkflowDef) -> None:
    """DFS 三色标记检测依赖环（含自依赖）。"""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {step.id: WHITE for step in definition.steps}

    def visit(node_id: str, stack: list[str]) -> None:
        color[node_id] = GRAY
        stack.append(node_id)
        node = definition.get_step(node_id)
        assert node is not None
        for dep in node.depends_on:
            if color[dep] == GRAY:
                cycle = stack[stack.index(dep):] + [dep]
                raise InvalidDefinitionError(
                    "dependency cycle detected: " + " -> ".join(cycle)
                )
            if color[dep] == WHITE:
                visit(dep, stack)
        stack.pop()
        color[node_id] = BLACK

    for step in definition.steps:
        if color[step.id] == WHITE:
            visit(step.id, [])


class DefinitionRegistry:
    """保存工作流定义的当前版本与全部历史版本快照。

    定义对象本身不可变；已发布版本不可被覆盖（保证在途运行拿到的快照稳定）。
    """

    def __init__(self) -> None:
        self._versions: dict[int, WorkflowDef] = {}
        self._current_version: int | None = None
        self._lock = threading.Lock()

    def put(self, definition: WorkflowDef) -> WorkflowDef:
        """校验后发布新版本；非法定义抛出 InvalidDefinitionError。

        已存在的版本号不可重复发布（自相矛盾的变更会被拒绝）。
        """
        validate_definition(definition)
        with self._lock:
            if definition.version in self._versions:
                raise InvalidDefinitionError(
                    f"definition version {definition.version} already published; "
                    "publish a new version instead of overwriting"
                )
            self._versions[definition.version] = definition
            if (
                self._current_version is None
                or definition.version > self._current_version
            ):
                self._current_version = definition.version
            return definition

    def current(self) -> WorkflowDef:
        """获取当前版本；从未发布过时抛 InvalidDefinitionError。"""
        with self._lock:
            if self._current_version is None:
                raise InvalidDefinitionError("no workflow definition published yet")
            return self._versions[self._current_version]

    def get_version(self, version: int) -> WorkflowDef | None:
        """按版本号获取历史快照。"""
        with self._lock:
            return self._versions.get(version)


def build_definition(
    name: str,
    version: int,
    raw_steps: list[dict],
    retry_policy: RetryPolicy | None = None,
) -> WorkflowDef:
    """从朴素数据结构构建 WorkflowDef（只构造对象，不做发布）。"""
    steps = tuple(
        StepDef(
            id=raw["id"],
            depends_on=tuple(raw.get("depends_on", []) or []),
            max_attempts=raw.get("max_attempts"),
        )
        for raw in raw_steps
    )
    return WorkflowDef(
        name=name,
        version=version,
        steps=steps,
        retry_policy=retry_policy or RetryPolicy(),
    )
