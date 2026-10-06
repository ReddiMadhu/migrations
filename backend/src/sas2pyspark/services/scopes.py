"""ScopeService — scope hierarchy, policy narrowing, provider isolation."""

from __future__ import annotations

import os
from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.json import assert_safe_segment
from sas2pyspark.services.policy import (
    DEFAULT_EXECUTION_POLICY,
    ExecutionPolicy,
    effective_policy,
)

ScopeKind = str  # "tenant" | "migration" | "attempt" | "candidate" | "execution"


class ScopeRef:
    """A scope reference — identity, policy, workspace, and provider registry."""

    __slots__ = (
        "id", "kind", "parent", "policy", "workspace",
        "evidence_prefix", "artifact_base", "children",
        "_active", "_providers",
    )

    def __init__(
        self,
        *,
        id: str,
        kind: ScopeKind,
        policy: ExecutionPolicy,
        workspace: str,
        evidence_prefix: str,
        artifact_base: str,
        parent: "ScopeRef | None" = None,
    ) -> None:
        self.id = id
        self.kind = kind
        self.parent = parent
        self.policy = policy
        self.workspace = workspace
        self.evidence_prefix = evidence_prefix
        self.artifact_base = artifact_base
        self.children: list[ScopeRef] = []
        self._active = True
        self._providers: dict[str, Any] = {}

    @property
    def active(self) -> bool:
        return self._active

    def mount_provider(self, key: str, provider: Any) -> None:
        self._providers[key] = provider

    def get_provider(self, key: str) -> Any | None:
        cursor: ScopeRef | None = self
        while cursor is not None:
            if key in cursor._providers:
                return cursor._providers[key]
            cursor = cursor.parent
        return None

    def dispose(self) -> None:
        self._active = False
        for child in list(self.children):
            child.dispose()


def scope_chain(scope: ScopeRef) -> list[ScopeRef]:
    result: list[ScopeRef] = []
    cursor: ScopeRef | None = scope
    while cursor is not None:
        result.append(cursor)
        cursor = cursor.parent
    return result


def is_descendant(parent: ScopeRef, child: ScopeRef) -> bool:
    cursor: ScopeRef | None = child
    while cursor is not None:
        if cursor is parent:
            return True
        cursor = cursor.parent
    return False


def assert_scope_owned(parent: ScopeRef, candidate: ScopeRef) -> None:
    if not is_descendant(parent, candidate):
        raise WorkbenchError(
            WorkbenchErrorCode.POLICY_DENIED,
            "scope is not owned by the requested parent",
            {"parent": parent.id, "candidate": candidate.id},
        )


class ScopeService:
    def __init__(self, root_dir: str, base_policy: ExecutionPolicy | None = None) -> None:
        if not root_dir:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "ScopeService requires an absolute rootDir")
        self._root_dir = os.path.abspath(root_dir)
        self._base_policy = base_policy or DEFAULT_EXECUTION_POLICY
        self._scopes: dict[str, ScopeRef] = {}
        self._roots: set[ScopeRef] = set()
        self._disposed = False

    @property
    def root_dir(self) -> str:
        return self._root_dir

    @property
    def base_policy(self) -> ExecutionPolicy:
        return self._base_policy

    def _assert_usable(self) -> None:
        if self._disposed:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "scope service is disposed")

    def create_tenant(self, tenant_id: str, policy: dict[str, Any] | None = None) -> ScopeRef:
        self._assert_usable()
        safe = assert_safe_segment(tenant_id, "tenantId")
        id = f"tenant/{safe}"
        if id in self._scopes:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, f"scope {id} already exists")
        eff = effective_policy(self._base_policy, policy)
        workspace = os.path.join(self._root_dir, "tenants", safe)
        ref = ScopeRef(
            id=id, kind="tenant", policy=eff,
            workspace=workspace, evidence_prefix=id, artifact_base=workspace,
        )
        self._scopes[id] = ref
        self._roots.add(ref)
        return ref

    def create_child(
        self,
        parent: ScopeRef,
        kind: str,
        local_id: str,
        *,
        policy: dict[str, Any] | None = None,
        workspace_suffix: str | None = None,
        isolate_providers: bool | None = None,
    ) -> ScopeRef:
        self._assert_usable()
        self.require(parent)
        safe = assert_safe_segment(local_id, f"{kind}Id")
        id = f"{parent.id}/{kind}/{safe}"
        if id in self._scopes:
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, f"scope {id} already exists")
        eff = effective_policy(parent.policy, policy)
        suffix = workspace_suffix if workspace_suffix else f"{kind}-{safe}"
        if workspace_suffix:
            assert_safe_segment(workspace_suffix, "workspaceSuffix")
        workspace = os.path.join(parent.workspace, kind, suffix)
        do_isolate = isolate_providers if isolate_providers is not None else (kind in ("candidate", "execution"))
        ref = ScopeRef(
            id=id, kind=kind, policy=eff,
            workspace=workspace,
            evidence_prefix=f"{parent.evidence_prefix}/{kind}/{safe}",
            artifact_base=workspace,
            parent=parent,
        )
        parent.children.append(ref)
        self._scopes[id] = ref
        return ref

    def require(self, scope: ScopeRef) -> ScopeRef:
        found = self._scopes.get(scope.id)
        if found is None or found is not scope or not found.active:
            raise WorkbenchError(WorkbenchErrorCode.POLICY_DENIED, "scope is not active in this runtime", {"scope": scope.id})
        return found

    def assert_scope_owned(self, parent: ScopeRef, child: ScopeRef) -> None:
        self.require(parent)
        self.require(child)
        assert_scope_owned(parent, child)

    def get(self, scope_id: str) -> ScopeRef | None:
        scope = self._scopes.get(scope_id)
        return scope if scope and scope.active else None

    def list_all(self) -> list[ScopeRef]:
        return [s for s in self._scopes.values() if s.active]

    def require_by_id(self, scope_id: str) -> ScopeRef:
        scope = self.get(scope_id)
        if scope is None:
            raise WorkbenchError(WorkbenchErrorCode.POLICY_DENIED, "scope is not active", {"scopeId": scope_id})
        return scope

    def mount(self, scope: ScopeRef, key: str, provider: Any) -> None:
        self._assert_usable()
        self.require(scope)
        scope.mount_provider(key, provider)

    def dispose_all(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        for root in list(self._roots):
            root.dispose()
        self._scopes.clear()
        self._roots.clear()
