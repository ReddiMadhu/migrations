"""GuardService — deny-only guards that can reject but never authorize."""

from __future__ import annotations

import logging
import re
from typing import Callable, Sequence

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.artifacts import ArtifactRecord
from sas2pyspark.services.scopes import ScopeRef

_logger = logging.getLogger(__name__)


class GuardRequest:
    __slots__ = ("scope", "action", "artifact", "input_snapshot_id", "environment_fingerprint", "policy_fingerprint")

    def __init__(
        self, *,
        scope: ScopeRef, action: str, artifact: ArtifactRecord,
        input_snapshot_id: str, environment_fingerprint: str, policy_fingerprint: str,
    ) -> None:
        self.scope = scope
        self.action = action
        self.artifact = artifact
        self.input_snapshot_id = input_snapshot_id
        self.environment_fingerprint = environment_fingerprint
        self.policy_fingerprint = policy_fingerprint


class GuardDecision:
    __slots__ = ("allow", "reason_code", "detail")

    def __init__(self, *, allow: bool, reason_code: str | None = None, detail: str | None = None) -> None:
        self.allow = allow
        self.reason_code = reason_code
        self.detail = detail


Guard = Callable[[GuardRequest], GuardDecision]


class GuardDenial:
    __slots__ = ("guard_id", "reason_code", "detail", "scope_id", "action", "policy_fingerprint")

    def __init__(self, *, guard_id: str, reason_code: str, detail: str, scope_id: str, action: str, policy_fingerprint: str) -> None:
        self.guard_id = guard_id
        self.reason_code = reason_code
        self.detail = detail
        self.scope_id = scope_id
        self.action = action
        self.policy_fingerprint = policy_fingerprint


_GUARD_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class GuardService:
    def __init__(self, *, fail_closed: bool = True) -> None:
        self._guards: list[tuple[str, Guard]] = []
        self._denial_log: list[GuardDenial] = []
        self._fail_closed = fail_closed
        if not fail_closed:
            _logger.warning(
                "GuardService constructed with fail_closed=False; "
                "empty guard list will allow all requests — "
                "this must only be used in explicit development/test contexts"
            )

    def register(self, id: str, guard: Guard) -> Callable[[], None]:
        if not _GUARD_ID_RE.match(id):
            raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "guard id is invalid")
        entry = (id, guard)
        self._guards.append(entry)

        def unregister() -> None:
            try:
                self._guards.remove(entry)
            except ValueError:
                pass

        return unregister

    def evaluate(self, request: GuardRequest) -> None:
        if not self._guards and self._fail_closed:
            raise WorkbenchError(
                WorkbenchErrorCode.POLICY_DENIED,
                "no guards registered and fail_closed=True; denying by default",
                {"reasonCode": WorkbenchErrorCode.NO_GUARDS_REGISTERED.value, "scopeId": request.scope.id, "action": request.action},
            )
        for id, guard in self._guards:
            decision = guard(request)
            if decision.allow:
                continue
            denial = GuardDenial(
                guard_id=id,
                reason_code=decision.reason_code or "GUARD_DENIED",
                detail=decision.detail or "guard denied request",
                scope_id=request.scope.id,
                action=request.action,
                policy_fingerprint=request.policy_fingerprint,
            )
            self._denial_log.append(denial)
            raise WorkbenchError(
                WorkbenchErrorCode.POLICY_DENIED,
                f"guard {id} denied request",
                {"guardId": denial.guard_id, "reasonCode": denial.reason_code, "detail": denial.detail, "scopeId": denial.scope_id, "action": denial.action, "policyFingerprint": denial.policy_fingerprint},
            )

    def denials(self) -> Sequence[GuardDenial]:
        return tuple(self._denial_log)
