"""Human approval binding freshness tests."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.approvals import binding_for
from sas2pyspark.services.policy import policy_fingerprint

from .conftest import make_scopes


async def test_approval_rejects_changed_environment_or_input(workbench):
    _, _, _, scope = make_scopes(workbench)
    artifact = workbench.artifacts.create(
        scope=scope,
        data="candidate bytes",
        media_type="text/x-python",
        created_by="hitl-test",
    )
    policy = scope.policy
    fingerprint = policy_fingerprint(policy)
    original = binding_for(scope, "spark.submit", artifact, policy, fingerprint, "env-a", "snapshot-a")
    workbench.approvals.grant(original)

    stale_environment = binding_for(scope, "spark.submit", artifact, policy, fingerprint, "env-b", "snapshot-a")
    with pytest.raises(WorkbenchError) as environment_error:
        workbench.approvals.assert_approved(stale_environment)
    assert environment_error.value.code == WorkbenchErrorCode.STALE_APPROVAL

    stale_input = binding_for(scope, "spark.submit", artifact, policy, fingerprint, "env-a", "snapshot-b")
    with pytest.raises(WorkbenchError) as input_error:
        workbench.approvals.assert_approved(stale_input)
    assert input_error.value.code == WorkbenchErrorCode.STALE_APPROVAL