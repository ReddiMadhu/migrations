"""Runtime tests — fixed clock, policy monotonicity, scope isolation."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.clock import FixedClock
from sas2pyspark.services.policy import ExecutionPolicy, DEFAULT_EXECUTION_POLICY, effective_policy, assert_policy_no_wider

from .conftest import make_scopes


async def test_fixed_clock_produces_deterministic_timestamps():
    clock = FixedClock()
    t1 = clock.now()
    t2 = clock.now()
    assert t1 == "2026-01-01T00:00:01.000Z"
    assert t2 == "2026-01-01T00:00:02.000Z"


async def test_fixed_clock_produces_deterministic_uuids():
    clock = FixedClock()
    u1 = clock.uuid()
    u2 = clock.uuid()
    assert u1 == "00000000-0000-4000-8000-000000000001"
    assert u2 == "00000000-0000-4000-8000-000000000002"


async def test_policy_monotonicity_rejects_widening():
    parent = DEFAULT_EXECUTION_POLICY
    with pytest.raises(WorkbenchError) as exc_info:
        effective_policy(parent, {"maximumCostUnits": parent.maximum_cost_units + 1})
    assert exc_info.value.code == WorkbenchErrorCode.POLICY_DENIED


async def test_policy_monotonicity_rejects_network_widening():
    parent = DEFAULT_EXECUTION_POLICY
    with pytest.raises(WorkbenchError) as exc_info:
        effective_policy(parent, {"network": "any"})
    assert exc_info.value.code == WorkbenchErrorCode.POLICY_DENIED


async def test_policy_monotonicity_allows_narrowing():
    parent = DEFAULT_EXECUTION_POLICY
    child = effective_policy(parent, {"maximumCostUnits": parent.maximum_cost_units - 1})
    assert child.maximum_cost_units == parent.maximum_cost_units - 1


async def test_approval_required_is_monotonic_or():
    parent = ExecutionPolicy(
        version="test", network="none", allowedHosts=(), allowedVolumePrefixes=("/v",),
        maximumRuntimeMs=1000, maximumCostUnits=10, maximumDataClassification="internal",
        approvalRequired=True, allowedActions=("spark.submit",),
    )
    child = effective_policy(parent, {"approvalRequired": False})
    assert child.approval_required is True


async def test_scope_hierarchy_creates_nested_scopes(workbench):
    tenant, migration, attempt, candidate = make_scopes(workbench)
    assert tenant.kind == "tenant"
    assert migration.kind == "migration"
    assert attempt.kind == "attempt"
    assert candidate.kind == "candidate"
    assert candidate.parent is attempt
    assert attempt.parent is migration
    assert migration.parent is tenant


async def test_scope_isolation_for_candidate(workbench):
    _, _, _, candidate = make_scopes(workbench)
    workbench.scopes.mount(candidate, "executor.spark", workbench.dummy_spark)
    provider = candidate.get_provider("executor.spark")
    assert provider is not None


async def test_scope_create_rejects_duplicate(workbench):
    tenant = workbench.scopes.create_tenant("tenant-dup")
    with pytest.raises(WorkbenchError) as exc_info:
        workbench.scopes.create_tenant("tenant-dup")
    assert exc_info.value.code == WorkbenchErrorCode.INVALID_INPUT


async def test_child_scope_narrows_policy(workbench):
    tenant = workbench.scopes.create_tenant("tenant-p")
    migration = workbench.scopes.create_child(tenant, "migration", "migration-p")
    assert migration.policy.maximum_cost_units <= tenant.policy.maximum_cost_units
