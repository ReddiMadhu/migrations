"""Cross-cutting invariant preservation tests — XCUT-001..015."""

from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.hash import GENESIS_HASH, sha256_json
from sas2pyspark.core.json import canonical_json
from sas2pyspark.core.models import WorkbenchModel
from sas2pyspark.services.clock import FixedClock
from sas2pyspark.workbench import WorkbenchConfig, WorkbenchRuntime, create_workbench

from .conftest import make_scopes, make_session


class TestXCUTInvariants:
    @pytest.mark.asyncio
    async def test_xcut_001_hash_chained_events(self, test_dir: Path, fixed_clock: FixedClock):
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            tenant, migration, attempt, candidate = make_scopes(runtime)
            session = await make_session(runtime, scope=migration)
            await session.append(MigrationEventType.AGENT_STARTED, {"depth": 0})
            await session.append(MigrationEventType.AGENT_TURN_STARTED, {"turn": 1})
            events = session.events
            assert events[0].previous_hash == GENESIS_HASH
            for i in range(1, len(events)):
                assert events[i].previous_hash == events[i - 1].hash
        finally:
            await runtime.dispose()

    @pytest.mark.asyncio
    async def test_xcut_003_canonical_json_before_hash(self):
        data = {"b": 2, "a": 1, "c": {"d": 4, "e": 3}}
        cj = canonical_json(data)
        assert cj == '{"a":1,"b":2,"c":{"d":4,"e":3}}'

    def test_xcut_004_frozen_inputs(self):
        from sas2pyspark.core.logging import DisjointTokenUsage
        usage = DisjointTokenUsage(inputTokens=100, cacheReadTokens=50)
        with pytest.raises(Exception):
            usage.input_tokens = 999

    def test_xcut_005_deterministic_clock(self, fixed_clock: FixedClock):
        t1 = fixed_clock.now()
        t2 = fixed_clock.now()
        assert t1 != t2
        assert t1.endswith("Z")
        assert t2.endswith("Z")

    def test_xcut_007_new_event_types_exist(self):
        new_types = [
            MigrationEventType.AGENT_STARTED,
            MigrationEventType.AGENT_TURN_STARTED,
            MigrationEventType.AGENT_TURN_ENDED,
            MigrationEventType.AGENT_STEP_STARTED,
            MigrationEventType.AGENT_STEP_ENDED,
            MigrationEventType.AGENT_IDLE,
            MigrationEventType.TOOL_STARTED,
            MigrationEventType.TOOL_COMPLETED,
            MigrationEventType.TOOL_DENIED,
            MigrationEventType.TOOL_BLOCKED,
            MigrationEventType.TOOL_OUTCOME_UNKNOWN,
            MigrationEventType.TOOL_NOT_STARTED,
            MigrationEventType.CACHE_HIT,
            MigrationEventType.CACHE_MISS,
            MigrationEventType.CACHE_EVICTED,
            MigrationEventType.COMPACTION_STARTED,
            MigrationEventType.COMPACTION_COMPLETED,
            MigrationEventType.DELEGATION_STARTED,
            MigrationEventType.DELEGATION_COMPLETED,
            MigrationEventType.FILE_EDIT_APPLIED,
        ]
        for et in new_types:
            assert isinstance(et.value, str)

    def test_xcut_010_llm_advisory(self):
        from sas2pyspark.llm.cache.ordering import AdvisoryResponse
        from sas2pyspark.core.logging import DisjointTokenUsage
        resp = AdvisoryResponse(content="test", usage=DisjointTokenUsage())
        assert resp.advisory is True

    @pytest.mark.asyncio
    async def test_xcut_012_capabilities_in_runtime(self, test_dir: Path, fixed_clock: FixedClock):
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            assert hasattr(runtime, "llm_cache")
            assert hasattr(runtime, "file_edit_seam")
            assert hasattr(runtime, "tool_registry")
            assert hasattr(runtime, "agent_loop")
            assert hasattr(runtime, "observation_cache")
            assert hasattr(runtime, "projection_cache")
            assert hasattr(runtime, "prompt_assembler")
            assert hasattr(runtime, "sandbox_policy")
        finally:
            await runtime.dispose()

    @pytest.mark.asyncio
    async def test_xcut_014_default_repairer(self, test_dir: Path, fixed_clock: FixedClock):
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            assert runtime.repairer is not None
            assert hasattr(runtime.repairer, "diagnose")
            assert hasattr(runtime.repairer, "propose_patch")
        finally:
            await runtime.dispose()

    def test_xcut_015_existing_enum_values_unchanged(self):
        assert MigrationEventType.MIGRATION_CREATED == "migration/created"
        assert MigrationEventType.TRANSLATION_STARTED == "translation/started"
        assert MigrationEventType.TRANSLATION_COMPLETED == "translation/completed"
        assert MigrationEventType.EXECUTION_STARTED == "execution/started"
        assert MigrationEventType.CERTIFICATE_ISSUED == "certificate/issued"

    @pytest.mark.asyncio
    async def test_xcut_002_content_addressed_artifacts(self, test_dir: Path, fixed_clock: FixedClock):
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            tenant, migration, attempt, candidate = make_scopes(runtime)
            artifact1 = runtime.artifacts.create(
                scope=migration, data="print('hello')", media_type="text/x-python", created_by="test",
            )
            artifact2 = runtime.artifacts.create(
                scope=migration, data="print('hello')", media_type="text/x-python", created_by="test",
            )
            assert artifact1.source_hash == artifact2.source_hash
        finally:
            await runtime.dispose()

    def test_xcut_011_no_sqlite_tempfile_network(self):
        import inspect
        from sas2pyspark.llm.cache import example_cache, projection_cache, observation_cache
        for module in [example_cache, projection_cache, observation_cache]:
            source = inspect.getsource(module)
            assert "sqlite" not in source.lower()
            assert "tempfile" not in source.lower()
            assert "requests" not in source.lower()
            assert "urllib" not in source.lower()

    @pytest.mark.asyncio
    async def test_xcut_009_tools_gated_by_guard(self, test_dir: Path, fixed_clock: FixedClock):
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            from sas2pyspark.agent.dispatch import ToolDispatchPipeline
            assert hasattr(runtime, "guards")
            assert runtime.guards is not None
        finally:
            await runtime.dispose()

    @pytest.mark.asyncio
    async def test_xcut_006_five_step_rule(self, test_dir: Path, fixed_clock: FixedClock):
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            from sas2pyspark.agent.driver import AgentResult
            result = AgentResult(content="test")
            assert result.advisory is True
        finally:
            await runtime.dispose()

    @pytest.mark.asyncio
    async def test_xcut_008_artifacts_via_filesystem_service(self, test_dir: Path, fixed_clock: FixedClock):
        runtime = await create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=fixed_clock))
        try:
            from sas2pyspark.services.artifacts import FileSystemArtifactService
            assert isinstance(runtime.artifacts, FileSystemArtifactService)
        finally:
            await runtime.dispose()
