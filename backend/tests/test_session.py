"""Session tests — hash chain, projection, replay, tamper detection."""

from __future__ import annotations

import pytest

from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.json import canonical_json
from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.artifacts import artifact_reference
from sas2pyspark.services.clock import FixedClock
from sas2pyspark.workbench import WorkbenchConfig, create_workbench

from .conftest import make_scopes, make_session



async def test_replay_preserves_events_and_projection(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    artifact = workbench.artifacts.create(scope=candidate, data="select 1;", media_type="text/x-sas", created_by="test")
    await session.append(MigrationEventType.ARTIFACT_CREATED, {"artifact": artifact_reference(artifact).model_dump(by_alias=True)})
    await session.append(MigrationEventType.TRANSLATION_STARTED, {"translationId": "tr-1", "strategy": "fixture"})
    await session.append(MigrationEventType.TRANSLATION_COMPLETED, {"translationId": "tr-1", "candidateArtifact": artifact_reference(artifact).model_dump(by_alias=True), "confidence": "fixture"})
    replay = await workbench.sessions.replay(session.id)
    assert canonical_json([e.model_dump(by_alias=True) for e in replay.events]) == canonical_json([e.model_dump(by_alias=True) for e in session.events])
    assert replay.projection["state"] == "CANDIDATE_READY"


async def test_hash_chain_links_to_genesis(workbench):
    from sas2pyspark.core.hash import GENESIS_HASH
    make_scopes(workbench)
    session = await make_session(workbench)
    assert len(session.events) == 1
    assert session.events[0].previous_hash == GENESIS_HASH
    assert session.events[0].sequence == 1


async def test_event_sequence_is_contiguous(workbench):
    make_scopes(workbench)
    session = await make_session(workbench)
    await session.append(MigrationEventType.TRANSLATION_STARTED, {"translationId": "tr-1", "strategy": "fixture"})
    assert session.events[-1].sequence == 2


async def test_evidence_verify_detects_tampering(workbench):
    from sas2pyspark.core.events import MigrationEvent
    make_scopes(workbench)
    session = await make_session(workbench)
    events = session.events
    tampered_data = dict(events[0].data)
    tampered_data["title"] = "evil migration"
    tampered = MigrationEvent(**{**events[0].model_dump(by_alias=True), "data": tampered_data})
    with pytest.raises(WorkbenchError) as exc_info:
        workbench.evidence.verify([tampered])
    assert exc_info.value.code == WorkbenchErrorCode.EVIDENCE_TAMPERED


async def test_safe_boundary_with_open_execution(workbench):
    _, _, attempt, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    artifact = workbench.artifacts.create(scope=candidate, data="x", media_type="text/plain", created_by="test")
    await session.append(MigrationEventType.EXECUTION_STARTED, {
        "executionId": "exec-open", "provider": "executor.spark", "action": "spark.submit",
        "artifact": artifact_reference(artifact).model_dump(by_alias=True),
        "inputSnapshotId": "input-1", "policyFingerprint": "policy-hash",
        "environmentFingerprint": "env-1", "idempotencyKey": "idem-open",
    })
    assert workbench.sessions.safe_boundary(session) == 1


async def test_projection_tracks_execution_outcomes(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    artifact = workbench.artifacts.create(scope=candidate, data="x", media_type="text/plain", created_by="test")
    await session.append(MigrationEventType.EXECUTION_STARTED, {
        "executionId": "exec-1", "provider": "executor.spark", "action": "spark.submit",
        "artifact": artifact_reference(artifact).model_dump(by_alias=True),
        "inputSnapshotId": "input-1", "policyFingerprint": "policy-hash",
        "environmentFingerprint": "env-1", "idempotencyKey": "idem-1",
    })
    await session.append(MigrationEventType.EXECUTION_COMPLETED, {
        "executionId": "exec-1", "provider": "executor.spark", "outcome": "SUCCEEDED", "runId": "run-1",
    })
    assert session.projection["state"] == "RECONCILING"
    assert session.projection["executionOutcomes"]["exec-1"] == "SUCCEEDED"
    assert len(session.projection["openExecutionIds"]) == 0


async def test_unknown_outcome_sets_human_required(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench)
    await session.append(MigrationEventType.EXECUTION_UNKNOWN, {
        "executionId": "exec-unknown", "reason": "UNRESOLVED_EXTERNAL_OUTCOME",
    })
    assert session.projection["state"] == "HUMAN_REQUIRED"
