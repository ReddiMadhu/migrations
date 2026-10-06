"""Attestation and evidence chain tests — hash chain integrity, tamper detection, certificates."""

from __future__ import annotations

import pytest

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.events import MigrationEventType
from sas2pyspark.core.hash import GENESIS_HASH, sha256_text
from sas2pyspark.core.json import canonical_json, snapshot_json
from sas2pyspark.services.clock import FixedClock
from sas2pyspark.workbench import WorkbenchConfig, create_workbench

from .conftest import make_scopes, make_session


async def test_hash_chain_is_sequential_and_linked(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench, scope=candidate, session_id="chain-test")
    events = session.events
    assert len(events) >= 1
    assert events[0].previous_hash == GENESIS_HASH
    for i in range(1, len(events)):
        assert events[i].previous_hash == events[i - 1].hash


async def test_tamper_detection_breaks_chain(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench, scope=candidate, session_id="tamper-test")
    await session.append(MigrationEventType.WORKSPACE_DISCOVERED, {"discoveryId": "d1", "scopeId": candidate.id, "fileCount": 1, "rootPaths": ["main.sas"]})
    await session.flush()
    events = list(session.events)
    assert len(events) >= 2
    tampered = events[1]
    tampered_data = dict(tampered.data)
    tampered_data["tampered"] = True
    tampered_events = [events[0], type(tampered)(
        event_id=tampered.event_id, migration_id=tampered.migration_id, scope_id=tampered.scope_id,
        sequence=tampered.sequence, type=tampered.type, created_at=tampered.created_at,
        data=snapshot_json(tampered_data), previous_hash=tampered.previous_hash, hash=tampered.hash,
    )]
    with pytest.raises(WorkbenchError) as exc_info:
        workbench.evidence.verify(tampered_events)
    assert exc_info.value.code in (WorkbenchErrorCode.EVIDENCE_TAMPERED, WorkbenchErrorCode.SESSION_CORRUPTION)


async def test_event_hashes_are_deterministic(test_dir):
    from sas2pyspark.services.clock import FixedClock as FC
    wb1 = await create_workbench(WorkbenchConfig(root_dir=str(test_dir / "wb1"), clock=FC()))
    _, _, _, c1 = make_scopes(wb1)
    s1 = await make_session(wb1, scope=c1, session_id="det-test")
    events1 = [e.hash for e in s1.events]
    await wb1.dispose()

    wb2 = await create_workbench(WorkbenchConfig(root_dir=str(test_dir / "wb2"), clock=FC()))
    _, _, _, c2 = make_scopes(wb2)
    s2 = await make_session(wb2, scope=c2, session_id="det-test")
    events2 = [e.hash for e in s2.events]
    await wb2.dispose()

    assert events1 == events2


async def test_artifact_digest_is_content_addressed(workbench):
    _, _, _, candidate = make_scopes(workbench)
    data = b"test content for addressing"
    artifact1 = workbench.artifacts.create(
        scope=candidate, data=data, media_type="text/plain", created_by="test",
    )
    artifact2 = workbench.artifacts.create(
        scope=candidate, data=data, media_type="text/plain", created_by="test-2",
    )
    assert artifact1.source_hash == artifact2.source_hash
    assert artifact1.artifact_id == artifact2.artifact_id


async def test_artifact_overwrite_rejected(workbench):
    _, _, _, candidate = make_scopes(workbench)
    workbench.artifacts.create(
        scope=candidate, data=b"same content", media_type="text/plain", created_by="test-1",
    )
    with pytest.raises(WorkbenchError) as exc_info:
        workbench.artifacts.create(
            scope=candidate, data=b"same content", media_type="text/csv", created_by="test-2",
        )
    assert exc_info.value.code == WorkbenchErrorCode.DUPLICATE_ARTIFACT


async def test_canonical_json_is_sorted_and_compact():
    obj = {"b": 2, "a": 1, "c": {"d": 4, "c": 3}}
    result = canonical_json(obj)
    assert result == '{"a":1,"b":2,"c":{"c":3,"d":4}}'


async def test_snapshot_json_is_stable():
    obj = {"z": 1, "a": 2}
    snap = snapshot_json(obj)
    assert canonical_json(snap) == canonical_json(obj)


async def test_session_projection_tracks_state(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench, scope=candidate, session_id="projection-test")
    assert session.projection["state"] == "CREATED"
    assert session.projection["lastSequence"] == 1
    await session.append(MigrationEventType.WORKSPACE_DISCOVERED, {"discoveryId": "d1", "scopeId": candidate.id, "fileCount": 1, "rootPaths": ["main.sas"]})
    assert session.projection["lastSequence"] == 2


async def test_genesis_hash_is_all_zeros():
    assert GENESIS_HASH == "sha256:" + "0" * 64


async def test_evidence_roots_are_computed(workbench):
    _, _, _, candidate = make_scopes(workbench)
    session = await make_session(workbench, scope=candidate, session_id="roots-test")
    root = workbench.evidence.root(session.events)
    assert root is not None
    assert root == session.events[-1].hash
