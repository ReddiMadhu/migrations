"""EvidenceService — hash-chained event log with tamper detection."""

from __future__ import annotations

from typing import Any, Sequence

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.hash import GENESIS_HASH, sha256_json
from sas2pyspark.core.json import snapshot_json
from sas2pyspark.core.events import MigrationEvent, MigrationEventType


class EventHashInput:
    __slots__ = ("migration_id", "scope_id", "sequence", "type", "created_at", "data", "previous_hash")

    def __init__(
        self,
        *,
        migration_id: str,
        scope_id: str,
        sequence: int,
        type: MigrationEventType,
        created_at: str,
        data: Any,
        previous_hash: str,
    ) -> None:
        self.migration_id = migration_id
        self.scope_id = scope_id
        self.sequence = sequence
        self.type = type
        self.created_at = created_at
        self.data = data
        self.previous_hash = previous_hash


class EvidenceService:
    def hash(self, input: EventHashInput) -> str:
        return sha256_json(snapshot_json({
            "migrationId": input.migration_id,
            "scopeId": input.scope_id,
            "sequence": input.sequence,
            "eventType": input.type.value if isinstance(input.type, MigrationEventType) else input.type,
            "payload": snapshot_json(input.data),
            "previousHash": input.previous_hash,
            "createdAt": input.created_at,
        }))

    def verify(self, events: Sequence[MigrationEvent]) -> None:
        previous = GENESIS_HASH
        expected_sequence = 1
        for event in events:
            if event.sequence != expected_sequence:
                raise WorkbenchError(
                    WorkbenchErrorCode.SESSION_CORRUPTION,
                    "event sequence is not contiguous",
                    {"expected": expected_sequence, "actual": event.sequence},
                )
            if event.previous_hash != previous:
                raise WorkbenchError(
                    WorkbenchErrorCode.EVIDENCE_TAMPERED,
                    "event previous hash does not match chain",
                    {"sequence": event.sequence, "expected": previous, "actual": event.previous_hash},
                )
            actual = self.hash(EventHashInput(
                migration_id=event.migration_id,
                scope_id=event.scope_id,
                sequence=event.sequence,
                type=event.type,
                created_at=event.created_at,
                data=event.data,
                previous_hash=event.previous_hash,
            ))
            if actual != event.hash:
                raise WorkbenchError(
                    WorkbenchErrorCode.EVIDENCE_TAMPERED,
                    "event hash does not match canonical content",
                    {"sequence": event.sequence, "expected": actual, "actual": event.hash},
                )
            previous = event.hash
            expected_sequence += 1

    def root(self, events: Sequence[MigrationEvent]) -> str:
        self.verify(events)
        return events[-1].hash if events else GENESIS_HASH
