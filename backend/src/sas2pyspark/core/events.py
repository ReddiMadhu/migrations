"""Migration event types, artifact references, and the hash-chained event envelope."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import Field

from sas2pyspark.core.models import WorkbenchModel


class MigrationEventType(str, Enum):
    MIGRATION_CREATED = "migration/created"
    WORKSPACE_DISCOVERED = "workspace/discovered"
    DEPENDENCY_GRAPH_CREATED = "dependency/graph-created"
    SEMANTIC_IR_CREATED = "semantic-ir/created"
    MACRO_RESOLUTION_CREATED = "macro-resolution/created"
    WORK_UNIT_CREATED = "work-unit/created"
    WORK_UNIT_UPDATED = "work-unit/updated"
    WORK_UNIT_CLOSED = "work-unit/closed"
    SOURCE_REGISTERED = "source/registered"
    SNAPSHOT_CREATED = "snapshot/created"
    TRANSLATION_STARTED = "translation/started"
    TRANSLATION_COMPLETED = "translation/completed"
    ARTIFACT_CREATED = "artifact/created"
    EXECUTION_STARTED = "execution/started"
    EXECUTION_COMPLETED = "execution/completed"
    RECONCILIATION_STARTED = "reconciliation/started"
    RECONCILIATION_COMPLETED = "reconciliation/completed"
    RECONCILIATION_FAILED = "reconciliation/failed"
    DIFFERENCE_REPORT_CREATED = "difference/report-created"
    REPAIR_STARTED = "repair/started"
    REPAIR_PROPOSED = "repair/proposed"
    PATCH_APPLIED = "patch/applied"
    CERTIFICATE_ISSUED = "certificate/issued"
    MIGRATION_ESCALATED = "migration/escalated"
    SESSION_FORKED = "session/forked"
    SESSION_RECOVERED = "session/recovered"
    EXECUTION_UNKNOWN = "execution/unknown"
    AGENT_STARTED = "agent/started"
    AGENT_TURN_STARTED = "agent/turn-started"
    AGENT_TURN_ENDED = "agent/turn-ended"
    AGENT_STEP_STARTED = "agent/step-started"
    AGENT_STEP_ENDED = "agent/step-ended"
    AGENT_IDLE = "agent/idle"
    TOOL_STARTED = "tool/started"
    TOOL_COMPLETED = "tool/completed"
    TOOL_DENIED = "tool/denied"
    TOOL_BLOCKED = "tool/blocked"
    TOOL_OUTCOME_UNKNOWN = "tool/outcome-unknown"
    TOOL_NOT_STARTED = "tool/not-started"
    CACHE_HIT = "cache/hit"
    CACHE_MISS = "cache/miss"
    CACHE_EVICTED = "cache/evicted"
    COMPACTION_STARTED = "compaction/started"
    COMPACTION_COMPLETED = "compaction/completed"
    DELEGATION_STARTED = "delegation/started"
    DELEGATION_COMPLETED = "delegation/completed"
    FILE_EDIT_APPLIED = "file/edit-applied"


Verdict = Literal["PASS", "FAIL", "NOT_APPLICABLE"]

ExecutionOutcome = Literal[
    "SUCCEEDED", "FAILED", "CANCELED", "TIMED_OUT", "PARTIAL_SUCCESS", "UNKNOWN"
]

ExternalRunState = Literal[
    "SUBMITTING", "QUEUED", "RUNNING", "TERMINATING",
    "SUCCEEDED", "FAILED", "CANCELED", "TIMED_OUT", "PARTIAL_SUCCESS", "UNKNOWN",
]


class ArtifactReferenceData(WorkbenchModel):
    artifact_id: str = Field(alias="artifactId")
    digest: str
    media_type: str = Field(alias="mediaType")
    size: int
    semantic_hash: str | None = Field(default=None, alias="semanticHash")

    model_config = {
        **WorkbenchModel.model_config,
        "populate_by_name": True,
    }


class MigrationEvent(WorkbenchModel):
    """A single hash-chained event in a migration session."""

    event_id: str = Field(alias="eventId")
    migration_id: str = Field(alias="migrationId")
    scope_id: str = Field(alias="scopeId")
    sequence: int
    type: MigrationEventType
    created_at: str = Field(alias="createdAt")
    data: dict[str, Any]
    previous_hash: str = Field(alias="previousHash")
    hash: str

    model_config = {
        **WorkbenchModel.model_config,
        "populate_by_name": True,
    }


class EventHashInput(WorkbenchModel):
    migration_id: str = Field(alias="migrationId")
    scope_id: str = Field(alias="scopeId")
    sequence: int
    type: MigrationEventType
    created_at: str = Field(alias="createdAt")
    data: Any
    previous_hash: str = Field(alias="previousHash")

    model_config = {
        **WorkbenchModel.model_config,
        "populate_by_name": True,
    }


def event_data(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError("event data must be an object")
    return value
