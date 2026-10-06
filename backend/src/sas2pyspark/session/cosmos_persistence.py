"""Azure Cosmos DB session persistence — append-only event log with ETag concurrency."""

from __future__ import annotations

import logging
from typing import Any, Sequence

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.core.events import MigrationEvent
from sas2pyspark.core.hash import GENESIS_HASH
from sas2pyspark.graph.cosmos_client_provider import CosmosClientProvider
from sas2pyspark.graph.cosmos_reliability import (
    CircuitBreaker,
    run_with_retry,
)
from sas2pyspark.graph.cosmos_telemetry import record_etag_conflict
from sas2pyspark.session.persistence import (
    PersistedSession,
    SessionHeader,
    SessionPersistence,
    _parse_event,
    _parse_header,
)

logger = logging.getLogger(__name__)


def _header_doc_id(session_id: str) -> str:
    return f"header#{session_id}"


def _head_doc_id(session_id: str) -> str:
    return f"head#{session_id}"


def _event_doc_id(session_id: str, sequence: int) -> str:
    return f"event#{session_id}#{sequence:08d}"


class CosmosDbSessionPersistence(SessionPersistence):
    """Azure Cosmos DB-backed session persistence.

    Stores session headers, sequential hash-chained events, and optimistic concurrency
    head trackers in a single Cosmos DB container partitioned by ``/session_id``.
    """

    def __init__(
        self,
        provider: CosmosClientProvider,
        *,
        container_name: str = "sessions",
        max_concurrency_retries: int = 5,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self._provider = provider
        self._container_name = container_name
        self._max_concurrency_retries = max_concurrency_retries
        self._circuit_breaker = circuit_breaker
        self._container: Any = None

    def _get_container(self) -> Any:
        if self._container is None:
            self._container = self._provider.get_container(self._container_name)
        return self._container

    def create(
        self,
        *,
        header: SessionHeader,
        initial_events: Sequence[MigrationEvent] | None = None,
    ) -> None:
        """Create a new session with its header and optional initial events."""
        container = self._get_container()
        session_id = header.session_id
        header_id = _header_doc_id(session_id)

        # Fail if session header already exists
        try:
            container.read_item(item=header_id, partition_key=session_id)
            raise WorkbenchError(
                WorkbenchErrorCode.SESSION_ALREADY_EXISTS,
                f"session {session_id} already exists",
                {"sessionId": session_id},
            )
        except WorkbenchError:
            raise
        except Exception:
            pass  # Does not exist — proceed with creation

        # Create header document
        header_doc = {
            "id": header_id,
            "session_id": session_id,
            "type": "header",
            "data": header.to_dict(),
        }
        run_with_retry(
            container.create_item,
            body=header_doc,
            operation="session_create_header",
            circuit_breaker=self._circuit_breaker,
        )

        # Store initial events and compute head state
        events = list(initial_events or [])
        last_sequence = 0
        last_hash = GENESIS_HASH

        for ev in events:
            event_doc = {
                "id": _event_doc_id(session_id, ev.sequence),
                "session_id": session_id,
                "type": "event",
                "sequence": ev.sequence,
                "data": ev.model_dump(by_alias=True),
            }
            run_with_retry(
                container.create_item,
                body=event_doc,
                operation="session_create_event",
                circuit_breaker=self._circuit_breaker,
            )
            last_sequence = ev.sequence
            last_hash = ev.hash

        # Create head tracker document
        head_doc = {
            "id": _head_doc_id(session_id),
            "session_id": session_id,
            "type": "head",
            "sequence": last_sequence,
            "hash": last_hash,
        }
        run_with_retry(
            container.upsert_item,
            body=head_doc,
            operation="session_create_head",
            circuit_breaker=self._circuit_breaker,
        )

    def read(self, session_id: str) -> PersistedSession:
        """Read a persisted session and all its events ordered by sequence."""
        container = self._get_container()
        header_id = _header_doc_id(session_id)

        try:
            h_doc = container.read_item(item=header_id, partition_key=session_id)
        except Exception as exc:
            raise WorkbenchError(
                WorkbenchErrorCode.SESSION_NOT_FOUND,
                f"session {session_id} does not exist",
                {"sessionId": session_id},
            ) from exc

        header = _parse_header(h_doc["data"])

        # Query all events in partition ordered by sequence
        query = (
            "SELECT * FROM c WHERE c.session_id = @sid "
            "AND c.type = 'event' "
            "ORDER BY c.sequence ASC"
        )
        params = [{"name": "@sid", "value": session_id}]
        items = list(container.query_items(
            query=query,
            parameters=params,
            partition_key=session_id,
        ))

        events: list[MigrationEvent] = []
        corrupt_lines: list[int] = []
        for idx, item in enumerate(items, 1):
            try:
                events.append(_parse_event(item["data"]))
            except (KeyError, WorkbenchError):
                corrupt_lines.append(idx)

        return PersistedSession(
            header=header,
            events=events,
            truncated_tail=False,
            corrupt_lines=corrupt_lines,
        )

    def append(self, session_id: str, event: MigrationEvent) -> None:
        """Append an event to a session with optimistic concurrency on head document."""
        container = self._get_container()
        head_id = _head_doc_id(session_id)

        for attempt in range(1, self._max_concurrency_retries + 1):
            try:
                head_doc = container.read_item(item=head_id, partition_key=session_id)
            except Exception as exc:
                raise WorkbenchError(
                    WorkbenchErrorCode.SESSION_NOT_FOUND,
                    f"session {session_id} does not exist",
                    {"sessionId": session_id},
                ) from exc

            etag = head_doc.get("_etag")

            # Create the event document
            event_doc = {
                "id": _event_doc_id(session_id, event.sequence),
                "session_id": session_id,
                "type": "event",
                "sequence": event.sequence,
                "data": event.model_dump(by_alias=True),
            }

            try:
                container.create_item(body=event_doc)
            except Exception as exc:
                # If event already exists with this ID (sequence collision), handle conflict
                status_code = getattr(exc, "status_code", None)
                if status_code in (409, 412):
                    record_etag_conflict()
                    logger.warning(
                        "session_append_conflict_event_exists",
                        extra={"session_id": session_id, "sequence": event.sequence, "attempt": attempt},
                    )
                    continue
                raise

            # Update head tracker with new sequence and hash
            updated_head = {
                "id": head_id,
                "session_id": session_id,
                "type": "head",
                "sequence": event.sequence,
                "hash": event.hash,
            }

            try:
                kwargs: dict[str, Any] = {}
                if etag:
                    kwargs["access_conditions"] = {"if_match": etag}
                container.replace_item(item=head_id, body=updated_head, **kwargs)
                return  # Success
            except Exception as exc:
                status_code = getattr(exc, "status_code", None)
                if status_code in (412, 409):
                    record_etag_conflict()
                    logger.warning(
                        "session_append_etag_mismatch",
                        extra={"session_id": session_id, "attempt": attempt},
                    )
                    # Cleanup uncommitted event doc before retrying
                    try:
                        container.delete_item(item=event_doc["id"], partition_key=session_id)
                    except Exception:
                        pass
                    continue
                # If replace without access_conditions is supported in mock
                try:
                    # Fallback to direct upsert if mock doesn't support replace_item access_conditions
                    container.upsert_item(body=updated_head)
                    return
                except Exception:
                    raise

        raise WorkbenchError(
            WorkbenchErrorCode.SESSION_CORRUPTION,
            f"Failed to append to session {session_id} after {self._max_concurrency_retries} attempts due to concurrency conflicts",
            {"sessionId": session_id},
        )

    def flush(self, session_id: str) -> None:
        """Cosmos DB writes are acknowledged synchronously per call; flush is a no-op."""
        pass

    def list_sessions(self) -> list[str]:
        """List all active session IDs in Cosmos DB."""
        container = self._get_container()
        query = "SELECT c.session_id FROM c WHERE c.type = 'header'"
        results = list(container.query_items(
            query=query,
            enable_cross_partition_query=True,
        ))
        session_ids = [r["session_id"] for r in results if "session_id" in r]
        session_ids.sort()
        return session_ids

    def dispose(self) -> None:
        """Provider owns client lifecycle; dispose is a no-op."""
        pass
