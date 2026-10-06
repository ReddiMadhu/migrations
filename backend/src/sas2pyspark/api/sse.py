"""SSE event streaming — live migration event timeline via Server-Sent Events.

Uses FastAPI StreamingResponse with text/event-stream content type.
Supports EventSource reconnection via Last-Event-ID header.
Emits :keep-alive heartbeat every 15s to prevent proxy timeouts.
Detects client disconnect and stops the generator.
Reads from the durable event log so streaming survives process restart.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from sas2pyspark.core.errors import WorkbenchError
from sas2pyspark.api.utils import error_to_http_code

_HEARTBEAT_INTERVAL = 15.0


def _sse_format(event_type: str, event_id: str, data: str) -> str:
    return f"event: {event_type}\nid: {event_id}\ndata: {data}\n\n"


def create_sse_router(runtime: Any, graph: Any, api_config: Any) -> APIRouter:
    async def require_auth(authorization: str | None = Header(None)) -> None:
        token = getattr(api_config, "auth_token", None)
        if token is not None and authorization != f"Bearer {token}":
            raise HTTPException(status_code=401, detail="invalid or missing bearer token")

    router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])

    @router.get("/sessions/{session_id}/stream")
    async def stream_events(
        request: Request,
        session_id: str,
        last_event_id: str | None = Header(None, alias="Last-Event-ID"),
    ):
        try:
            session = await runtime.sessions.open(session_id)
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

        async def event_generator():
            persistence = getattr(runtime.sessions, "_persistence", None)
            last_interrupt: str | None = None
            sent_count = 0
            initial_events = session.events
            start_idx = 0
            if last_event_id is not None:
                for i, e in enumerate(initial_events):
                    if e.event_id == last_event_id:
                        start_idx = i + 1
                        break
            for event in initial_events[start_idx:]:
                yield _sse_format("migration-event", event.event_id, json.dumps(event.model_dump(by_alias=True)))
                sent_count += 1
            last_heartbeat = asyncio.get_event_loop().time()
            while True:
                if await request.is_disconnected():
                    break
                now = asyncio.get_event_loop().time()
                if now - last_heartbeat >= _HEARTBEAT_INTERVAL:
                    yield ": keep-alive\n\n"
                    last_heartbeat = now
                await asyncio.sleep(0.5)
                if persistence is not None:
                    try:
                        persisted = persistence.read(session_id)
                        current_events = persisted.events
                    except Exception:
                        current_events = session.events
                else:
                    current_events = session.events
                if len(current_events) > sent_count + start_idx:
                    for event in current_events[sent_count + start_idx:]:
                        yield _sse_format("migration-event", event.event_id, json.dumps(event.model_dump(by_alias=True)))
                        sent_count += 1
                try:
                    checkpoint = graph.get_state({"configurable": {"thread_id": session_id}}, subgraphs=True)
                except TypeError:
                    checkpoint = graph.get_state({"configurable": {"thread_id": session_id}})

                def pending_values(current: Any) -> dict[str, Any]:
                    values = getattr(current, "values", {}) or {}
                    if values.get("pending_approval") or values.get("pending_operator_decision"):
                        return values
                    for task in getattr(current, "tasks", ()):
                        nested = getattr(task, "state", None)
                        if nested is not None:
                            nested_values = pending_values(nested)
                            if nested_values.get("pending_approval") or nested_values.get("pending_operator_decision"):
                                return nested_values
                    return values

                values = pending_values(checkpoint)
                interrupt_payload = None
                if values.get("pending_approval"):
                    interrupt_payload = values.get("approval_request")
                elif values.get("pending_operator_decision"):
                    interrupt_payload = values.get("operator_decision_request")
                if interrupt_payload is not None:
                    payload = json.dumps(interrupt_payload, sort_keys=True)
                    if payload != last_interrupt:
                        yield _sse_format("approval", f"{session_id}:interrupt", payload)
                        last_interrupt = payload

        return StreamingResponse(event_generator(), media_type="text/event-stream")

    return router
