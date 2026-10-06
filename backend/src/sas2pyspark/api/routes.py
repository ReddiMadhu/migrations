"""REST API routes — migration CRUD, session queries, scopes, reconciliation."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from langgraph.types import Command
from pydantic import BaseModel, Field

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.api.utils import error_to_http_code


class CreateMigrationRequest(BaseModel):
    migration_id: str = Field(alias="migrationId")
    scope_id: str = Field(alias="scopeId")
    title: str = "Untitled migration"
    sas_source: str = Field(alias="sasSource")
    sas_source_path: str = Field(default="main.sas", alias="sasSourcePath")
    sas_files: dict[str, str] = Field(default_factory=dict, alias="sasFiles")
    input_snapshot_id: str | None = Field(default=None, alias="inputSnapshotId")
    environment_fingerprint: str | None = Field(default=None, alias="environmentFingerprint")
    reference_input_path: str | None = Field(default=None, alias="referenceInputPath")
    reference_output_path: str | None = Field(default=None, alias="referenceOutputPath")

    model_config = {"populate_by_name": True}


class CreateMigrationResponse(BaseModel):
    migration_id: str = Field(alias="migrationId")
    session_id: str = Field(alias="sessionId")

    model_config = {"populate_by_name": True}


class RunMigrationResponse(BaseModel):
    thread_id: str = Field(alias="threadId")
    final_state: str = Field(alias="finalState")

    model_config = {"populate_by_name": True}


class CreateScopeRequest(BaseModel):
    parent_id: str = Field(alias="parentId")
    kind: str
    name: str

    model_config = {"populate_by_name": True}


class CreateTenantScopeRequest(BaseModel):
    tenant_id: str = Field(
        alias="tenantId",
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$",
    )

    model_config = {"populate_by_name": True}


def _check_auth(token: str | None, provided: str | None) -> None:
    if token is not None and provided != token:
        raise HTTPException(status_code=401, detail="invalid or missing auth token")


def create_router(runtime: Any, graph: Any, api_config: Any) -> APIRouter:
    async def require_auth(authorization: str | None = Header(default=None)) -> None:
        token = getattr(api_config, "auth_token", None)
        if token is not None and authorization != f"Bearer {token}":
            raise HTTPException(status_code=401, detail="invalid or missing bearer token")

    router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])
    _migration_artifacts: dict[str, dict[str, Any]] = {}
    _jobs: dict[str, dict[str, Any]] = {}

    def checkpoint_values(session_id: str) -> dict[str, Any]:
        config = {"configurable": {"thread_id": session_id}}
        try:
            snapshot = graph.get_state(config, subgraphs=True)
        except TypeError:
            snapshot = graph.get_state(config)

        def find_pending(current: Any) -> dict[str, Any] | None:
            values = current if isinstance(current, dict) else getattr(current, "values", {})
            if isinstance(values, dict) and (
                values.get("pending_approval") or values.get("pending_operator_decision")
            ):
                return values
            tasks = getattr(current, "tasks", ())
            for task in tasks:
                nested = getattr(task, "state", None)
                if nested is not None:
                    found = find_pending(nested)
                    if found is not None:
                        return found
            return values if isinstance(values, dict) else {}

        return find_pending(snapshot) or {}

    @router.post("/migrations", response_model=CreateMigrationResponse)
    async def create_migration(req: CreateMigrationRequest):
        try:
            scope = runtime.scopes.require_by_id(req.scope_id)
            executor = getattr(runtime, "spark_executor", None)
            if executor is not None:
                runtime.scopes.mount(scope, "executor.spark", executor)
            session = await runtime.sessions.create(
                migration_id=req.migration_id, scope=scope, title=req.title,
            )
            artifact = runtime.artifacts.create(
                scope=scope, data=req.sas_source,
                media_type="text/x-sas", created_by="api",
            )
            from sas2pyspark.services.artifacts import artifact_reference
            dependency_artifacts = []
            for path, source in req.sas_files.items():
                dependency = runtime.artifacts.create(
                    scope=scope,
                    data=source,
                    media_type="text/x-sas",
                    created_by="api-dependency",
                    label=path,
                )
                dependency_artifacts.append({
                    "path": path,
                    "artifact": artifact_reference(dependency).model_dump(by_alias=True),
                })
            _migration_artifacts[req.migration_id] = {
                "artifact": artifact_reference(artifact).model_dump(by_alias=True),
                "sasSourcePath": req.sas_source_path,
                "sasDependencyArtifacts": dependency_artifacts,
                "inputSnapshotId": req.input_snapshot_id,
                "environmentFingerprint": req.environment_fingerprint,
                "referenceInputPath": req.reference_input_path,
                "referenceOutputPath": req.reference_output_path,
            }
            return CreateMigrationResponse(migrationId=req.migration_id, sessionId=session.id)
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    def _derive_migration_input(session: Any) -> dict[str, Any] | None:
        for event in session.events:
            if event.type.value == "migration/created":
                artifact_ref = event.data.get("artifact")
                if artifact_ref:
                    return {
                        "artifact": artifact_ref,
                        "sasSourcePath": event.data.get("sasSourcePath", "main.sas"),
                        "sasDependencyArtifacts": event.data.get("sasDependencyArtifacts", []),
                        "inputSnapshotId": event.data.get("inputSnapshotId"),
                        "environmentFingerprint": event.data.get("environmentFingerprint"),
                        "referenceInputPath": event.data.get("referenceInputPath"),
                        "referenceOutputPath": event.data.get("referenceOutputPath"),
                    }
        return None

    @router.post("/migrations/{migration_id}/run")
    async def run_migration(migration_id: str, response: Response, sync: bool = Query(False)):
        try:
            session = await runtime.sessions.find_by_migration_id(migration_id)
            if session is None:
                raise WorkbenchError(WorkbenchErrorCode.SESSION_NOT_FOUND, f"migration {migration_id} not found")
            migration_input = _migration_artifacts.get(migration_id)
            if migration_input is None:
                migration_input = _derive_migration_input(session)
            if migration_input is None:
                raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "no SAS source artifact")
            initial_state = {
                "session_id": session.id,
                "migration_id": migration_id,
                "scope_id": session.scope.id,
                "state": "CREATED",
                "sas_source_artifact": migration_input["artifact"],
                "sas_source_path": migration_input["sasSourcePath"],
                "sas_dependency_artifacts": migration_input["sasDependencyArtifacts"],
                "input_snapshot_id": migration_input["inputSnapshotId"],
                "environment_fingerprint": migration_input["environmentFingerprint"],
                "reference_input_path": migration_input["referenceInputPath"],
                "reference_output_path": migration_input["referenceOutputPath"],
                "repair_history": [],
                "repair_iteration": 0,
                "open_execution_ids": [],
                "execution_outcomes": {},
                "pending_approval": False,
            }
            if sync:
                result = await graph.ainvoke(
                    initial_state,
                    config={"configurable": {"thread_id": session.id}},
                )
                final_state = result.get("state", "UNKNOWN") if isinstance(result, dict) else "UNKNOWN"
                return RunMigrationResponse(threadId=session.id, finalState=final_state)

            job_id = str(uuid.uuid4())
            _jobs[job_id] = {
                "jobId": job_id,
                "migrationId": migration_id,
                "sessionId": session.id,
                "state": "RUNNING",
                "finalState": None,
                "startedAt": runtime.clock.now(),
            }

            async def _run_background():
                try:
                    result = await graph.ainvoke(
                        initial_state,
                        config={"configurable": {"thread_id": session.id}},
                    )
                    final_state = result.get("state", "UNKNOWN") if isinstance(result, dict) else "UNKNOWN"
                    _jobs[job_id]["state"] = "COMPLETED"
                    _jobs[job_id]["finalState"] = final_state
                except Exception as e:
                    _jobs[job_id]["state"] = "FAILED"
                    _jobs[job_id]["finalState"] = str(e)

            asyncio.create_task(_run_background())
            response.status_code = 202
            response.headers["Location"] = f"/api/jobs/{job_id}"
            return {"jobId": job_id, "migrationId": migration_id, "sessionId": session.id}
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    @router.get("/jobs/{job_id}")
    async def get_job(job_id: str):
        job = _jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    @router.get("/sessions/{session_id}")
    async def get_session(session_id: str):
        try:
            session = await runtime.sessions.open(session_id)
            return {
                "sessionId": session.id,
                "migrationId": session.migration_id,
                "scopeId": session.scope.id,
                "header": session.header.to_dict(),
                "projection": session.projection,
            }
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    @router.get("/sessions/{session_id}/events")
    async def get_events(session_id: str, offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=500)):
        try:
            session = await runtime.sessions.open(session_id)
            events = session.events[offset:offset + limit]
            return {
                "sessionId": session_id,
                "offset": offset,
                "limit": limit,
                "total": len(session.events),
                "events": [e.model_dump(by_alias=True) for e in events],
            }
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    @router.post("/sessions/{session_id}/approve")
    async def approve(session_id: str, decision: Literal["approve", "reject"] = "approve"):
        try:
            await runtime.sessions.open(session_id)
            values = checkpoint_values(session_id)
            pending_approval = bool(values.get("pending_approval") and values.get("approval_request"))
            pending_decision = bool(values.get("pending_operator_decision") and values.get("operator_decision_request"))
            if not pending_approval and not pending_decision:
                raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "session has no pending operator decision")
            approved = decision == "approve"
            result = await graph.ainvoke(
                Command(resume=approved),
                config={"configurable": {"thread_id": session_id}},
            )
            return {"sessionId": session_id, "approved": approved, "state": result.get("state", "UNKNOWN")}
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    @router.get("/sessions/{session_id}/approval")
    async def get_approval(session_id: str):
        try:
            await runtime.sessions.open(session_id)
            values = checkpoint_values(session_id)
            pre_execution = bool(values.get("pending_approval"))
            post_reconciliation = bool(values.get("pending_operator_decision"))
            return {
                "sessionId": session_id,
                "pending": pre_execution or post_reconciliation,
                "kind": "execution" if pre_execution else "certificate" if post_reconciliation else None,
                "request": values.get("approval_request") if pre_execution else values.get("operator_decision_request"),
            }
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    @router.get("/sessions/{session_id}/reconciliation")
    async def get_reconciliation(session_id: str):
        try:
            session = await runtime.sessions.open(session_id)
            completed = next((event for event in reversed(session.events) if event.type.value == "reconciliation/completed"), None)
            return {
                "sessionId": session_id,
                "verdicts": completed.data if completed else None,
                "reportArtifact": completed.data.get("reportArtifact") if completed else None,
            }
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    @router.get("/scopes")
    async def list_scopes():
        scopes = runtime.scopes.list_all()
        return {"scopes": [{"id": s.id, "kind": s.kind, "parentId": s.parent.id if s.parent else None} for s in scopes]}

    @router.post("/scopes/tenants")
    async def create_tenant_scope(req: CreateTenantScopeRequest):
        try:
            tenant = runtime.scopes.create_tenant(req.tenant_id)
            return {"id": tenant.id, "kind": tenant.kind, "parentId": None}
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    @router.post("/scopes")
    async def create_scope(req: CreateScopeRequest):
        try:
            parent = runtime.scopes.require_by_id(req.parent_id)
            child = runtime.scopes.create_child(parent, req.kind, req.name)
            return {"id": child.id, "kind": child.kind, "parentId": parent.id}
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    @router.post("/sessions/{session_id}/executions/{execution_id}/repoll")
    async def repoll_execution(session_id: str, execution_id: str):
        try:
            session = await runtime.sessions.open(session_id)
            started = next(
                (e for e in session.events if e.type.value == "execution/started" and e.data.get("executionId") == execution_id),
                None,
            )
            if started is None:
                raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "execution not found", {"executionId": execution_id})
            unknown = next(
                (e for e in session.events if e.type.value == "execution/unknown" and e.data.get("executionId") == execution_id),
                None,
            )
            run_id = (unknown.data.get("runId") if unknown else None) or started.data.get("runId")
            if not run_id:
                raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "execution has no external run id", {"executionId": execution_id})
            scope = runtime.scopes.require_by_id(session.scope.id)
            provider = scope.get_provider("executor.spark")
            if provider is None:
                raise WorkbenchError(WorkbenchErrorCode.PROVIDER_UNAVAILABLE, "no Spark executor mounted", {"scope": scope.id})
            import asyncio
            status = await asyncio.to_thread(provider.status, run_id)
            return {
                "sessionId": session_id,
                "executionId": execution_id,
                "runId": run_id,
                "state": status.state,
                "providerMessage": status.provider_message,
            }
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    @router.post("/sessions/{session_id}/executions/{execution_id}/cancel")
    async def cancel_execution(session_id: str, execution_id: str):
        try:
            session = await runtime.sessions.open(session_id)
            unknown = next(
                (e for e in session.events if e.type.value == "execution/unknown" and e.data.get("executionId") == execution_id),
                None,
            )
            started = next(
                (e for e in session.events if e.type.value == "execution/started" and e.data.get("executionId") == execution_id),
                None,
            )
            run_id = (unknown.data.get("runId") if unknown else None) or (started.data.get("runId") if started else None)
            if not run_id:
                raise WorkbenchError(WorkbenchErrorCode.INVALID_INPUT, "execution has no external run id", {"executionId": execution_id})
            scope = runtime.scopes.require_by_id(session.scope.id)
            provider = scope.get_provider("executor.spark")
            if provider is None:
                raise WorkbenchError(WorkbenchErrorCode.PROVIDER_UNAVAILABLE, "no Spark executor mounted", {"scope": scope.id})
            import asyncio
            result = await asyncio.to_thread(provider.cancel, run_id)
            return {"sessionId": session_id, "executionId": execution_id, "runId": run_id, "accepted": result.get("accepted", True)}
        except WorkbenchError as e:
            raise HTTPException(status_code=error_to_http_code(e.code), detail=e.message)

    return router
