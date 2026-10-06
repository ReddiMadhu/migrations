"""API test suite — app creation, route registration, OpenAPI schema.

Full HTTP tests require a non-sandbox environment with a working event loop.
Sandbox tests verify app construction and route registration.
"""

from __future__ import annotations

import asyncio

from sas2pyspark.api.app import WorkbenchApiConfig, create_app
from sas2pyspark.workbench import WorkbenchConfig, create_workbench
from sas2pyspark.services.clock import FixedClock


def _run(coro):
    try:
        while True:
            coro.send(None)
    except StopIteration as e:
        return e.value


def test_api_imports():
    from sas2pyspark.api.app import create_app, WorkbenchApiConfig
    from sas2pyspark.api.routes import create_router
    from sas2pyspark.api.sse import create_sse_router
    from sas2pyspark.api.utils import error_to_http_code
    from sas2pyspark.api.main import app
    assert create_app is not None
    assert create_router is not None
    assert create_sse_router is not None
    assert error_to_http_code is not None
    assert callable(app)


def test_app_creation(test_dir):
    runtime = _run(create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=FixedClock())))
    app = _run(create_app(WorkbenchApiConfig(root_dir=str(test_dir)), runtime=runtime))
    assert app is not None
    assert app.title == "SAS→PySpark Migration Workbench"


def test_app_has_routes(test_dir):
    runtime = _run(create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=FixedClock())))
    app = _run(create_app(WorkbenchApiConfig(root_dir=str(test_dir)), runtime=runtime))
    routes = []
    for r in app.routes:
        if hasattr(r, "path"):
            routes.append(r.path)
        elif hasattr(r, "original_router"):
            routes.extend(sub.path for sub in r.original_router.routes if hasattr(sub, "path"))
    assert any("/api/migrations" in r for r in routes)
    assert any("/api/sessions" in r for r in routes)
    assert any("/api/scopes" in r for r in routes)


def test_openapi_schema_generation(test_dir):
    runtime = _run(create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=FixedClock())))
    app = _run(create_app(WorkbenchApiConfig(root_dir=str(test_dir)), runtime=runtime))
    schema = app.openapi()
    assert schema["info"]["title"] == "SAS→PySpark Migration Workbench"
    assert "/api/migrations" in schema["paths"]
    assert "/api/sessions/{session_id}" in schema["paths"]
    assert "/api/scopes" in schema["paths"]
    assert "/api/scopes/tenants" in schema["paths"]
    tenant_request = schema["components"]["schemas"]["CreateTenantScopeRequest"]
    assert tenant_request["required"] == ["tenantId"]


def test_create_tenant_scope_api(test_dir):
    import httpx

    runtime = _run(create_workbench(WorkbenchConfig(root_dir=str(test_dir), clock=FixedClock())))
    app = _run(create_app(WorkbenchApiConfig(root_dir=str(test_dir)), runtime=runtime))

    async def exercise_scope_endpoints():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://workbench.test") as client:
            response = await client.post("/api/scopes/tenants", json={"tenantId": "analytics"})
            assert response.status_code == 200
            assert response.json() == {"id": "tenant/analytics", "kind": "tenant", "parentId": None}
            scopes_response = await client.get("/api/scopes")
            assert scopes_response.status_code == 200
            assert scopes_response.json()["scopes"] == [response.json()]
            invalid_response = await client.post("/api/scopes/tenants", json={"tenantId": "../outside"})
            assert invalid_response.status_code == 422

    asyncio.run(exercise_scope_endpoints())
    _run(runtime.dispose())


def test_error_to_http_code_mapping():
    from sas2pyspark.api.utils import error_to_http_code
    from sas2pyspark.core.errors import WorkbenchErrorCode
    assert error_to_http_code(WorkbenchErrorCode.SESSION_NOT_FOUND) == 404
    assert error_to_http_code(WorkbenchErrorCode.SESSION_ALREADY_EXISTS) == 409
    assert error_to_http_code(WorkbenchErrorCode.INVALID_INPUT) == 422
    assert error_to_http_code(WorkbenchErrorCode.POLICY_DENIED) == 403
