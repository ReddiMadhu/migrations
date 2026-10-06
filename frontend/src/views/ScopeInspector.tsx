import { useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiFetch, type Scope } from "../api/client";

export default function ScopeInspector() {
  const [tenantId, setTenantId] = useState("");
  const [parentId, setParentId] = useState("");
  const [childKind, setChildKind] = useState("migration");
  const [childName, setChildName] = useState("");
  const queryClient = useQueryClient();
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["scopes"],
    queryFn: () => apiFetch<{ scopes: Scope[] }>("/scopes"),
  });
  const createTenant = useMutation({
    mutationFn: (id: string) => apiFetch<Scope>("/scopes/tenants", {
      method: "POST",
      body: JSON.stringify({ tenantId: id }),
    }),
    onSuccess: async () => {
      setTenantId("");
      await queryClient.invalidateQueries({ queryKey: ["scopes"] });
    },
  });
  const createChild = useMutation({
    mutationFn: () => apiFetch<Scope>("/scopes", {
      method: "POST",
      body: JSON.stringify({ parentId: parentId || scopes[0]?.id, kind: childKind, name: childName.trim() }),
    }),
    onSuccess: async () => {
      setChildName("");
      await queryClient.invalidateQueries({ queryKey: ["scopes"] });
    },
  });
  const scopes = data?.scopes ?? [];
  const tenantScopes = scopes.filter((scope) => scope.kind === "tenant");

  const handleCreateTenant = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const normalizedId = tenantId.trim();
    if (normalizedId) createTenant.mutate(normalizedId);
  };

  const handleCreateChild = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (scopes.length > 0 && childName.trim()) createChild.mutate();
  };

  return (
    <>
      <div className="page-heading developer-page-heading">
        <div><p className="eyebrow">Workspace / Scopes</p><h1>Scopes</h1></div>
        <span className="count-pill">{scopes.length} {scopes.length === 1 ? "scope" : "scopes"}</span>
      </div>

      <div className="scope-workspace">
        <section className="surface scope-panel">
          <div className="surface-header">
            <div><h2>Scope hierarchy</h2><p>Workspace boundaries used by migrations and artifacts.</p></div>
          </div>
          {isLoading ? <div className="loading-state">Loading scopes…</div> : null}
          {isError ? <div className="surface-body"><div className="alert" role="alert">Could not load scopes: {String(error)}</div></div> : null}
          {!isLoading && !isError && scopes.length === 0 ? (
            <div className="scope-empty-state">
              <span className="scope-empty-icon" aria-hidden="true">⌘</span>
              <strong>No scopes yet</strong>
              <p>Create a tenant scope to enable migration runs.</p>
            </div>
          ) : null}
          {!isLoading && !isError && scopes.length > 0 ? (
            <div className="surface-body">
              <div className="scope-list">
                {scopes.map((scope) => (
                  <div className="scope-row" key={scope.id}>
                    <span className="scope-symbol" aria-hidden="true">{scope.kind === "tenant" ? "⌂" : "⌘"}</span>
                    <div className="scope-main">
                      <span className="scope-kind">{scope.kind}</span>
                      <span className="scope-id mono">{scope.id}</span>
                    </div>
                    <span className="scope-parent">{scope.parentId ? `parent · ${scope.parentId}` : "root"}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </section>

        <aside className="surface scope-create-panel">
          <div className="surface-header">
            <div><h2>Create tenant scope</h2><p>Tenants are root scopes for migration work.</p></div>
          </div>
          <form className="surface-body scope-create-form" onSubmit={handleCreateTenant}>
            <div className="field">
              <label htmlFor="tenant-id">Tenant ID</label>
              <input
                id="tenant-id"
                className="text-input mono"
                value={tenantId}
                onChange={(event) => setTenantId(event.target.value)}
                placeholder="engineering"
                pattern="[A-Za-z0-9][A-Za-z0-9._\-]{0,127}"
                maxLength={128}
                autoComplete="off"
                required
              />
              <span className="field-hint">Start with a letter or number; then use letters, numbers, dots, underscores, or hyphens.</span>
            </div>
            <button className="button button-primary" type="submit" disabled={createTenant.isPending || !tenantId.trim()}>
              {createTenant.isPending ? "Creating scope…" : "Create tenant scope"}
            </button>
            {createTenant.isError && <div className="alert" role="alert">Could not create scope: {String(createTenant.error)}</div>}
            {createTenant.isSuccess && <div className="alert alert-success" role="status">Tenant scope created: {createTenant.data.id}</div>}
          </form>
          <form className="surface-body scope-create-form child-scope-form" onSubmit={handleCreateChild}>
            <div>
              <h3>Create child scope</h3>
              <p className="scope-form-description">Add a migration or execution boundary under an existing scope.</p>
            </div>
            <div className="field">
              <label htmlFor="scope-parent">Parent scope</label>
              <select
                id="scope-parent"
                className="text-input"
                value={parentId || scopes[0]?.id || ""}
                onChange={(event) => setParentId(event.target.value)}
                disabled={scopes.length === 0}
                required
              >
                {scopes.map((scope) => <option key={scope.id} value={scope.id}>{scope.kind} · {scope.id}</option>)}
              </select>
            </div>
            <div className="field">
              <label htmlFor="scope-kind">Child kind</label>
              <select id="scope-kind" className="text-input" value={childKind} onChange={(event) => setChildKind(event.target.value)}>
                <option value="migration">Migration</option>
                <option value="attempt">Attempt</option>
                <option value="candidate">Candidate</option>
                <option value="execution">Execution</option>
              </select>
            </div>
            <div className="field">
              <label htmlFor="scope-name">Scope name</label>
              <input
                id="scope-name"
                className="text-input mono"
                value={childName}
                onChange={(event) => setChildName(event.target.value)}
                pattern="[A-Za-z0-9][A-Za-z0-9._\-]{0,127}"
                maxLength={128}
                placeholder="migration-a"
                autoComplete="off"
                required
              />
              <span className="field-hint">Use a safe path segment: letters, numbers, dots, underscores, or hyphens.</span>
            </div>
            <button className="button button-primary" type="submit" disabled={scopes.length === 0 || createChild.isPending || !childName.trim()}>
              {createChild.isPending ? "Creating scope…" : "Create child scope"}
            </button>
            {createChild.isError && <div className="alert" role="alert">Could not create child scope: {String(createChild.error)}</div>}
            {createChild.isSuccess && <div className="alert alert-success" role="status">Child scope created: {createChild.data.id}</div>}
            <div className="scope-create-note"><strong>Root boundary</strong><span>Tenant scopes own workspace paths, artifacts, and child migration scopes.</span></div>
          </form>
        </aside>
      </div>
      {tenantScopes.length > 0 && (
        <p className="scope-footer-note">Migration runs use the first available tenant scope. {tenantScopes.length} tenant {tenantScopes.length === 1 ? "scope is" : "scopes are"} available.</p>
      )}
    </>
  );
}
