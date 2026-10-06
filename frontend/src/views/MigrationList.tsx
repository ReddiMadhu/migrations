import { useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useCreateMigration, useRunMigration } from "../hooks/useMigrations";
import { apiFetch, type Scope } from "../api/client";

interface Props {
  onSelectSession: (id: string, jobId: string | null) => void;
  onOpenScopes: () => void;
}

export default function MigrationList({ onSelectSession, onOpenScopes }: Props) {
  const [sasSource, setSasSource] = useState("data temp;\n  set in;\nrun;");
  const [sasSourcePath, setSasSourcePath] = useState("main.sas");
  const [sasFiles, setSasFiles] = useState<Array<{ path: string; source: string }>>([]);
  const [migrationId, setMigrationId] = useState("migration-1");
  const [title, setTitle] = useState("New SAS migration");
  const [inputSnapshotId, setInputSnapshotId] = useState("");
  const [environmentFingerprint, setEnvironmentFingerprint] = useState("");
  const [referenceInputPath, setReferenceInputPath] = useState("");
  const [referenceOutputPath, setReferenceOutputPath] = useState("");
  const [submissionError, setSubmissionError] = useState<string | null>(null);
  const [fileError, setFileError] = useState<string | null>(null);
  const entryFilePath = sasSourcePath.trim() || "main.sas";
  const editorRef = useRef<HTMLTextAreaElement>(null);
  const gutterRef = useRef<HTMLDivElement>(null);
  const scopesQuery = useQuery({
    queryKey: ["scopes"],
    queryFn: () => apiFetch<{ scopes: Scope[] }>("/scopes"),
  });
  const createMut = useCreateMigration();
  const runMut = useRunMigration();
  const isBusy = createMut.isPending || runMut.isPending;
  const scope = scopesQuery.data?.scopes[0];
  const lineNumbers = Array.from({ length: Math.max(1, sasSource.split("\n").length) }, (_, index) => index + 1);

  const syncEditorGutter = () => {
    if (editorRef.current && gutterRef.current) {
      gutterRef.current.style.transform = `translateY(-${editorRef.current.scrollTop}px)`;
    }
  };

  const handleCreate = async () => {
    if (!scope || !migrationId.trim() || !sasSource.trim()) return;
    setSubmissionError(null);
    let createdSessionId: string | null = null;
    try {
      const result = await createMut.mutateAsync({
        migrationId: migrationId.trim(),
        scopeId: scope.id,
        title: title.trim() || "Untitled migration",
        sasSource,
        sasSourcePath: entryFilePath,
        sasFiles: Object.fromEntries(sasFiles.map(({ path, source }) => [path, source])),
        inputSnapshotId: inputSnapshotId.trim() || null,
        environmentFingerprint: environmentFingerprint.trim() || null,
        referenceInputPath: referenceInputPath.trim() || null,
        referenceOutputPath: referenceOutputPath.trim() || null,
      });
      createdSessionId = result.sessionId;
      const job = await runMut.mutateAsync(result.migrationId);
      onSelectSession(job.sessionId, job.jobId);
    } catch (error) {
      setSubmissionError(createdSessionId
        ? `Migration was created, but could not start: ${String(error)}`
        : `Could not create migration: ${String(error)}`);
    }
  };

  const handleDependencyFiles = async (files: FileList | null) => {
    const selectedFiles = Array.from(files ?? []);
    const sasFilesOnly = selectedFiles.filter((file) => file.name.toLowerCase().endsWith(".sas"));
    if (sasFilesOnly.length !== selectedFiles.length) {
      setFileError("Only .sas dependency files can be added.");
    } else {
      setFileError(null);
    }
    try {
      const loadedFiles = await Promise.all(sasFilesOnly.map(async (file) => ({
        path: file.name,
        source: await file.text(),
      })));
      setSasFiles((current) => [
        ...current.filter((existing) => !loadedFiles.some((file) => file.path === existing.path)),
        ...loadedFiles,
      ]);
    } catch (error) {
      setFileError(`Could not read selected SAS files: ${String(error)}`);
    }
  };

  return (
    <>
      <div className="page-heading developer-page-heading">
        <div><p className="eyebrow">New run</p><h1>New migration</h1></div>
      </div>

      <div className="developer-layout">
        <section className="surface developer-editor">
          <div className="surface-header">
            <div><h2>Source</h2><p>{entryFilePath}</p></div>
            <span className="editor-language">SAS</span>
          </div>
          <div className="surface-body">
            <div className="editor-toolbar">
              <div className="editor-tab"><span className="editor-file">{entryFilePath}</span><span className="editor-tab-dirty" /></div>
              <div className="editor-toolbar-meta"><span>Spaces: 2</span><span>UTF-8</span><span>SAS</span></div>
            </div>
            <div className="code-editor">
              <div className="code-gutter" aria-hidden="true"><div ref={gutterRef}>{lineNumbers.map((line) => <span key={line}>{line}</span>)}</div></div>
              <textarea
                ref={editorRef}
                aria-label="SAS source code"
                className="text-area"
                value={sasSource}
                onChange={(event) => setSasSource(event.target.value)}
                onScroll={syncEditorGutter}
                spellCheck={false}
                autoCapitalize="off"
                autoComplete="off"
                autoCorrect="off"
                wrap="off"
              />
            </div>
            <div className="editor-statusbar"><span><i className="status-dot" /> Ready</span><span>{lineNumbers.length} lines</span></div>
            <div className="source-files-config">
              <div className="field">
                <label htmlFor="sas-source-path">Entry file path</label>
                <input id="sas-source-path" className="text-input mono" value={sasSourcePath} onChange={(event) => setSasSourcePath(event.target.value)} />
              </div>
              <div className="field">
                <label htmlFor="sas-dependency-files">Included SAS files</label>
                <input id="sas-dependency-files" className="text-input" type="file" accept=".sas,text/x-sas" multiple onChange={(event) => {
                  void handleDependencyFiles(event.currentTarget.files);
                  event.currentTarget.value = "";
                }} />
              </div>
              {sasFiles.length > 0 && (
                <ul className="sas-dependency-list">
                  {sasFiles.map((file) => (
                    <li key={file.path}>
                      <span className="mono">{file.path}</span>
                      <button className="file-remove-button" type="button" aria-label={`Remove ${file.path}`} onClick={() => setSasFiles((current) => current.filter((item) => item.path !== file.path))}>Remove</button>
                    </li>
                  ))}
                </ul>
              )}
              {fileError && <div className="alert" role="alert">{fileError}</div>}
            </div>
            <div className="form-footer">
              <span className="form-footer-note">Source is stored as an immutable artifact.</span>
              <button className="button button-primary" onClick={() => void handleCreate()} disabled={isBusy || !scope || !migrationId.trim() || !sasSource.trim()}>
                {createMut.isPending ? "Creating…" : runMut.isPending ? "Starting…" : <>Run migration <span aria-hidden="true">→</span></>}
              </button>
            </div>
            {scopesQuery.isError && <div className="alert" role="alert">Could not load scopes: {String(scopesQuery.error)}</div>}
            {!scopesQuery.isLoading && !scopesQuery.isError && !scope && (
              <div className="alert scope-required-alert" role="alert">
                <span>No execution scope is available. Create one before starting a migration.</span>
                <button className="button button-secondary" type="button" onClick={onOpenScopes}>Manage scopes</button>
              </div>
            )}
            {submissionError && <div className="alert" role="alert">{submissionError}</div>}
          </div>
        </section>

        <aside className="surface developer-config">
          <div className="surface-header">
            <div><h2>Run configuration</h2><p>Required identifiers and optional verification inputs.</p></div>
          </div>
          <div className="surface-body developer-config-fields">
            <div className="field">
              <label htmlFor="migration-title">Name</label>
              <input id="migration-title" className="text-input" value={title} onChange={(event) => setTitle(event.target.value)} placeholder="Monthly claims pipeline" />
            </div>
            <div className="field">
              <label htmlFor="migration-id">Migration ID</label>
              <input id="migration-id" className="text-input mono" value={migrationId} onChange={(event) => setMigrationId(event.target.value)} placeholder="claims-pipeline-v1" />
            </div>
            <details className="advanced-config">
              <summary><span>Verification inputs</span><small>Optional</small><span className="advanced-chevron" aria-hidden="true">⌄</span></summary>
              <div className="advanced-config-fields">
                <div className="field">
                  <label htmlFor="snapshot-id">Input snapshot digest</label>
                  <input id="snapshot-id" className="text-input mono" value={inputSnapshotId} onChange={(event) => setInputSnapshotId(event.target.value)} placeholder="sha256:…" />
                </div>
                <div className="field">
                  <label htmlFor="environment-fingerprint">Environment fingerprint</label>
                  <input id="environment-fingerprint" className="text-input mono" value={environmentFingerprint} onChange={(event) => setEnvironmentFingerprint(event.target.value)} placeholder="Runtime / dependency fingerprint" />
                </div>
                <div className="field">
                  <label htmlFor="reference-input">Reference input path</label>
                  <input id="reference-input" className="text-input mono" value={referenceInputPath} onChange={(event) => setReferenceInputPath(event.target.value)} placeholder="/Volumes/catalog/schema/…" />
                </div>
                <div className="field">
                  <label htmlFor="reference-output">Reference output path</label>
                  <input id="reference-output" className="text-input mono" value={referenceOutputPath} onChange={(event) => setReferenceOutputPath(event.target.value)} placeholder="/Volumes/catalog/schema/…" />
                </div>
              </div>
            </details>
            <div className="config-scope-row">
              <span className={scope ? "scope-status-dot" : "scope-status-dot unavailable"} />
              <div><strong>Execution scope</strong><small>{scopesQuery.isLoading ? "Loading…" : scope ? `${scope.kind} · ${scope.id}` : scopesQuery.isError ? "Could not load scope" : "No scope available"}</small></div>
            </div>
          </div>
        </aside>
      </div>
    </>
  );
}
