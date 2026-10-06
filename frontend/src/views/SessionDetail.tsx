import { useMigrationJob } from "../hooks/useMigrations";
import {
  useApproval,
  useCancelExecution,
  useEvents,
  useRepollExecution,
  useReconciliation,
  useRespondToApproval,
  useSession,
} from "../hooks/useSessions";
import EventTimeline from "../components/EventTimeline";
import StateBadge from "../components/StateBadge";

interface Props { sessionId: string; jobId: string | null; onBack: () => void; }

interface ExecutionRecord {
  executionId: string;
  runId: string | null;
  state: "RUNNING" | "UNKNOWN" | "COMPLETED";
}

export default function SessionDetail({ sessionId, jobId, onBack }: Props) {
  const { data: session, isLoading, isError, error } = useSession(sessionId);
  const { data: events, accumulated, isError: eventsError, error: eventsErrorValue } = useEvents(sessionId);
  const { data: approval } = useApproval(sessionId);
  const job = useMigrationJob(jobId);
  const reconciliation = useReconciliation(sessionId);
  const repollExecution = useRepollExecution(sessionId);
  const cancelExecution = useCancelExecution(sessionId);
  const respondToApproval = useRespondToApproval(sessionId);

  if (isLoading) return <div className="loading-state">Loading session details…</div>;
  if (isError) return <div><button className="back-button" onClick={onBack}>← Back to migrations</button><div className="alert" role="alert">Could not load this session: {String(error)}</div></div>;
  if (!session) return <div><button className="back-button" onClick={onBack}>← Back to migrations</button><div className="empty-state">Session not found.</div></div>;

  const projection = session.projection as Record<string, unknown>;
  const state = typeof projection.state === "string" ? projection.state : "UNKNOWN";
  const candidateArtifact = approval?.request?.candidateArtifact as { digest?: unknown } | undefined;
  const verdicts = reconciliation.data?.verdicts ?? null;
  const sessionEvents = accumulated.length > 0 ? accumulated : events?.events ?? [];
  const executionMap = new Map<string, ExecutionRecord>();
  for (const rawEvent of sessionEvents) {
    const event = rawEvent as Record<string, unknown>;
    const data = event.data && typeof event.data === "object" ? event.data as Record<string, unknown> : {};
    if (event.type !== "execution/started" && event.type !== "execution/unknown" && event.type !== "execution/completed") continue;
    if (typeof data.executionId !== "string") continue;
    const previous = executionMap.get(data.executionId);
    const runId = typeof data.runId === "string" ? data.runId : previous?.runId ?? null;
    executionMap.set(data.executionId, {
      executionId: data.executionId,
      runId,
      state: event.type === "execution/completed" ? "COMPLETED" : event.type === "execution/unknown" ? "UNKNOWN" : previous?.state ?? "RUNNING",
    });
  }
  const executions = [...executionMap.values()];

  return (
    <>
      <button className="back-button" onClick={onBack}>← Back to migrations</button>
      <div className="page-heading">
        <div>
          <p className="eyebrow">Migration session</p>
          <h1>{session.migrationId}</h1>
          <div className="detail-meta">
            <span>Session <strong className="mono">{session.sessionId}</strong></span>
            <span>Scope <strong className="mono">{session.scopeId}</strong></span>
          </div>
        </div>
        <StateBadge state={state} />
      </div>

      <div className="detail-grid">
        {jobId && (
          <section className="surface job-status-card">
            <div className="surface-header">
              <div><h2>Migration job</h2><p className="mono">{jobId}</p></div>
              <span className="state-badge" data-tone={job.data?.state === "FAILED" ? "danger" : job.data?.state === "COMPLETED" ? "success" : "warning"}>
                {job.isLoading ? "LOADING" : job.data?.state ?? "UNKNOWN"}
              </span>
            </div>
            {job.isError && <div className="surface-body"><div className="alert" role="alert">Could not load job status: {String(job.error)}</div></div>}
            {job.data && (
              <div className="surface-body job-status-body">
                <span>Started <strong>{job.data.startedAt}</strong></span>
                {job.data.finalState && <span>Final state <strong>{job.data.finalState}</strong></span>}
              </div>
            )}
          </section>
        )}
        {approval?.pending && (
          <section className="surface approval-card">
            <div className="surface-header"><div><h2>{approval.kind === "certificate" ? "Certification decision" : "Execution approval"}</h2><p>An operator decision is required to continue.</p></div><span className="state-badge" data-tone="warning">REVIEW REQUIRED</span></div>
            <div className="surface-body">
              <p className="approval-message">{String(approval.request?.question ?? "Review the candidate and choose how to proceed.")}</p>
              <div className="digest-box">Candidate digest: {String(candidateArtifact?.digest ?? "not provided")}</div>
              <div className="button-row">
                <button className="button button-primary" onClick={() => respondToApproval.mutate("approve")} disabled={respondToApproval.isPending}>
                  {approval.kind === "certificate" ? "Issue certificate" : "Approve execution"}
                </button>
                <button className="button button-danger" onClick={() => respondToApproval.mutate("reject")} disabled={respondToApproval.isPending}>
                  {approval.kind === "certificate" ? "Escalate" : "Reject candidate"}
                </button>
              </div>
              {respondToApproval.error && <p className="inline-error" role="alert">{String(respondToApproval.error)}</p>}
            </div>
          </section>
        )}

        <section className="surface">
          <div className="surface-header"><div><h2>Reconciliation</h2><p>Latest deterministic comparison results.</p></div></div>
          {reconciliation.isError ? (
            <div className="surface-body"><div className="alert" role="alert">Could not load reconciliation: {String(reconciliation.error)}</div></div>
          ) : verdicts ? (
            <div className="surface-body verdict-list">
              <div className="verdict-row"><span>Schema</span><strong>{String(verdicts.schemaVerdict ?? "—")}</strong></div>
              <div className="verdict-row"><span>Aggregate</span><strong>{String(verdicts.aggregateVerdict ?? "—")}</strong></div>
              <div className="verdict-row"><span>Rows</span><strong>{String(verdicts.rowVerdict ?? "—")}</strong></div>
              <div className="verdict-row"><span>Statistical</span><strong>{String(verdicts.statisticalVerdict ?? "—")}</strong></div>
              <div className="verdict-row"><span>Differences</span><strong>{String(verdicts.differenceCount ?? "—")}</strong></div>
            </div>
          ) : <div className="empty-state">{reconciliation.isLoading ? "Loading reconciliation…" : "Reconciliation results will appear here when available."}</div>}
        </section>
      </div>

      {executions.length > 0 && (
        <section className="surface executions-section">
          <div className="surface-header"><div><h2>External executions</h2><p>Inspect or cancel executor runs when an outcome is unresolved.</p></div></div>
          <div className="surface-body execution-list">
            {executions.map((execution) => (
              <div className="execution-row" key={execution.executionId}>
                <div><strong className="mono">{execution.executionId}</strong><small>{execution.runId ? `Run ${execution.runId}` : "External run ID unavailable"}</small></div>
                <span className="event-type-chip">{execution.state}</span>
                <div className="button-row">
                  {execution.state === "UNKNOWN" && execution.runId && (
                    <button className="button button-secondary" type="button" onClick={() => repollExecution.mutate(execution.executionId)} disabled={repollExecution.isPending}>
                      {repollExecution.isPending && repollExecution.variables === execution.executionId ? "Checking…" : "Check status"}
                    </button>
                  )}
                  {execution.state !== "COMPLETED" && execution.runId && (
                    <button className="button button-danger" type="button" onClick={() => cancelExecution.mutate(execution.executionId)} disabled={cancelExecution.isPending}>
                      {cancelExecution.isPending && cancelExecution.variables === execution.executionId ? "Cancelling…" : "Cancel run"}
                    </button>
                  )}
                </div>
              </div>
            ))}
            {repollExecution.error && <div className="alert" role="alert">Could not check execution status: {String(repollExecution.error)}</div>}
            {repollExecution.data && <div className="execution-action-result" role="status">Provider status: {repollExecution.data.state}{repollExecution.data.providerMessage ? ` · ${repollExecution.data.providerMessage}` : ""}</div>}
            {cancelExecution.error && <div className="alert" role="alert">Could not cancel execution: {String(cancelExecution.error)}</div>}
            {cancelExecution.data && <div className="execution-action-result" role="status">{cancelExecution.data.accepted ? "Cancellation accepted by executor." : "Executor did not accept cancellation."}</div>}
          </div>
        </section>
      )}

      <section className="surface events-section">
        <div className="surface-header">
          <div><h2>Workbench activity</h2><p>Recorded proposals, tool steps, and verification evidence from this run.</p></div>
          <span className="count-pill">{events?.total ?? 0} EVENTS</span>
        </div>
        {eventsError ? <div className="surface-body"><div className="alert" role="alert">Could not load session events: {String(eventsErrorValue)}</div></div> : <EventTimeline events={sessionEvents} sessionId={sessionId} />}
      </section>
    </>
  );
}
