interface Props { events: unknown[]; sessionId: string; }

interface ActivityPresentation {
  title: string;
  detail: string;
  tool: string;
  tone: "neutral" | "accent" | "success" | "warning" | "danger";
}

const ACTIVITY_PRESENTATION: Record<string, ActivityPresentation> = {
  "migration/created": { title: "Migration session initialized", detail: "The submitted source was recorded for this run.", tool: "SESSION", tone: "neutral" },
  "workspace/discovered": { title: "Source workspace discovered", detail: "The SAS workspace and its source files were inspected.", tool: "DISCOVERY", tone: "accent" },
  "dependency/graph-created": { title: "Dependency graph assembled", detail: "Source dependencies were mapped before analysis.", tool: "DEPENDENCIES", tone: "accent" },
  "semantic-ir/created": { title: "SAS structure analyzed", detail: "A bounded structural representation was recorded.", tool: "SAS PARSER", tone: "accent" },
  "macro-resolution/created": { title: "Macro evidence resolved", detail: "Static macro-resolution evidence was recorded.", tool: "MACROS", tone: "accent" },
  "work-unit/created": { title: "Migration work unit created", detail: "A bounded context was prepared for the migration.", tool: "WORK UNIT", tone: "neutral" },
  "work-unit/updated": { title: "Migration context updated", detail: "The work unit was updated with new evidence.", tool: "WORK UNIT", tone: "neutral" },
  "work-unit/closed": { title: "Migration work unit closed", detail: "The bounded work unit reached a terminal state.", tool: "WORK UNIT", tone: "neutral" },
  "translation/started": { title: "Translation proposal started", detail: "The translation stage began; output is not verified until reconciliation.", tool: "TRANSLATOR", tone: "warning" },
  "translation/completed": { title: "Translation proposal recorded", detail: "A candidate was recorded for the execution harness.", tool: "PROPOSAL", tone: "accent" },
  "artifact/created": { title: "Artifact stored", detail: "An immutable artifact was added to the workbench.", tool: "ARTIFACT STORE", tone: "neutral" },
  "execution/started": { title: "Harness execution started", detail: "The candidate was handed to the configured executor.", tool: "EXECUTOR", tone: "warning" },
  "execution/completed": { title: "Harness execution completed", detail: "The executor recorded an outcome for this run.", tool: "EXECUTOR", tone: "success" },
  "reconciliation/started": { title: "Deterministic reconciliation started", detail: "Candidate output is being compared against the reference.", tool: "RECONCILER", tone: "warning" },
  "reconciliation/completed": { title: "Reconciliation results recorded", detail: "Parity verdicts were recorded as evidence.", tool: "RECONCILER", tone: "success" },
  "reconciliation/failed": { title: "Reconciliation did not complete", detail: "Review the failure evidence before proceeding.", tool: "RECONCILER", tone: "danger" },
  "difference/report-created": { title: "Difference report created", detail: "A report artifact captures the detected differences.", tool: "DIFF REPORT", tone: "warning" },
  "repair/started": { title: "Repair cycle started", detail: "A bounded repair attempt began.", tool: "REPAIR", tone: "warning" },
  "repair/proposed": { title: "Repair proposal recorded", detail: "A proposed change is available for guarded review.", tool: "PROPOSAL", tone: "accent" },
  "patch/applied": { title: "Patch applied", detail: "The recorded patch was applied to the candidate.", tool: "PATCH", tone: "warning" },
  "certificate/issued": { title: "Certificate issued", detail: "The workbench recorded a certificate for this run.", tool: "CERTIFICATE", tone: "success" },
  "migration/escalated": { title: "Operator review requested", detail: "The workflow paused because a human decision is required.", tool: "HUMAN REVIEW", tone: "warning" },
  "session/forked": { title: "Session forked", detail: "A new session was created from existing evidence.", tool: "SESSION", tone: "neutral" },
  "session/recovered": { title: "Session recovered", detail: "The workbench resumed from persisted state.", tool: "SESSION", tone: "neutral" },
  "execution/unknown": { title: "Execution outcome is unknown", detail: "The executor could not confirm the final run status.", tool: "EXECUTOR", tone: "danger" },
};

function displayTime(value: unknown): string {
  if (typeof value !== "string") return "Time unavailable";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(date);
}

export default function EventTimeline({ events }: Props) {
  if (events.length === 0) return <div className="empty-state">Run activity will appear here as evidence is recorded.</div>;

  return (
    <div className="activity-feed">
      {events.map((event, index) => {
        const entry = event as Record<string, unknown>;
        const eventType = typeof entry.type === "string" ? entry.type : "unknown";
        const presentation = ACTIVITY_PRESENTATION[eventType] ?? {
          title: eventType.replaceAll("/", " · ").replaceAll("-", " "),
          detail: "A workbench event was recorded.",
          tool: "WORKBENCH",
          tone: "neutral" as const,
        };
        const eventId = typeof entry.eventId === "string" ? entry.eventId : `${entry.sequence ?? index}-${index}`;
        const hash = typeof entry.hash === "string" ? entry.hash : null;
        const sequence = typeof entry.sequence === "number" ? entry.sequence : index + 1;

        return (
          <article className="activity-item" key={eventId}>
            <div className={`activity-avatar activity-avatar-${presentation.tone}`} aria-hidden="true">
              {presentation.tone === "success" ? "✓" : presentation.tone === "danger" ? "!" : "✳"}
            </div>
            <div className="activity-content">
              <div className="activity-heading">
                <strong>{presentation.title}</strong>
                <time>{displayTime(entry.createdAt)}</time>
              </div>
              <p>{presentation.detail}</p>
              <div className="activity-meta">
                <span className={`tool-chip tool-chip-${presentation.tone}`}><span className="tool-chip-icon">↗</span>{presentation.tool}</span>
                <span className="event-type-chip">{eventType}</span>
                <span className="activity-sequence">#{String(sequence).padStart(2, "0")}</span>
                {hash && <span className="activity-hash" title={hash}>SHA · {hash.slice(0, 10)}</span>}
              </div>
            </div>
          </article>
        );
      })}
    </div>
  );
}
