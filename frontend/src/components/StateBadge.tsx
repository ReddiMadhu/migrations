interface Props { state: string; }

const STATE_TONES: Record<string, string> = {
  CERTIFIED: "success",
  COMPLETED: "success",
  FAILED: "danger",
  HUMAN_REQUIRED: "danger",
  TRANSLATING: "warning",
  CANDIDATE_READY: "warning",
  EXECUTING: "warning",
  RECONCILING: "warning",
  DIAGNOSING: "warning",
  REPAIRING: "warning",
};

export default function StateBadge({ state }: Props) {
  const normalized = state.toUpperCase();
  return <span className="state-badge" data-tone={STATE_TONES[normalized] ?? "default"}>{normalized.replaceAll("_", " ")}</span>;
}
