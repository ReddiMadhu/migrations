export const API_BASE = (import.meta.env.VITE_API_URL as string | undefined) || "/api";

function getAuthToken(): string | null {
  return (import.meta.env.VITE_AUTH_TOKEN as string | undefined) || null;
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const token = getAuthToken();
  const headers: Record<string, string> = { "Content-Type": "application/json", ...init?.headers as Record<string, string> };
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }
  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers,
  });
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${await res.text()}`);
  }
  return res.json();
}

export interface Migration { migrationId: string; sessionId: string; }
export interface MigrationJobStart { jobId: string; migrationId: string; sessionId: string; }
export interface MigrationJob {
  jobId: string;
  migrationId: string;
  sessionId: string;
  state: "RUNNING" | "COMPLETED" | "FAILED" | string;
  finalState: string | null;
  startedAt: string;
}
export interface Session { sessionId: string; migrationId: string; scopeId: string; projection: Record<string, unknown>; }
export interface Scope { id: string; kind: string; parentId: string | null; }
export interface EventLog { sessionId: string; offset: number; limit: number; total: number; events: unknown[]; }
export interface Reconciliation {
  sessionId: string;
  verdicts: Record<string, unknown> | null;
  reportArtifact: unknown;
}
export interface ExecutionStatus {
  sessionId: string;
  executionId: string;
  runId: string;
  state: string;
  providerMessage: string | null;
}
