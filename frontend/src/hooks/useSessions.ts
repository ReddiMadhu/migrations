import { useEffect, useRef, useState, useCallback } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiFetch, API_BASE, type ExecutionStatus, type Reconciliation, type Session, type EventLog } from "../api/client";

function getAuthToken(): string | null {
  return (import.meta.env.VITE_AUTH_TOKEN as string | undefined) || null;
}

export function useSession(sessionId: string) {
  return useQuery({
    queryKey: ["session", sessionId],
    queryFn: () => apiFetch<Session>(`/sessions/${sessionId}`),
  });
}

export function useEvents(sessionId: string) {
  const queryClient = useQueryClient();
  const lastEventIdRef = useRef<string | null>(null);
  const [offset, setOffset] = useState(0);
  const [accumulated, setAccumulated] = useState<unknown[]>([]);
  const limit = 50;

  useEffect(() => {
    const token = getAuthToken();
    let cancelled = false;
    let controller: AbortController | null = null;

    const connect = async () => {
      controller = new AbortController();
      const headers: Record<string, string> = { Accept: "text/event-stream" };
      if (token) headers["Authorization"] = `Bearer ${token}`;
      if (lastEventIdRef.current) headers["Last-Event-ID"] = lastEventIdRef.current;

      try {
        const res = await fetch(`${API_BASE}/sessions/${sessionId}/stream`, {
          headers,
          signal: controller.signal,
        });
        const reader = res.body?.getReader();
        if (!reader) return;
        const decoder = new TextDecoder();
        let buffer = "";

        while (!cancelled) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";
          for (const line of lines) {
            if (line.startsWith("id: ")) {
              lastEventIdRef.current = line.slice(4);
            }
            if (line.startsWith("event: migration-event") || line.startsWith("event: approval")) {
              void queryClient.invalidateQueries({ queryKey: ["events", sessionId] });
              void queryClient.invalidateQueries({ queryKey: ["session", sessionId] });
              void queryClient.invalidateQueries({ queryKey: ["approval", sessionId] });
            }
          }
        }
      } catch {
        if (!cancelled) setTimeout(connect, 1000);
      }
    };

    void connect();
    return () => {
      cancelled = true;
      controller?.abort();
    };
  }, [queryClient, sessionId]);

  const query = useQuery({
    queryKey: ["events", sessionId, offset],
    queryFn: () => apiFetch<EventLog>(`/sessions/${sessionId}/events?offset=${offset}&limit=${limit}`),
  });

  useEffect(() => {
    if (query.data) {
      setAccumulated((prev) => [...prev, ...query.data!.events]);
    }
  }, [query.data]);

  const loadMore = useCallback(() => {
    setOffset((prev) => prev + limit);
  }, []);

  return { ...query, accumulated, loadMore, hasMore: query.data ? offset + limit < query.data.total : false };
}

export function useApproval(sessionId: string) {
  return useQuery({
    queryKey: ["approval", sessionId],
    queryFn: () => apiFetch<{
      pending: boolean;
      kind: "execution" | "certificate" | null;
      request: Record<string, unknown> | null;
    }>(`/sessions/${sessionId}/approval`),
    refetchInterval: 1000,
  });
}

export function useReconciliation(sessionId: string) {
  return useQuery({
    queryKey: ["reconciliation", sessionId],
    queryFn: () => apiFetch<Reconciliation>(`/sessions/${sessionId}/reconciliation`),
  });
}

export function useRepollExecution(sessionId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (executionId: string) =>
      apiFetch<ExecutionStatus>(`/sessions/${sessionId}/executions/${encodeURIComponent(executionId)}/repoll`, { method: "POST" }),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["events", sessionId] }),
        queryClient.invalidateQueries({ queryKey: ["session", sessionId] }),
      ]);
    },
  });
}

export function useCancelExecution(sessionId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (executionId: string) =>
      apiFetch<{ sessionId: string; executionId: string; runId: string; accepted: boolean }>(
        `/sessions/${sessionId}/executions/${encodeURIComponent(executionId)}/cancel`,
        { method: "POST" },
      ),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["events", sessionId] }),
        queryClient.invalidateQueries({ queryKey: ["session", sessionId] }),
      ]);
    },
  });
}

export function useRespondToApproval(sessionId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (decision: "approve" | "reject") =>
      apiFetch<{ approved: boolean; state: string }>(`/sessions/${sessionId}/approve?decision=${decision}`, { method: "POST" }),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["approval", sessionId] }),
        queryClient.invalidateQueries({ queryKey: ["session", sessionId] }),
        queryClient.invalidateQueries({ queryKey: ["events", sessionId] }),
      ]);
    },
  });
}
