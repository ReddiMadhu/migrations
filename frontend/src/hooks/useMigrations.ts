import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiFetch, type Migration, type MigrationJob, type MigrationJobStart } from "../api/client";

export interface CreateMigrationInput {
  migrationId: string;
  scopeId: string;
  title: string;
  sasSource: string;
  sasSourcePath: string;
  sasFiles: Record<string, string>;
  inputSnapshotId: string | null;
  environmentFingerprint: string | null;
  referenceInputPath: string | null;
  referenceOutputPath: string | null;
}

export function useCreateMigration() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: CreateMigrationInput) =>
      apiFetch<Migration>("/migrations", { method: "POST", body: JSON.stringify(body) }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["migrations"] }),
  });
}

export function useRunMigration() {
  return useMutation({
    mutationFn: (migrationId: string) =>
      apiFetch<MigrationJobStart>(`/migrations/${encodeURIComponent(migrationId)}/run`, { method: "POST" }),
  });
}

export function useMigrationJob(jobId: string | null) {
  return useQuery({
    queryKey: ["migration-job", jobId],
    queryFn: () => apiFetch<MigrationJob>(`/jobs/${encodeURIComponent(jobId!)}`),
    enabled: Boolean(jobId),
    refetchInterval: (query) => query.state.data?.state === "RUNNING" ? 1000 : false,
  });
}
