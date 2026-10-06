import { create } from "zustand";

interface SessionStore {
  selectedSessionId: string | null;
  setSelectedSession: (id: string | null) => void;
}

export const useSessionStore = create<SessionStore>((set) => ({
  selectedSessionId: null,
  setSelectedSession: (id) => set({ selectedSessionId: id }),
}));
