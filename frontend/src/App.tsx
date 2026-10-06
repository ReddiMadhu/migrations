import { useState } from "react";
import MigrationList from "./views/MigrationList";
import SessionDetail from "./views/SessionDetail";
import ScopeInspector from "./views/ScopeInspector";
import "./styles.css";

type View = "migrations" | "session" | "scopes";
type Theme = "light" | "dark";

export default function App() {
  const [view, setView] = useState<View>("migrations");
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [jobId, setJobId] = useState<string | null>(null);
  const [theme, setTheme] = useState<Theme>(() => {
    const savedTheme = window.localStorage.getItem("workbench-theme");
    return savedTheme === "dark" ? "dark" : "light";
  });

  const openSession = (id: string, job: string | null = null) => {
    setSessionId(id);
    setJobId(job);
    setView("session");
  };

  return (
    <div className="app-shell" data-theme={theme}>
      <aside className="sidebar">
        <a className="brand" href="#" onClick={(event) => { event.preventDefault(); setView("migrations"); }}>
          <span className="brand-mark">S</span>
          <span className="brand-copy">
            <strong>Workbench</strong>
            <small>Developer workbench</small>
          </span>
        </a>

        <div className="sidebar-section-label">WORKSPACE</div>
        <nav className="primary-nav" aria-label="Main navigation">
          <button className={view === "migrations" || view === "session" ? "nav-item active" : "nav-item"} onClick={() => setView("migrations")}>
            <span className="nav-icon">⇄</span>
            <span>Migrations</span>
          </button>
          <button className={view === "scopes" ? "nav-item active" : "nav-item"} onClick={() => setView("scopes")}>
            <span className="nav-icon">⌘</span>
            <span>Scopes</span>
          </button>
        </nav>

        <div className="sidebar-spacer" />
        <div className="sidebar-note">
          <span className="status-dot" />
          <div><strong>Local environment</strong><small>API connection is proxied</small></div>
        </div>
        <div className="sidebar-footer">SAS → PYSPARK <span>v1.0</span></div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <div className="breadcrumbs">
            <span>Workspace</span><span className="crumb-separator">/</span>
            <strong>{view === "scopes" ? "Scopes" : view === "session" ? "Session detail" : "Migrations"}</strong>
          </div>
          <div className="topbar-right">
            <span className="environment-pill"><span className="status-dot" /> Development</span>
            <button
              className="theme-toggle"
              type="button"
              aria-label={`Switch to ${theme === "light" ? "dark" : "light"} theme`}
              aria-pressed={theme === "dark"}
              onClick={() => {
                const nextTheme = theme === "light" ? "dark" : "light";
                window.localStorage.setItem("workbench-theme", nextTheme);
                setTheme(nextTheme);
              }}
            >
              <span aria-hidden="true">{theme === "light" ? "☾" : "☀"}</span>
              <span className="theme-toggle-label">{theme === "light" ? "Dark" : "Light"}</span>
            </button>
            <div className="avatar" aria-label="Operator">OP</div>
          </div>
        </header>

        <div className="page-content">
          {view === "migrations" && (
            <MigrationList onSelectSession={openSession} onOpenScopes={() => setView("scopes")} />
          )}
          {view === "session" && sessionId && (
            <SessionDetail sessionId={sessionId} jobId={jobId} onBack={() => setView("migrations")} />
          )}
          {view === "scopes" && <ScopeInspector />}
        </div>
      </main>
    </div>
  );
}
