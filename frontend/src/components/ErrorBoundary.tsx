import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
  fallback?: (error: Error, reset: () => void) => ReactNode;
}

interface State {
  error: Error | null;
}

const ACTIONABLE_STATES: Record<string, string> = {
  HUMAN_REQUIRED: "Human intervention required — review the migration state and approve or reject the proposed action.",
  EXECUTION_FAILED: "Execution failed — check the execution logs and retry or adjust the candidate.",
  RECONCILIATION_FAILED: "Reconciliation failed — the candidate output does not match the reference. Review differences.",
  MIGRATION_RUNNING: "Migration is running — please wait for completion.",
  WAITING_APPROVAL: "Waiting for operator approval — review the pending request.",
  COMPLETED: "Migration completed successfully.",
};

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, _info: ErrorInfo): void {
    console.error("ErrorBoundary caught:", error);
  }

  reset = (): void => {
    this.setState({ error: null });
  };

  render(): ReactNode {
    if (this.state.error) {
      if (this.props.fallback) {
        return this.props.fallback(this.state.error, this.reset);
      }
      const message = ACTIONABLE_STATES[this.state.error.name] || this.state.error.message;
      return (
        <div role="alert" style={{ padding: "1rem", border: "1px solid #e74c3c", borderRadius: "4px", background: "#fdf2f2" }}>
          <h3 style={{ margin: "0 0 0.5rem 0", color: "#e74c3c" }}>Something went wrong</h3>
          <p style={{ margin: "0 0 1rem 0" }}>{message}</p>
          <button onClick={this.reset} style={{ padding: "0.5rem 1rem", cursor: "pointer" }}>
            Try again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
