import React from "react";

export default class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null, errorInfo: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    this.setState({ errorInfo });
    console.error("ErrorBoundary caught:", error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{
          padding: "2rem",
          margin: "2rem auto",
          maxWidth: "700px",
          background: "rgba(244, 63, 94, 0.1)",
          border: "1px solid rgba(244, 63, 94, 0.3)",
          borderRadius: "12px",
          color: "#f8d7da",
          fontFamily: "'Inter', sans-serif"
        }}>
          <h2 style={{ color: "#f43f5e", marginBottom: "1rem" }}>⚠️ Something went wrong</h2>
          <p style={{ marginBottom: "0.5rem", color: "#fca5a5" }}>
            The page encountered an error and could not render properly.
          </p>
          <details style={{ marginTop: "1rem" }}>
            <summary style={{ cursor: "pointer", color: "#fb7185" }}>Show Error Details</summary>
            <pre style={{
              marginTop: "0.5rem",
              padding: "1rem",
              background: "rgba(0,0,0,0.3)",
              borderRadius: "8px",
              fontSize: "12px",
              overflow: "auto",
              maxHeight: "300px",
              color: "#fda4af"
            }}>
              {this.state.error && this.state.error.toString()}
              {"\n\n"}
              {this.state.errorInfo && this.state.errorInfo.componentStack}
            </pre>
          </details>
          <button
            onClick={() => {
              this.setState({ hasError: false, error: null, errorInfo: null });
              window.location.reload();
            }}
            style={{
              marginTop: "1rem",
              padding: "0.5rem 1.5rem",
              background: "#f43f5e",
              color: "#fff",
              border: "none",
              borderRadius: "8px",
              cursor: "pointer",
              fontWeight: "600"
            }}
          >
            🔄 Reload Page
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}
