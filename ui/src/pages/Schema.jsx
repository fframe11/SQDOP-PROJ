import React, { useState } from "react";
import { useApi, postApi } from "../hooks/useApi";

export default function Schema() {
  const [statusFilter, setStatusFilter] = useState("PENDING");
  const proposals = useApi(`/schema/proposals?status=${statusFilter}`, { refreshInterval: 10000 });

  const [selectedId, setSelectedId] = useState(null);
  const [primaryKeyOverride, setPrimaryKeyOverride] = useState("");
  const [dateColumnOverride, setDateColumnOverride] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [actionResult, setActionResult] = useState(null);

  const selectedProposal = proposals.data?.proposals?.find((p) => p.id === selectedId);

  const handleAction = async (proposalId, action) => {
    setSubmitting(true);
    setActionResult(null);
    try {
      let endpoint = `/schema/proposals/${proposalId}/${action}`;
      if (action === "approve") {
        const params = [];
        if (primaryKeyOverride) params.push(`primary_key=${encodeURIComponent(primaryKeyOverride)}`);
        if (dateColumnOverride) params.push(`date_column=${encodeURIComponent(dateColumnOverride)}`);
        if (params.length > 0) {
          endpoint += `?${params.join("&")}`;
        }
      }

      const res = await postApi(endpoint);
      setActionResult({ success: true, message: res.message || `Proposal ${action}d successfully.` });
      
      // Reset inputs & refresh
      setPrimaryKeyOverride("");
      setDateColumnOverride("");
      setSelectedId(null);
      proposals.refetch();
    } catch (err) {
      setActionResult({ success: false, message: `Failed to ${action} proposal: ${err.message}` });
    } finally {
      setSubmitting(false);
    }
  };

  const getSeverityColor = (severity) => {
    if (severity >= 10) return "#f43f5e"; // Critical
    if (severity >= 5) return "#fbbf24"; // High/Warning
    return "#38bdf8"; // Low/Info
  };

  const getErrorTypeBadge = (error) => {
    if (error === "missing_column") return <span className="badge badge-danger">MISSING COLUMN</span>;
    if (error === "type_mismatch") return <span className="badge badge-warning">TYPE MISMATCH</span>;
    return <span className="badge badge-success">NEW COLUMN</span>;
  };

  return (
    <div className="page-container" style={{ overflowY: "auto", height: "calc(100vh - 56px)", paddingBottom: "2rem" }}>
      <div className="page-header">
        <h1>Schema Governance</h1>
        <p>Review and approve/reject schema drift proposals detected by Spark engines</p>
      </div>

      {actionResult && (
        <div className={`alert-box ${actionResult.success ? "info" : "critical"}`} style={{ marginBottom: "1rem" }}>
          <span>{actionResult.message}</span>
        </div>
      )}

      {/* Navigation Filter Tabs */}
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1rem" }}>
        {["PENDING", "APPROVED", "REJECTED"].map((tab) => (
          <button
            key={tab}
            className={`btn ${statusFilter === tab ? "btn-primary" : "btn-secondary"}`}
            style={{ padding: "0.4rem 1rem", fontSize: "0.8rem" }}
            onClick={() => {
              setStatusFilter(tab);
              setSelectedId(null);
              setActionResult(null);
            }}
          >
            {tab}
          </button>
        ))}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: selectedProposal ? "1fr 1fr" : "1fr", gap: "1.5rem" }}>
        {/* Left Column: Proposals List */}
        <div className="glass-card animate-in">
          <h3 className="section-title">{statusFilter} Proposals</h3>
          <p className="section-subtitle">Awaiting schema validation review</p>

          {proposals.loading ? (
            <div className="loading-state">
              <div className="loading-spinner"></div>
              <span>Loading proposals...</span>
            </div>
          ) : proposals.error ? (
            <div className="alert-box critical">Failed to fetch proposals: {proposals.error}</div>
          ) : !proposals.data?.proposals || proposals.data.proposals.length === 0 ? (
            <div style={{ padding: "2rem", textAlign: "center", color: "var(--text-muted)" }}>
              No {statusFilter.toLowerCase()} proposals found.
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
              {proposals.data.proposals.map((p) => {
                const isSelected = p.id === selectedId;
                return (
                  <div
                    key={p.id}
                    className="glass-card"
                    style={{
                      padding: "1rem",
                      cursor: "pointer",
                      border: isSelected ? "1px solid var(--accent-blue)" : "1px solid var(--border-card)",
                      background: isSelected ? "rgba(56, 189, 248, 0.05)" : "var(--bg-card)",
                    }}
                    onClick={() => {
                      setSelectedId(p.id);
                      setActionResult(null);
                    }}
                  >
                    <div style={{ display: "flex", justifycontent: "space-between", alignitems: "center", marginBottom: "0.5rem" }}>
                      <span style={{ fontSize: "1rem", fontWeight: 700, color: "var(--text-main)" }}>
                        {p.table_name}
                      </span>
                      <span
                        className="badge"
                        style={{
                          backgroundColor: getSeverityColor(p.drift_severity),
                          color: "#fff",
                          fontSize: "0.7rem",
                        }}
                      >
                        SEVERITY: {p.drift_severity}
                      </span>
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.75rem", color: "var(--text-muted)" }}>
                      <div>Run ID: <span style={{ fontFamily: "var(--font-mono)", color: "var(--text-main)" }}>{p.run_id}</span></div>
                      <div>Proposed: <span style={{ color: "var(--text-main)" }}>{new Date(p.proposed_at || p.timestamp).toLocaleString()}</span></div>
                      {p.proposed_by && <div>Source: <span style={{ color: "var(--text-main)" }}>{p.proposed_by}</span></div>}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Right Column: Detailed Proposal view & actions */}
        {selectedProposal && (
          <div className="glass-card animate-in">
            <h3 className="section-title">Proposal Details</h3>
            <p className="section-subtitle">Reviewing schema modifications for {selectedProposal.table_name}</p>

            <div style={{ display: "flex", flexDirection: "column", gap: "1rem", marginTop: "1rem" }}>
              {/* Drift details */}
              <div>
                <h4 style={{ fontSize: "0.85rem", color: "var(--text-muted)", marginBottom: "0.5rem" }}>Drift Modifications:</h4>
                <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                  {Object.entries(selectedProposal.drift_details || {}).map(([col, details]) => (
                    <div
                      key={col}
                      className="glass-card"
                      style={{
                        padding: "0.75rem",
                        display: "flex",
                        justifycontent: "space-between",
                        alignitems: "center",
                        fontSize: "0.8rem",
                      }}
                    >
                      <div>
                        <span style={{ fontFamily: "var(--font-mono)", fontWeight: 700 }}>{col}</span>
                        {details.expected && (
                          <span style={{ color: "var(--text-muted)", fontSize: "0.75rem", marginLeft: "0.5rem" }}>
                            (Expected: {details.expected} | Actual: {details.actual})
                          </span>
                        )}
                        {details.action && (
                          <div style={{ fontSize: "0.7rem", color: "var(--accent-blue)", marginTop: "0.25rem" }}>
                            Auto-heal: {details.action}
                          </div>
                        )}
                      </div>
                      {getErrorTypeBadge(details.error)}
                    </div>
                  ))}
                </div>
              </div>

              {/* Schema JSON Comparison */}
              <div>
                <h4 style={{ fontSize: "0.85rem", color: "var(--text-muted)", marginBottom: "0.5rem" }}>Proposed Schema Spec:</h4>
                <pre
                  style={{
                    background: "rgba(0, 0, 0, 0.4)",
                    padding: "1rem",
                    borderRadius: "8px",
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.75rem",
                    maxHeight: "200px",
                    overflowY: "auto",
                    border: "1px solid var(--border-card)",
                    color: "var(--accent-blue)",
                  }}
                >
                  {JSON.stringify(selectedProposal.proposed_schema || {}, null, 2)}
                </pre>
              </div>

              {/* Approval Override inputs (only for PENDING) */}
              {statusFilter === "PENDING" && (
                <div className="glass-card" style={{ padding: "1rem", background: "rgba(255,255,255,0.02)" }}>
                  <h4 style={{ fontSize: "0.85rem", color: "var(--text-main)", marginBottom: "0.75rem" }}>
                    Approval Overrides (Optional)
                  </h4>
                  <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                    <div>
                      <label style={{ display: "block", fontSize: "0.75rem", color: "var(--text-muted)", marginBottom: "0.25rem" }}>
                        Primary Key Column
                      </label>
                      <input
                        type="text"
                        placeholder="e.g. id"
                        value={primaryKeyOverride}
                        onChange={(e) => setPrimaryKeyOverride(e.target.value)}
                        style={{
                          width: "100%",
                          padding: "0.5rem",
                          borderRadius: "4px",
                          border: "1px solid var(--border-card)",
                          background: "var(--bg-primary)",
                          color: "#fff",
                        }}
                      />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: "0.75rem", color: "var(--text-muted)", marginBottom: "0.25rem" }}>
                        Date Column
                      </label>
                      <input
                        type="text"
                        placeholder="e.g. updated_at"
                        value={dateColumnOverride}
                        onChange={(e) => setDateColumnOverride(e.target.value)}
                        style={{
                          width: "100%",
                          padding: "0.5rem",
                          borderRadius: "4px",
                          border: "1px solid var(--border-card)",
                          background: "var(--bg-primary)",
                          color: "#fff",
                        }}
                      />
                    </div>
                  </div>
                </div>
              )}

              {/* Interactive buttons */}
              {statusFilter === "PENDING" ? (
                <div style={{ display: "flex", gap: "1rem", marginTop: "0.5rem" }}>
                  <button
                    className="btn btn-primary"
                    disabled={submitting}
                    style={{ flex: 1, padding: "0.75rem" }}
                    onClick={() => handleAction(selectedProposal.id, "approve")}
                  >
                    {submitting ? "Approving..." : "Approve Proposal"}
                  </button>
                  <button
                    className="btn"
                    disabled={submitting}
                    style={{ flex: 1, padding: "0.75rem", backgroundColor: "var(--accent-red)", color: "#fff" }}
                    onClick={() => handleAction(selectedProposal.id, "reject")}
                  >
                    {submitting ? "Rejecting..." : "Reject Proposal"}
                  </button>
                </div>
              ) : (
                <div
                  className="glass-card"
                  style={{
                    padding: "0.75rem",
                    textAlign: "center",
                    border: `1px solid ${statusFilter === "APPROVED" ? "var(--accent-green)" : "var(--accent-red)"}`,
                    color: statusFilter === "APPROVED" ? "var(--accent-green)" : "var(--accent-red)",
                    fontWeight: 700,
                  }}
                >
                  PROPOSAL {statusFilter} AT {new Date(selectedProposal.resolved_at).toLocaleString()}
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
