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

  React.useEffect(() => {
    if (selectedProposal) {
      if (selectedProposal.table_name === "products") {
        setPrimaryKeyOverride("product_id");
        setDateColumnOverride("");
      } else if (selectedProposal.table_name === "orders") {
        setPrimaryKeyOverride("order_id");
        setDateColumnOverride("order_date");
      } else if (selectedProposal.table_name === "users") {
        setPrimaryKeyOverride("id");
        setDateColumnOverride("created_utc");
      } else {
        setPrimaryKeyOverride("");
        setDateColumnOverride("");
      }
    } else {
      setPrimaryKeyOverride("");
      setDateColumnOverride("");
    }
  }, [selectedId, selectedProposal]);

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

  // Format date/time to matching the mockup "Proposed detected at HH:MM GST" or similar
  const formatProposedTime = (isoString) => {
    if (!isoString) return "";
    try {
      const date = new Date(isoString);
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      return `Proposed detected at ${hours}:${minutes} GST`;
    } catch (e) {
      return "";
    }
  };

  const getModificationLines = (proposal) => {
    if (!proposal || !proposal.drift_details) return [];
    return Object.entries(proposal.drift_details).map(([col, details]) => {
      let typeLabel = "New column";
      let badgeLabel = "NEW COLUMN";
      let badgeColor = "#10b981"; // Green
      let detailText = col;

      if (details.error === "type_mismatch" || details.error === "type") {
        typeLabel = "Type column";
        badgeLabel = "TYPE MISMATCH";
        badgeColor = "#fbbf24"; // Amber/Yellow
        detailText = `${col} ${details.actual || "type"}`;
      } else if (details.error === "missing_column") {
        typeLabel = "Missing column";
        badgeLabel = "MISSING COLUMN";
        badgeColor = "#f43f5e"; // Red
        detailText = col;
      }

      return {
        typeLabel,
        badgeLabel,
        badgeColor,
        detailText
      };
    });
  };

  return (
    <div className="page-container" style={{ overflowY: "auto", height: "calc(100vh - 56px)", paddingBottom: "2rem" }}>
      <div className="page-header" style={{ marginBottom: "1.5rem" }}>
        <h1>Schema Governance</h1>
        <p>Review and approve/reject schema drift proposals detected by Spark engines</p>
      </div>

      {actionResult && (
        <div className={`alert-box ${actionResult.success ? "info" : "critical"}`} style={{ marginBottom: "1rem" }}>
          <span>{actionResult.message}</span>
        </div>
      )}

      {/* Navigation Filter Tabs */}
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1.5rem" }}>
        {["PENDING", "APPROVED", "REJECTED"].map((tab) => (
          <button
            key={tab}
            className={`btn ${statusFilter === tab ? "btn-primary" : "btn-secondary"}`}
            style={{ 
              padding: "0.5rem 1.25rem", 
              fontSize: "0.85rem",
              borderRadius: "20px",
              background: statusFilter === tab ? "var(--accent-blue)" : "rgba(255,255,255,0.03)",
              border: "1px solid rgba(255,255,255,0.08)",
              color: "#fff",
              cursor: "pointer"
            }}
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

      <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", gap: "1.5rem", alignItems: "start" }}>
        {/* Left Column: Proposals List */}
        <div>
          <h3 style={{ fontSize: "1rem", fontWeight: 600, color: "#fff", marginBottom: "0.25rem" }}>
            {statusFilter === "PENDING" ? "PENDING" : statusFilter === "APPROVED" ? "APPROVED" : "REJECTED"} Proposals
          </h3>
          <p style={{ fontSize: "0.78rem", color: "var(--text-muted)", marginBottom: "1rem" }}>
            Awaiting schema validation review
          </p>

          <div 
            className="glass-card" 
            style={{ 
              padding: "1rem", 
              background: "rgba(10, 14, 26, 0.4)",
              border: "1px solid rgba(255,255,255,0.05)",
              display: "flex", 
              flexDirection: "column", 
              gap: "0.75rem", 
              maxHeight: "calc(100vh - 280px)", 
              overflowY: "auto" 
            }}
          >
            {proposals.loading ? (
              <div className="loading-state">
                <div className="loading-spinner"></div>
                <span>Loading proposals...</span>
              </div>
            ) : proposals.error ? (
              <div className="alert-box critical">Failed to fetch proposals: {proposals.error}</div>
            ) : !proposals.data?.proposals || proposals.data.proposals.length === 0 ? (
              <div style={{ padding: "2rem", textAlign: "center", color: "var(--text-muted)", fontSize: "0.8rem" }}>
                No {statusFilter.toLowerCase()} proposals found.
              </div>
            ) : (
              proposals.data.proposals.map((p) => {
                const isSelected = p.id === selectedId;
                return (
                  <div
                    key={p.id}
                    style={{
                      padding: "1rem",
                      cursor: "pointer",
                      borderRadius: "8px",
                      border: isSelected ? "2px solid #38bdf8" : "1px solid rgba(255,255,255,0.06)",
                      background: isSelected ? "rgba(56, 189, 248, 0.08)" : "rgba(255, 255, 255, 0.01)",
                      boxShadow: isSelected ? "0 0 10px rgba(56, 189, 248, 0.15)" : "none",
                      transition: "all 0.2s ease"
                    }}
                    onClick={() => {
                      setSelectedId(p.id);
                      setActionResult(null);
                    }}
                  >
                    <div style={{ fontSize: "0.95rem", fontWeight: 700, color: "#fff", marginBottom: "0.5rem" }}>
                      {p.table_name}
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.75rem", color: "var(--text-muted)" }}>
                      <div>Run ID: {p.run_id}</div>
                      <div>{formatProposedTime(p.proposed_at || p.timestamp)}</div>
                    </div>
                  </div>
                )
              })
            )}
          </div>
        </div>

        {/* Right Column: Detailed Proposal view & actions */}
        {selectedProposal ? (
          <div 
            className="glass-card animate-in" 
            style={{ 
              padding: "2rem", 
              background: "rgba(10, 14, 26, 0.4)",
              border: "1px solid rgba(255,255,255,0.05)",
              borderRadius: "12px"
            }}
          >
            <h2 style={{ fontSize: "1.5rem", fontWeight: 700, color: "#fff", marginBottom: "0.25rem" }}>
              {selectedProposal.table_name}
            </h2>
            <p style={{ fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "1.5rem" }}>
              Run ID: {selectedProposal.run_id}
            </p>

            {actionResult && (
              <div 
                className={`alert-box ${actionResult.success ? 'success' : 'critical'}`} 
                style={{ marginBottom: '1.25rem', padding: '0.75rem', borderRadius: '6px', fontSize: '0.85rem' }}
              >
                {actionResult.message}
              </div>
            )}

            <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
              {/* Drift details list in mockup style */}
              <div style={{ display: "flex", flexDirection: "column", gap: "0.6rem" }}>
                {getModificationLines(selectedProposal).map((line, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "center", gap: "0.5rem", fontSize: "0.85rem" }}>
                    <span style={{ color: line.badgeColor }}>•</span>
                    <span style={{ color: "#fff" }}>{line.typeLabel}:</span>
                    <span 
                      style={{ 
                        fontSize: "0.68rem", 
                        fontWeight: 700,
                        padding: "0.15rem 0.5rem", 
                        borderRadius: "12px", 
                        backgroundColor: line.badgeColor,
                        color: "#040815"
                      }}
                    >
                      {line.badgeLabel}
                    </span>
                    <span style={{ fontFamily: "var(--font-mono)", color: "var(--text-muted)" }}>
                      {line.detailText}
                    </span>
                  </div>
                ))}
              </div>

              {/* JSON code viewer with line numbers */}
              <div>
                <h4 style={{ fontSize: "0.8rem", fontWeight: 500, color: "var(--text-muted)", marginBottom: "0.5rem" }}>
                  JSON code viewer
                </h4>
                {(() => {
                  const jsonString = JSON.stringify(selectedProposal.proposed_schema || {}, null, 2);
                  const lines = jsonString.split("\n");
                  return (
                    <div 
                      style={{
                        background: "rgba(0, 0, 0, 0.4)",
                        borderRadius: "8px",
                        border: "1px solid rgba(255,255,255,0.06)",
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.78rem",
                        maxHeight: "none",
                        overflowY: "auto",
                        display: "flex",
                        padding: "0.75rem 0"
                      }}
                    >
                      <div 
                        style={{
                          color: "#4b5563",
                          textAlign: "right",
                          padding: "0 0.75rem",
                          borderRight: "1px solid rgba(255,255,255,0.08)",
                          userSelect: "none",
                          minWidth: "2rem"
                        }}
                      >
                        {lines.map((_, i) => <div key={i}>{i + 1}</div>)}
                      </div>
                      <div 
                        style={{
                          paddingLeft: "1rem",
                          color: "#38bdf8",
                          whiteSpace: "pre"
                        }}
                      >
                        {lines.map((line, i) => <div key={i}>{line}</div>)}
                      </div>
                    </div>
                  );
                })()}
              </div>

              {/* Approval Override inputs (only for PENDING) */}
              {statusFilter === "PENDING" && (
                <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
                  <div>
                    <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                      Primary Key definition
                    </label>
                    <input
                      type="text"
                      placeholder="e.g. users"
                      value={primaryKeyOverride}
                      onChange={(e) => setPrimaryKeyOverride(e.target.value)}
                      style={{
                        width: "100%",
                        padding: "0.6rem",
                        borderRadius: "6px",
                        border: "1px solid rgba(255,255,255,0.08)",
                        background: "rgba(0, 0, 0, 0.2)",
                        color: "#fff",
                        fontSize: "0.85rem"
                      }}
                    />
                  </div>
                  <div>
                    <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                      Date Column definition
                    </label>
                    <input
                      type="text"
                      placeholder="e.g. Date Column"
                      value={dateColumnOverride}
                      onChange={(e) => setDateColumnOverride(e.target.value)}
                      style={{
                        width: "100%",
                        padding: "0.6rem",
                        borderRadius: "6px",
                        border: "1px solid rgba(255,255,255,0.08)",
                        background: "rgba(0, 0, 0, 0.2)",
                        color: "#fff",
                        fontSize: "0.85rem"
                      }}
                    />
                  </div>
                </div>
              )}

              {/* Interactive buttons right aligned */}
              {statusFilter === "PENDING" ? (
                <div style={{ display: "flex", justifyContent: "flex-end", gap: "0.75rem", marginTop: "1rem" }}>
                  <button
                    disabled={submitting}
                    style={{
                      padding: "0.55rem 1.5rem",
                      backgroundColor: "#10b981", // Emerald Green
                      color: "#fff",
                      border: "none",
                      borderRadius: "6px",
                      fontWeight: 600,
                      fontSize: "0.82rem",
                      cursor: "pointer",
                      transition: "all 0.2s ease"
                    }}
                    onClick={() => handleAction(selectedProposal.id, "approve")}
                  >
                    {submitting ? "Approving..." : "Approve Proposal"}
                  </button>
                  <button
                    disabled={submitting}
                    style={{
                      padding: "0.55rem 1.5rem",
                      backgroundColor: "#ef4444", // Red
                      color: "#fff",
                      border: "none",
                      borderRadius: "6px",
                      fontWeight: 600,
                      fontSize: "0.82rem",
                      cursor: "pointer",
                      transition: "all 0.2s ease"
                    }}
                    onClick={() => handleAction(selectedProposal.id, "reject")}
                  >
                    {submitting ? "Rejecting..." : "Reject Proposal"}
                  </button>
                </div>
              ) : (
                <div
                  style={{
                    padding: "0.75rem",
                    textAlign: "center",
                    borderRadius: "6px",
                    border: `1px solid ${statusFilter === "APPROVED" ? "var(--accent-green)" : "var(--accent-red)"}`,
                    color: statusFilter === "APPROVED" ? "var(--accent-green)" : "var(--accent-red)",
                    fontWeight: 700,
                    fontSize: "0.85rem",
                    marginTop: "1rem"
                  }}
                >
                  PROPOSAL {statusFilter} AT {new Date(selectedProposal.resolved_at).toLocaleString()}
                </div>
              )}
            </div>
          </div>
        ) : (
          <div 
            className="glass-card animate-in" 
            style={{ 
              display: "flex", 
              flexDirection: "column", 
              alignItems: "center", 
              justifyContent: "center", 
              padding: "4rem", 
              textAlign: "center", 
              minHeight: "450px", 
              background: "rgba(10, 14, 26, 0.4)",
              border: "1px solid rgba(255,255,255,0.05)",
              borderRadius: "12px"
            }}
          >
            <div className="card-icon blue" style={{ width: 60, height: 60, fontSize: "2rem", display: "flex", alignItems: "center", justifyContent: "center", marginBottom: "1rem" }}>📋</div>
            <h3 className="section-title">No Proposal Selected</h3>
            <p className="section-subtitle" style={{ maxWidth: "320px", margin: "0.5rem auto 0" }}>
              Select a schema proposal from the list on the left to inspect drift details, configure overrides, and take governance actions.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
