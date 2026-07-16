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
      <div style={{ marginBottom: "1.5rem", width: "100%" }}>
        <div style={{ fontSize: "11px", color: "var(--text-muted)", marginBottom: "0.4rem", fontFamily: "var(--font-sans)", display: "flex", gap: "6px", alignItems: "center" }}>
          <span>SDOQAP Data Engine</span>
          <span style={{ opacity: 0.5 }}>&gt;</span>
          <span style={{ color: "var(--text-main)", fontWeight: 500 }}>Schema Drift</span>
        </div>
        <h1 style={{ fontSize: "1.6rem", fontWeight: 700, color: "var(--text-main)", letterSpacing: "-0.02em", margin: 0 }}>Schema Drift</h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", margin: "4px 0 0 0" }}>Review and approve/reject schema drift proposals detected by Spark engines</p>
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
              background: statusFilter === tab ? "var(--accent-indigo)" : "#FFFFFF",
              border: "1px solid #E2E8F0",
              color: statusFilter === tab ? "#FFFFFF" : "var(--text-muted)",
              cursor: "pointer",
              fontWeight: 500
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
          <h3 style={{ fontSize: "1rem", fontWeight: 600, color: "var(--text-main)", marginBottom: "0.25rem" }}>
            {statusFilter === "PENDING" ? "PENDING" : statusFilter === "APPROVED" ? "APPROVED" : "REJECTED"} Proposals
          </h3>
          <p style={{ fontSize: "0.78rem", color: "var(--text-muted)", marginBottom: "1rem" }}>
            Awaiting schema validation review
          </p>

          <div 
            className="glass-card" 
            style={{ 
              padding: "1rem", 
              background: "#FFFFFF",
              border: "1px solid #E2E8F0",
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
                      border: isSelected ? "2px solid var(--accent-indigo)" : "1px solid #E2E8F0",
                      background: isSelected ? "rgba(108, 71, 255, 0.08)" : "#FFFFFF",
                      boxShadow: isSelected ? "0 0 10px rgba(108, 71, 255, 0.15)" : "none",
                      transition: "all 0.2s ease"
                    }}
                    onClick={() => {
                      setSelectedId(p.id);
                      setActionResult(null);
                    }}
                  >
                    <div style={{ fontSize: "0.95rem", fontWeight: 700, color: "var(--text-main)", marginBottom: "0.5rem" }}>
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
              background: "#FFFFFF",
              border: "1px solid #E2E8F0",
              borderRadius: "12px"
            }}
          >
            <h2 style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--text-main)", marginBottom: "0.25rem" }}>
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
                    <span style={{ color: "var(--text-main)" }}>{line.typeLabel}:</span>
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
                        background: "#0f172a",
                        borderRadius: "8px",
                        border: "1px solid #1e293b",
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.78rem",
                        maxHeight: "none",
                        overflowY: "auto",
                        display: "flex",
                        padding: "0.75rem 0",
                        boxShadow: "inset 0 2px 4px 0 rgba(0,0,0,0.2)"
                      }}
                    >
                      <div 
                        style={{
                          color: "#475569",
                          textAlign: "right",
                          padding: "0 0.75rem",
                          borderRight: "1px solid #1e293b",
                          userSelect: "none",
                          minWidth: "2rem"
                        }}
                      >
                        {lines.map((_, i) => <div key={i}>{i + 1}</div>)}
                      </div>
                      <div 
                        style={{
                          paddingLeft: "1rem",
                          whiteSpace: "pre",
                          width: "100%"
                        }}
                      >
                        {lines.map((line, i) => {
                          // Simple regex highlighting for JSON
                          let highlighted = <span style={{ color: "#cbd5e1" }}>{line}</span>;
                          const trimmed = line.trim();
                          
                          if (trimmed === "{" || trimmed === "}" || trimmed === "}," || trimmed === "[" || trimmed === "]") {
                            highlighted = <span style={{ color: "#64748b" }}>{line}</span>;
                          } else {
                            const match = line.match(/^(\s*)(".*?")(\s*:\s*)(".*?"|\d+|true|false|null)(,?)(\s*)$/);
                            if (match) {
                              const [_, indent, key, colon, value, comma, trailing] = match;
                              const isTypeVal = value.includes("Type") || !value.startsWith('"');
                              const valColor = isTypeVal ? "#f59e0b" : "#38bdf8";
                              highlighted = (
                                <span>
                                  {indent}
                                  <span style={{ color: "#a5b4fc" }}>{key}</span>
                                  <span style={{ color: "#64748b" }}>{colon}</span>
                                  <span style={{ color: valColor }}>{value}</span>
                                  {comma && <span style={{ color: "#64748b" }}>{comma}</span>}
                                  {trailing}
                                </span>
                              );
                            }
                          }
                          return <div key={i}>{highlighted}</div>;
                        })}
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
              background: "#FFFFFF",
              border: "1px solid #E2E8F0",
              borderRadius: "12px"
            }}
          >
            <div className="card-icon blue" style={{ width: 60, height: 60, display: "flex", alignItems: "center", justifyContent: "center", marginBottom: "1rem" }}>
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
            </div>
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
