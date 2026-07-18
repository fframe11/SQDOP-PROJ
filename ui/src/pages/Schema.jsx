import React, { useState } from "react";
import { useApi, postApi } from "../hooks/useApi";
import "./Schema.css";

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

  const formatProposedTime = (isoString) => {
    if (!isoString) return "";
    try {
      const date = new Date(isoString);
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      return `Detected at ${hours}:${minutes} GST`;
    } catch (e) {
      return "";
    }
  };

  const getModificationLines = (proposal) => {
    if (!proposal || !proposal.drift_details) return [];
    return Object.entries(proposal.drift_details).map(([col, details]) => {
      let typeLabel = "New column";
      let badgeLabel = "NEW COLUMN";
      let badgeColor = "var(--accent-green)";
      let detailText = col;

      if (details.error === "type_mismatch" || details.error === "type") {
        typeLabel = "Type column";
        badgeLabel = "TYPE MISMATCH";
        badgeColor = "var(--accent-yellow)";
        detailText = `${col} → ${details.actual || "type"}`;
      } else if (details.error === "missing_column") {
        typeLabel = "Missing column";
        badgeLabel = "MISSING COLUMN";
        badgeColor = "var(--accent-red)";
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
    <div className="gs-schema">
      {/* 1. Page Header */}
      <div className="gs-page-header">
        <div>
          <h1 className="gs-page-title">Schema <span>Drift Governance</span></h1>
          <p className="gs-page-desc">Approve or reject table structure auto-evolutions proposal queue</p>
        </div>
      </div>

      {actionResult && (
        <div 
          style={{ 
            padding: '10px 14px', 
            borderRadius: '8px', 
            fontSize: '12px', 
            background: actionResult.success ? '#d1fae5' : '#fee2e2',
            border: `1px solid ${actionResult.success ? '#10b981' : '#ef4444'}`,
            color: actionResult.success ? '#059669' : '#dc2626',
            fontFamily: 'var(--font-mono)'
          }}
        >
          {actionResult.message}
        </div>
      )}

      {/* 2. Navigation Filter Tabs */}
      <div className="gs-filter-tabs" style={{ alignSelf: 'flex-start' }}>
        {["PENDING", "APPROVED", "REJECTED"].map((tab) => (
          <button
            key={tab}
            className={`gs-filter-btn ${statusFilter === tab ? "active" : ""}`}
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

      {/* 3. Main Workspace */}
      <div className="gs-schema-layout">
        {/* Left Column: Proposals List */}
        <div className="gs-schema-list">
          <div className="gs-scard">
            <h3>{statusFilter} Proposals</h3>
            <div className="gs-proposals">
              {proposals.loading ? (
                <div className="gs-empty">Loading proposals...</div>
              ) : proposals.error ? (
                <div className="gs-empty" style={{ color: 'var(--accent-red)' }}>Failed to load proposals</div>
              ) : !proposals.data?.proposals || proposals.data.proposals.length === 0 ? (
                <div className="gs-empty">No {statusFilter.toLowerCase()} proposals found</div>
              ) : (
                proposals.data.proposals.map((p) => {
                  const isSelected = p.id === selectedId;
                  return (
                    <div
                      key={p.id}
                      className={`gs-proposal-item ${isSelected ? "selected" : ""}`}
                      onClick={() => {
                        setSelectedId(p.id);
                        setActionResult(null);
                      }}
                    >
                      <div className="gs-proposal-header">
                        <span className="gs-table-tag">{p.table_name}</span>
                        <span className="gs-severity-badge" style={{ borderColor: 'var(--accent-purple)', color: 'var(--accent-purple)' }}>
                          SEV {p.severity_score || 1}
                        </span>
                      </div>
                      <div className="gs-proposal-meta">
                        <span>Run: {(p.run_id || '').slice(0, 10)}</span>
                        <span>{formatProposedTime(p.proposed_at || p.timestamp)}</span>
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        </div>

        {/* Right Column: Detailed Proposal view */}
        <div className="gs-schema-workspace">
          {selectedProposal ? (
            <div className="gs-scard gs-workspace-card">
              <div className="gs-workspace-header">
                <div>
                  <h2>Table Evolution Workspace: {selectedProposal.table_name}</h2>
                  <span>Run UUID: {selectedProposal.run_id}</span>
                </div>
                {statusFilter === "PENDING" && (
                  <div className="gs-actions">
                    <button
                      disabled={submitting}
                      className="gs-btn-approve"
                      onClick={() => handleAction(selectedProposal.id, "approve")}
                    >
                      {submitting ? "Processing..." : "Approve Evolution"}
                    </button>
                    <button
                      disabled={submitting}
                      className="gs-btn-reject"
                      onClick={() => handleAction(selectedProposal.id, "reject")}
                    >
                      {submitting ? "Rejecting..." : "Reject & Quarantine"}
                    </button>
                  </div>
                )}
              </div>

              <div className="gs-drift-details-section">
                <h4 style={{ fontSize: '12px', fontWeight: 800, marginBottom: '8px', textTransform: 'uppercase', color: 'var(--text-muted)' }}>Detected Drift Mutations</h4>
                <div className="gs-drift-lines">
                  {getModificationLines(selectedProposal).map((line, i) => (
                    <div key={i} className="gs-drift-line">
                      <span className="gs-drift-badge" style={{ backgroundColor: line.badgeColor }}>{line.badgeLabel}</span>
                      <div className="gs-drift-info">
                        <strong>{line.typeLabel}</strong>
                        <p>{line.detailText}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* JSON code viewer with line numbers */}
              <div style={{ marginBottom: '16px' }}>
                <h4 style={{ fontSize: "11px", fontWeight: 700, color: "var(--text-muted)", marginBottom: "6px", textTransform: "uppercase" }}>
                  Proposed Delta Schema JSON
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
                        fontSize: "11px",
                        display: "flex",
                        padding: "8px 0",
                        maxHeight: "220px",
                        overflowY: "auto"
                      }}
                    >
                      <div 
                        style={{
                          color: "#475569",
                          textAlign: "right",
                          padding: "0 10px",
                          borderRight: "1px solid #1e293b",
                          userSelect: "none",
                          minWidth: "2rem"
                        }}
                      >
                        {lines.map((_, i) => <div key={i}>{i + 1}</div>)}
                      </div>
                      <div style={{ paddingLeft: "12px", whiteSpace: "pre", width: "100%", color: '#cbd5e1' }}>
                        {lines.map((line, i) => {
                          const trimmed = line.trim();
                          let highlightedColor = '#cbd5e1';
                          if (trimmed === "{" || trimmed === "}" || trimmed === "}," || trimmed === "[" || trimmed === "]") {
                            highlightedColor = '#64748b';
                          } else if (trimmed.includes(":")) {
                            if (trimmed.includes("type") || trimmed.includes("Type")) highlightedColor = '#f59e0b';
                            else highlightedColor = '#34d399';
                          }
                          return <div key={i} style={{ color: highlightedColor }}>{line}</div>;
                        })}
                      </div>
                    </div>
                  );
                })()}
              </div>

              {/* Approval Override inputs */}
              {statusFilter === "PENDING" && (
                <div className="gs-governance-config">
                  <h4 style={{ fontSize: '11.5px', fontWeight: 800, textTransform: 'uppercase', color: 'var(--text-main)' }}>Approval Override Parameters</h4>
                  <p className="gs-sub-desc">Define delta table constraints and active partition columns</p>
                  
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                    <div className="gs-input-grp">
                      <label style={{ fontSize: '10px', fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Primary Key Column</label>
                      <input
                        type="text"
                        placeholder="e.g. user_id"
                        value={primaryKeyOverride}
                        onChange={(e) => setPrimaryKeyOverride(e.target.value)}
                      />
                    </div>
                    <div className="gs-input-grp">
                      <label style={{ fontSize: '10px', fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Partition Date Column</label>
                      <input
                        type="text"
                        placeholder="e.g. created_date"
                        value={dateColumnOverride}
                        onChange={(e) => setDateColumnOverride(e.target.value)}
                      />
                    </div>
                  </div>
                </div>
              )}

              {statusFilter !== "PENDING" && (
                <div 
                  style={{
                    padding: "12px",
                    textAlign: "center",
                    borderRadius: "8px",
                    border: `1.5px solid ${statusFilter === "APPROVED" ? "var(--accent-green)" : "var(--accent-red)"}`,
                    color: statusFilter === "APPROVED" ? "var(--accent-green)" : "var(--accent-red)",
                    fontWeight: 700,
                    fontSize: "12px",
                    fontFamily: 'var(--font-mono)',
                    textTransform: 'uppercase',
                    marginTop: "auto"
                  }}
                >
                  PROPOSAL {statusFilter} AT {new Date(selectedProposal.resolved_at || selectedProposal.timestamp).toLocaleString()}
                </div>
              )}
            </div>
          ) : (
            <div className="gs-workspace-placeholder">
              <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" style={{ color: 'var(--accent-purple)', marginBottom: '12px' }}>
                <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z" />
                <path d="M2 10h20" />
              </svg>
              <p>Select a schema proposal from the left pane to analyze structural drift mutations and configure overrides.</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
