import React, { useState, useEffect } from "react";
import { useApi, postApi } from "../hooks/useApi";
import Tooltip from "../components/Tooltip";
import ConfirmationModal from "../components/ConfirmationModal";

export default function RulesConfig() {
  const [activeTab, setActiveTab] = useState("tables"); // "tables" or "proposals" or "remediations"
  const [selectedTable, setSelectedTable] = useState("users");
  const [selectedProposalId, setSelectedProposalId] = useState(null);
  
  const [remediations, setRemediations] = useState([]);
  const [remediationsLoading, setRemediationsLoading] = useState(false);
  const [resolvingTicketId, setResolvingTicketId] = useState(null);
  const [remediationPage, setRemediationPage] = useState(1);
  const remediationPageSize = 8;

  useEffect(() => {
    setRemediationPage(1);
  }, [remediations]);

  // Modal configuration states
  const [modalOpen, setModalOpen] = useState(false);
  const [modalConfig, setModalConfig] = useState({ title: "", message: "", onConfirm: () => {} });

  const triggerConfirm = (title, message, onConfirm) => {
    setModalConfig({
      title,
      message,
      onConfirm: () => {
        onConfirm();
        setModalOpen(false);
      }
    });
    setModalOpen(true);
  };
  
  const [tables, setTables] = useState([]);
  const [tablesLoading, setTablesLoading] = useState(true);
  const [detailTab, setDetailTab] = useState("edit"); // "edit" or "profile"

  // Fetch Table Config
  const [tableRules, setTableRules] = useState(null);
  const [rulesLoading, setRulesLoading] = useState(false);
  const [rulesError, setRulesError] = useState(null);

  // Fetch Column Profiling
  const [profileData, setProfileData] = useState(null);
  const [profileLoading, setProfileLoading] = useState(false);

  // Proposals useApi
  const proposals = useApi("/rules/ai-proposals", { refreshInterval: 10000 });
  const [submitting, setSubmitting] = useState(false);
  const [actionResult, setActionResult] = useState(null);

  // Settings State Hooks
  const [groqApiKey, setGroqApiKey] = useState("");
  const [groqModel, setGroqModel] = useState("llama-3.3-70b-versatile");
  const [groqEnabled, setGroqEnabled] = useState(false);
  const [settingsLoading, setSettingsLoading] = useState(false);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [settingsMessage, setSettingsMessage] = useState(null);

  const fetchSettings = async () => {
    setSettingsLoading(true);
    try {
      const res = await fetch("/api/v1/system/settings");
      if (res.ok) {
        const data = await res.json();
        setGroqApiKey(data.groq_api_key_masked || "");
        setGroqModel(data.groq_model || "llama-3.3-70b-versatile");
        setGroqEnabled(data.groq_enabled || false);
      }
    } catch (err) {
      console.error("Failed to fetch system settings");
    } finally {
      setSettingsLoading(false);
    }
  };

  const fetchRemediations = async () => {
    setRemediationsLoading(true);
    try {
      const res = await fetch("/api/v1/system/remediations");
      if (res.ok) {
        const data = await res.json();
        setRemediations(data.tickets || []);
      }
    } catch (err) {
      console.error("Failed to fetch upstream remediations");
    } finally {
      setRemediationsLoading(false);
    }
  };

  const handleResolveRemediation = (ticketId) => {
    triggerConfirm(
      "Confirm Ticket Resolution",
      `Are you sure you want to mark remediation ticket '${ticketId}' as RESOLVED? Confirm only if the upstream data system team has resolved the root cause of this anomaly.`,
      async () => {
        setResolvingTicketId(ticketId);
        try {
          const res = await fetch(`/api/v1/system/remediations/${ticketId}/resolve`, {
            method: "POST"
          });
          if (res.ok) {
            fetchRemediations();
          }
        } catch (err) {
          console.error("Failed to resolve ticket", err);
        } finally {
          setResolvingTicketId(null);
        }
      }
    );
  };

  useEffect(() => {
    fetchSettings();
  }, []);

  useEffect(() => {
    if (activeTab === "remediations") {
      fetchRemediations();
    }
  }, [activeTab]);

  const handleSaveSettings = (e) => {
    e.preventDefault();
    triggerConfirm(
      "Confirm Global Settings Update",
      "Are you sure you want to update the global AI Advisor configuration? Toggling AI settings affects the fallback behavior of all running quality pipelines.",
      async () => {
        setSettingsSaving(true);
        setSettingsMessage(null);
        try {
          const res = await fetch("/api/v1/system/settings", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              groq_api_key: groqApiKey,
              groq_model: groqModel,
              groq_enabled: groqEnabled
            })
          });
          if (res.ok) {
            setSettingsMessage({ type: "success", text: "Settings saved successfully!" });
            fetchSettings();
          } else {
            const errData = await res.json();
            setSettingsMessage({ type: "critical", text: errData.detail || "Failed to save settings." });
          }
        } catch (err) {
          setSettingsMessage({ type: "critical", text: "Error connecting to settings API." });
        } finally {
          setSettingsSaving(false);
        }
      }
    );
  };

  // Load tables list
  const fetchTables = async () => {
    setTablesLoading(true);
    try {
      const res = await fetch("/api/v1/export/tables");
      if (res.ok) {
        const data = await res.json();
        const all = data.tables ? data.tables.map(t => t.name) : [];
        setTables(all.sort());
        if (all.length > 0 && selectedTable === "users") {
          setSelectedTable(all[0]);
        }
      }
    } catch (err) {
      console.error("Failed to load tables list.");
    } finally {
      setTablesLoading(false);
    }
  };

  useEffect(() => {
    fetchTables();
  }, []);

  // Fetch specific table rules
  const fetchTableRules = async (tbl) => {
    setRulesLoading(true);
    setRulesError(null);
    try {
      const res = await fetch(`/api/v1/rules/${tbl}`);
      if (res.ok) {
        const data = await res.json();
        setTableRules(data.effective_rules);
      } else {
        setRulesError("Failed to fetch rules config.");
      }
    } catch (err) {
      setRulesError("Error connecting to rules API.");
    } finally {
      setRulesLoading(false);
    }
  };

  // Fetch specific table profiles
  const fetchTableProfile = async (tbl) => {
    setProfileLoading(true);
    setProfileData(null);
    try {
      const res = await fetch(`/api/v1/rules/profiles/${tbl}`);
      if (res.ok) {
        const data = await res.json();
        setProfileData(data);
      }
    } catch (err) {
      console.error("Failed to fetch profiles.");
    } finally {
      setProfileLoading(false);
    }
  };

  useEffect(() => {
    if (selectedTable && activeTab === "tables") {
      fetchTableRules(selectedTable);
      fetchTableProfile(selectedTable);
    }
  }, [selectedTable, activeTab]);

  // Handle saving rules changes
  const handleSaveRules = (e) => {
    e.preventDefault();
    triggerConfirm(
      "Confirm Rules Update",
      `Are you sure you want to update the quality parameters for table '${selectedTable}'? This will immediately affect all incoming ingestion pipelines.`,
      async () => {
        setSubmitting(true);
        setActionResult(null);
        try {
          const res = await fetch(`/api/v1/rules/${selectedTable}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(tableRules)
          });
          if (res.ok) {
            setActionResult({ success: true, message: `Rules for '${selectedTable}' updated successfully.` });
            fetchTableRules(selectedTable);
          } else {
            const err = await res.json();
            setActionResult({ success: false, message: err.detail || "Failed to update rules." });
          }
        } catch (err) {
          setActionResult({ success: false, message: "Error updating rules config." });
        } finally {
          setSubmitting(false);
        }
      }
    );
  };

  // Handle proposals actions
  const handleProposalAction = (id, action) => {
    const actionLabel = action === "approve" ? "APPROVE" : "REJECT";
    triggerConfirm(
      `Confirm Proposal ${actionLabel}`,
      `Are you sure you want to ${action} this AI-suggested rule modification? Approving it will merge the suggestions directly into the active rules registry.`,
      async () => {
        setSubmitting(true);
        setActionResult(null);
        try {
          const res = await postApi(`/rules/ai-proposals/${id}/${action}`);
          setActionResult({ success: true, message: `AI Proposal ${action}d successfully. Config updated.` });
          setSelectedProposalId(null);
          proposals.refetch();
          if (selectedTable) {
            fetchTableRules(selectedTable);
          }
        } catch (err) {
          setActionResult({ success: false, message: `Failed to ${action} proposal: ${err.message}` });
        } finally {
          setSubmitting(false);
        }
      }
    );
  };

  // Deep update helper
  const updateNestedKey = (key, subkey, val) => {
    setTableRules(prev => {
      if (!prev) return prev;
      const copy = { ...prev };
      if (copy[key] && typeof copy[key] === "object") {
        copy[key] = { ...copy[key], [subkey]: val };
      } else {
        copy[key] = { [subkey]: val };
      }
      return copy;
    });
  };

  const proposalsList = proposals.data?.proposals || [];
  const selectedProposal = proposalsList.find(p => p.id === selectedProposalId || p.run_id === selectedProposalId || p._id === selectedProposalId);

  return (
    <div className="page-container" style={{ overflowY: "auto", height: "calc(100vh - 56px)", paddingBottom: "2rem" }}>
      <div style={{ marginBottom: "1.5rem", width: "100%" }}>
        <div style={{ fontSize: "11px", color: "var(--text-muted)", marginBottom: "0.4rem", fontFamily: "var(--font-sans)", display: "flex", gap: "6px", alignItems: "center" }}>
          <span>SDOQAP Data Engine</span>
          <span style={{ opacity: 0.5 }}>&gt;</span>
          <span style={{ color: "var(--text-main)", fontWeight: 500 }}>Rules Hub</span>
        </div>
        <h1 style={{ fontSize: "1.6rem", fontWeight: 700, color: "var(--text-main)", letterSpacing: "-0.02em", margin: 0 }}>Rules Hub</h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", margin: "4px 0 0 0" }}>Manage data quality thresholds, view profiling ranges, and review AI suggestions</p>
      </div>

      {actionResult && (
        <div className={`alert-box ${actionResult.success ? "info" : "critical"}`} style={{ marginBottom: "1rem" }}>
          <span>{actionResult.message}</span>
        </div>
      )}

      {/* Main Tabs */}
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1.5rem" }}>
        <button
          className={`btn ${activeTab === "tables" ? "btn-primary" : "btn-secondary"}`}
          style={{ 
            padding: "0.5rem 1.25rem", 
            borderRadius: "20px",
            background: activeTab === "tables" ? "var(--accent-indigo)" : "#FFFFFF",
            border: "1px solid #E2E8F0",
            color: activeTab === "tables" ? "#FFFFFF" : "var(--text-muted)",
            cursor: "pointer",
            fontWeight: 500
          }}
          onClick={() => {
            setActiveTab("tables");
            setActionResult(null);
          }}
        >
          Table Quality Rules
        </button>
        <button
          className={`btn ${activeTab === "proposals" ? "btn-primary" : "btn-secondary"}`}
          style={{ 
            padding: "0.5rem 1.25rem", 
            borderRadius: "20px",
            background: activeTab === "proposals" ? "var(--accent-indigo)" : "#FFFFFF",
            border: "1px solid #E2E8F0",
            color: activeTab === "proposals" ? "#FFFFFF" : "var(--text-muted)",
            cursor: "pointer",
            position: "relative",
            fontWeight: 500
          }}
          onClick={() => {
            setActiveTab("proposals");
            setActionResult(null);
          }}
        >
          AI Advisor Proposals
          {proposalsList.length > 0 && (
            <span style={{
              position: "absolute",
              top: "-5px",
              right: "-5px",
              background: "var(--accent-red)",
              color: "#fff",
              fontSize: "10px",
              fontWeight: "bold",
              borderRadius: "50%",
              width: "18px",
              height: "18px",
              display: "flex",
              alignItems: "center",
              justifyContent: "center"
            }}>
              {proposalsList.length}
            </span>
          )}
        </button>
        <button
          className={`btn ${activeTab === "settings" ? "btn-primary" : "btn-secondary"}`}
          style={{ 
            padding: "0.5rem 1.25rem", 
            borderRadius: "20px",
            background: activeTab === "settings" ? "var(--accent-indigo)" : "#FFFFFF",
            border: "1px solid #E2E8F0",
            color: activeTab === "settings" ? "#FFFFFF" : "var(--text-muted)",
            cursor: "pointer",
            fontWeight: 500
          }}
          onClick={() => {
            setActiveTab("settings");
            setActionResult(null);
          }}
        >
          Global AI Settings
        </button>
        <button
          className={`btn ${activeTab === "remediations" ? "btn-primary" : "btn-secondary"}`}
          style={{ 
            padding: "0.5rem 1.25rem", 
            borderRadius: "20px",
            background: activeTab === "remediations" ? "var(--accent-indigo)" : "#FFFFFF",
            border: "1px solid #E2E8F0",
            color: activeTab === "remediations" ? "#FFFFFF" : "var(--text-muted)",
            cursor: "pointer",
            position: "relative",
            fontWeight: 500
          }}
          onClick={() => {
            setActiveTab("remediations");
            setActionResult(null);
          }}
        >
          Upstream Governance
          {remediations.filter(t => t.status === "OPEN").length > 0 && (
            <span style={{
              position: "absolute",
              top: "-5px",
              right: "-5px",
              background: "var(--accent-red)",
              color: "#fff",
              fontSize: "10px",
              fontWeight: "bold",
              borderRadius: "50%",
              width: "18px",
              height: "18px",
              display: "flex",
              alignItems: "center",
              justifyContent: "center"
            }}>
              {remediations.filter(t => t.status === "OPEN").length}
            </span>
          )}
        </button>
      </div>

      {activeTab === "tables" && (
        <div style={{ display: "grid", gridTemplateColumns: "280px 1fr", gap: "1.5rem", alignItems: "start" }}>
          {/* Left Column: Tables List */}
          <div>
            <h3 style={{ fontSize: "1rem", fontWeight: 600, color: "var(--text-main)", marginBottom: "0.5rem" }}>Dataset Registry</h3>
            <div 
              className="glass-card" 
              style={{ 
                padding: "1rem", 
                background: "#FFFFFF",
                border: "1px solid #E2E8F0",
                display: "flex", 
                flexDirection: "column", 
                gap: "0.5rem", 
                maxHeight: "calc(100vh - 280px)", 
                overflowY: "auto" 
              }}
            >
              {tablesLoading ? (
                <div style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>Loading datasets...</div>
              ) : tables.map((tbl) => {
                const isSelected = tbl === selectedTable;
                return (
                  <div
                    key={tbl}
                    style={{
                      padding: "0.75rem 1rem",
                      cursor: "pointer",
                      borderRadius: "8px",
                      border: isSelected ? "1px solid var(--accent-indigo)" : "1px solid #E2E8F0",
                      background: isSelected ? "rgba(108, 71, 255, 0.08)" : "#FFFFFF",
                      color: isSelected ? "var(--accent-indigo)" : "var(--text-muted)",
                      fontWeight: isSelected ? 600 : 400,
                      transition: "all 0.2s ease"
                    }}
                    onClick={() => {
                      setSelectedTable(tbl);
                      setActionResult(null);
                    }}
                  >
                    {tbl}
                  </div>
                )
              })}
            </div>
          </div>

          {/* Right Column: Table Configuration & Profile */}
          <div className="glass-card" style={{ padding: "1.5rem", background: "#FFFFFF", border: "1px solid #E2E8F0" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid #F1F5F9", paddingBottom: "10px", marginBottom: "15px" }}>
              <h2 style={{ fontSize: "1.25rem", color: "var(--text-main)" }}>
                Governance Config: <span style={{ color: "var(--accent-indigo)" }}>{selectedTable}</span>
              </h2>
              <div style={{ display: "flex", gap: "0.25rem" }}>
                <button
                  className={`btn ${detailTab === "edit" ? "btn-primary" : "btn-secondary"}`}
                  style={{ padding: "0.3rem 0.8rem", fontSize: "0.75rem", borderRadius: "4px" }}
                  onClick={() => setDetailTab("edit")}
                >
                  Rules Editor
                </button>
                <button
                  className={`btn ${detailTab === "profile" ? "btn-primary" : "btn-secondary"}`}
                  style={{ padding: "0.3rem 0.8rem", fontSize: "0.75rem", borderRadius: "4px" }}
                  onClick={() => setDetailTab("profile")}
                >
                  Profiling Profiles
                </button>
              </div>
            </div>

            {rulesLoading ? (
              <div style={{ color: "var(--text-muted)", padding: "2rem", textAlign: "center" }}>Loading configuration...</div>
            ) : rulesError ? (
              <div className="alert-box critical">{rulesError}</div>
            ) : tableRules ? (
              detailTab === "edit" ? (
                <form onSubmit={handleSaveRules} style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
                  {/* Quality Score Threshold */}
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "1rem", background: "rgba(255,255,255,0.01)", padding: "1rem", borderRadius: "8px", border: "1px solid rgba(255,255,255,0.03)" }}>
                    <div>
                      <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                        Quality Score Check
                        <Tooltip text="Strict mode checks against a fixed threshold. Adaptive mode dynamically learns bounds from historical quality metrics." />
                      </label>
                      <select
                        value={typeof tableRules.quality_score_threshold === "object" ? tableRules.quality_score_threshold.mode : "strict"}
                        onChange={(e) => {
                          const val = e.target.value;
                          if (typeof tableRules.quality_score_threshold === "object") {
                            updateNestedKey("quality_score_threshold", "mode", val);
                          } else {
                            setTableRules(prev => ({
                              ...prev,
                              quality_score_threshold: { mode: val, base_value: prev.quality_score_threshold || 90.0, min_value: 70.0 }
                            }));
                          }
                        }}
                        style={{ width: "100%", padding: "0.5rem", borderRadius: "4px", border: "1px solid rgba(255,255,255,0.08)", background: "#0b0e1a", color: "#fff", fontSize: "0.85rem" }}
                      >
                        <option value="strict">Strict (Static)</option>
                        <option value="adaptive">Adaptive (Dynamic)</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                        Base Target Score (%)
                        <Tooltip text="The baseline target percentage of records that must pass quality verification for the pipeline to be healthy." />
                      </label>
                      <input
                        type="number"
                        step="0.1"
                        value={typeof tableRules.quality_score_threshold === "object" ? (tableRules.quality_score_threshold.base_value || 90.0) : tableRules.quality_score_threshold}
                        onChange={(e) => {
                          const val = parseFloat(e.target.value) || 0.0;
                          if (typeof tableRules.quality_score_threshold === "object") {
                            updateNestedKey("quality_score_threshold", "base_value", val);
                          } else {
                            setTableRules(prev => ({ ...prev, quality_score_threshold: val }));
                          }
                        }}
                        style={{ width: "100%", padding: "0.5rem", borderRadius: "4px", border: "1px solid rgba(255,255,255,0.08)", background: "#0b0e1a", color: "#fff", fontSize: "0.85rem" }}
                      />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                        Min Limit (%)
                        <Tooltip text="In adaptive mode, this represents the absolute floor below which quality thresholds are never allowed to fall." />
                      </label>
                      <input
                        type="number"
                        step="0.1"
                        disabled={typeof tableRules.quality_score_threshold !== "object" || tableRules.quality_score_threshold.mode !== "adaptive"}
                        value={typeof tableRules.quality_score_threshold === "object" ? (tableRules.quality_score_threshold.min_value || 70.0) : 70.0}
                        onChange={(e) => updateNestedKey("quality_score_threshold", "min_value", parseFloat(e.target.value) || 0.0)}
                        style={{ width: "100%", padding: "0.5rem", borderRadius: "4px", border: "1px solid rgba(255,255,255,0.08)", background: "#0b0e1a", color: "#fff", fontSize: "0.85rem" }}
                      />
                    </div>
                  </div>

                  {/* Freshness Threshold */}
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem", background: "rgba(255,255,255,0.01)", padding: "1rem", borderRadius: "8px", border: "1px solid rgba(255,255,255,0.03)" }}>
                    <div>
                      <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                        Freshness Check Mode
                        <Tooltip text="Checks how recently the data has been updated. Strict mode uses static hours; Adaptive mode learns typical delays from history." />
                      </label>
                      <select
                        value={typeof tableRules.freshness_threshold_hours === "object" ? tableRules.freshness_threshold_hours.mode : "strict"}
                        onChange={(e) => {
                          const val = e.target.value;
                          if (typeof tableRules.freshness_threshold_hours === "object") {
                            updateNestedKey("freshness_threshold_hours", "mode", val);
                          } else {
                            setTableRules(prev => ({
                              ...prev,
                              freshness_threshold_hours: { mode: val, base_value: prev.freshness_threshold_hours || 48 }
                            }));
                          }
                        }}
                        style={{ width: "100%", padding: "0.5rem", borderRadius: "4px", border: "1px solid rgba(255,255,255,0.08)", background: "#0b0e1a", color: "#fff", fontSize: "0.85rem" }}
                      >
                        <option value="strict">Strict (Static)</option>
                        <option value="adaptive">Adaptive (Dynamic)</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                        Max Delay Hours
                        <Tooltip text="The maximum allowed delay in hours between the latest record timestamp and execution time." />
                      </label>
                      <input
                        type="number"
                        value={
                          typeof tableRules.freshness_threshold_hours === "object"
                            ? tableRules.freshness_threshold_hours.base_value !== null
                              ? tableRules.freshness_threshold_hours.base_value
                              : ""
                            : tableRules.freshness_threshold_hours || ""
                        }
                        onChange={(e) => {
                          const val = e.target.value === "" ? null : parseInt(e.target.value);
                          if (typeof tableRules.freshness_threshold_hours === "object") {
                            updateNestedKey("freshness_threshold_hours", "base_value", val);
                          } else {
                            setTableRules(prev => ({ ...prev, freshness_threshold_hours: val }));
                          }
                        }}
                        placeholder="e.g. 48"
                        style={{ width: "100%", padding: "0.5rem", borderRadius: "4px", border: "1px solid rgba(255,255,255,0.08)", background: "#0b0e1a", color: "#fff", fontSize: "0.85rem" }}
                      />
                    </div>
                  </div>

                  {/* Null Checks & Outliers */}
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem", background: "rgba(255,255,255,0.01)", padding: "1rem", borderRadius: "8px", border: "1px solid rgba(255,255,255,0.03)" }}>
                    <div>
                      <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                        Null Check Profile Mode
                        <Tooltip text="Strict rejects any record containing a null value in non-nullable columns. Adaptive applies dynamic tolerance rates learned from historical stats." />
                      </label>
                      <select
                        value={tableRules.null_checks?.mode || "adaptive"}
                        onChange={(e) => updateNestedKey("null_checks", "mode", e.target.value)}
                        style={{ width: "100%", padding: "0.5rem", borderRadius: "4px", border: "1px solid rgba(255,255,255,0.08)", background: "#0b0e1a", color: "#fff", fontSize: "0.85rem" }}
                      >
                        <option value="strict">Strict ( Blanket Reject )</option>
                        <option value="adaptive">Adaptive ( Dynamic Tolerance )</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                        Outlier Range Mode (IQR)
                        <Tooltip text="Statistical method to isolate outlier values. Off disables it; Auto computes bounds based on the current run's IQR; Adaptive applies learned bounds." />
                      </label>
                      <select
                        value={tableRules.value_range?.mode || "auto"}
                        onChange={(e) => updateNestedKey("value_range", "mode", e.target.value)}
                        style={{ width: "100%", padding: "0.5rem", borderRadius: "4px", border: "1px solid rgba(255,255,255,0.08)", background: "#0b0e1a", color: "#fff", fontSize: "0.85rem" }}
                      >
                        <option value="off">Off (Disable Check)</option>
                        <option value="auto">Auto (Current Pass IQR)</option>
                        <option value="adaptive">Adaptive (Profile bounds)</option>
                      </select>
                    </div>
                  </div>

                  {/* AI Rule Advisor Settings */}
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem", background: "rgba(255,255,255,0.01)", padding: "1rem", borderRadius: "8px", border: "1px solid rgba(255,255,255,0.03)" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                      <input
                        type="checkbox"
                        id="ai_enabled"
                        checked={tableRules.ai_advisor?.enabled || false}
                        onChange={(e) => updateNestedKey("ai_advisor", "enabled", e.target.checked)}
                        style={{ width: "18px", height: "18px", cursor: "pointer" }}
                      />
                      <label htmlFor="ai_enabled" style={{ fontSize: "0.85rem", color: "#fff", cursor: "pointer" }}>
                        Enable AI Advisor
                        <Tooltip text="If checked, this table's anomalies will automatically trigger root-cause analysis via the Groq LLM." />
                      </label>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "0.4rem" }}>
                        AI Model Name
                      </label>
                      <input
                        type="text"
                        value={tableRules.ai_advisor?.model || "llama-3.3-70b-versatile"}
                        onChange={(e) => updateNestedKey("ai_advisor", "model", e.target.value)}
                        style={{ width: "100%", padding: "0.5rem", borderRadius: "4px", border: "1px solid rgba(255,255,255,0.08)", background: "#0b0e1a", color: "#fff", fontSize: "0.85rem" }}
                      />
                    </div>
                  </div>

                  <div style={{ display: "flex", justifyContent: "flex-end", marginTop: "1rem" }}>
                    <button
                      type="submit"
                      disabled={submitting}
                      className="btn btn-primary"
                      style={{ padding: "0.6rem 2rem", borderRadius: "6px", fontSize: "0.85rem" }}
                    >
                      {submitting ? "Saving..." : "Save Rule Overrides"}
                    </button>
                  </div>
                </form>
              ) : (
                /* Profiling Logs View */
                <div>
                  {profileLoading ? (
                    <div style={{ color: "var(--text-muted)", padding: "2rem", textAlign: "center" }}>Loading profiles...</div>
                  ) : profileData ? (
                    <div style={{ display: "flex", flexDirection: "column", gap: "1.5rem" }}>
                      {/* Dynamic Rules Log Metadata */}
                      <div style={{ fontSize: "0.8rem", color: "var(--text-muted)", background: "rgba(255,255,255,0.02)", padding: "0.75rem", borderRadius: "6px" }}>
                        <div>Last profiled run: <code style={{ color: "var(--accent-blue)" }}>{profileData.run_id}</code></div>
                        <div>Profiled at: {new Date(profileData.timestamp).toLocaleString()}</div>
                      </div>

                      {/* Null rates Profile */}
                      {profileData.null_profile && (
                        <div>
                          <h4 style={{ fontSize: "0.9rem", color: "#fff", marginBottom: "0.5rem" }}>Null Rate Profiles</h4>
                          <table className="runs-table" style={{ width: "100%", borderCollapse: "collapse", fontSize: "11px" }}>
                            <thead>
                              <tr style={{ background: "rgba(255,255,255,0.04)" }}>
                                <th style={{ padding: "6px 10px", textAlign: "left" }}>Column</th>
                                <th style={{ padding: "6px 10px", textAlign: "center" }}>Current Null Rate</th>
                                <th style={{ padding: "6px 10px", textAlign: "center" }}>Tolerance Limit</th>
                                <th style={{ padding: "6px 10px", textAlign: "center" }}>Required Column</th>
                              </tr>
                            </thead>
                            <tbody>
                              {Object.entries(profileData.null_profile).map(([col, data]) => (
                                <tr key={col} style={{ borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                                  <td style={{ padding: "6px 10px", color: "var(--accent-blue)", fontFamily: "monospace" }}>{col}</td>
                                  <td style={{ padding: "6px 10px", textAlign: "center" }}>{(data.null_rate * 100).toFixed(2)}%</td>
                                  <td style={{ padding: "6px 10px", textAlign: "center" }}>{(data.tolerance * 100).toFixed(1)}%</td>
                                  <td style={{ padding: "6px 10px", textAlign: "center" }}>
                                    <span style={{ color: data.is_required ? "var(--accent-red)" : "var(--accent-green)" }}>
                                      {data.is_required ? "YES" : "NO"}
                                    </span>
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}

                      {/* Value Range (IQR) Profiles */}
                      {profileData.value_ranges && Object.keys(profileData.value_ranges).length > 0 && (
                        <div>
                          <h4 style={{ fontSize: "0.9rem", color: "#fff", marginBottom: "0.5rem" }}>Numeric Outlier Ranges (IQR)</h4>
                          <table className="runs-table" style={{ width: "100%", borderCollapse: "collapse", fontSize: "11px" }}>
                            <thead>
                              <tr style={{ background: "rgba(255,255,255,0.04)" }}>
                                <th style={{ padding: "6px 10px", textAlign: "left" }}>Column</th>
                                <th style={{ padding: "6px 10px", textAlign: "center" }}>Q1 (25th)</th>
                                <th style={{ padding: "6px 10px", textAlign: "center" }}>Q3 (75th)</th>
                                <th style={{ padding: "6px 10px", textAlign: "center" }}>IQR Fences (Acceptable bounds)</th>
                              </tr>
                            </thead>
                            <tbody>
                              {Object.entries(profileData.value_ranges).map(([col, data]) => (
                                <tr key={col} style={{ borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                                  <td style={{ padding: "6px 10px", color: "var(--accent-blue)", fontFamily: "monospace" }}>{col}</td>
                                  <td style={{ padding: "6px 10px", textAlign: "center" }}>{data.q1?.toFixed(2) || "0.0"}</td>
                                  <td style={{ padding: "6px 10px", textAlign: "center" }}>{data.q3?.toFixed(2) || "0.0"}</td>
                                  <td style={{ padding: "6px 10px", textAlign: "center", color: "#fff" }}>
                                    <code>{data.lower_bound?.toFixed(1) || "-inf"}</code> to <code>{data.upper_bound?.toFixed(1) || "+inf"}</code>
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  ) : (
                    <div style={{ padding: "2rem", textAlign: "center", color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      No profiling runs logged for this table yet. Run a quality pipeline to profile columns.
                    </div>
                  )}
                </div>
              )
            ) : (
              <div style={{ color: "var(--text-muted)" }}>Select a table on the left.</div>
            )}
          </div>
        </div>
      )}

      {activeTab === "proposals" && (
        <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", gap: "1.5rem", alignItems: "start" }}>
          {/* Left Column: Proposals List */}
          <div>
            <h3 style={{ fontSize: "1rem", fontWeight: 600, color: "var(--text-main)", marginBottom: "0.5rem" }}>Rule Proposals</h3>
            <div 
              className="glass-card" 
              style={{ 
                padding: "1rem", 
                background: "#FFFFFF",
                border: "1px solid #E2E8F0",
                display: "flex", 
                flexDirection: "column", 
                gap: "0.5rem", 
                maxHeight: "calc(100vh - 280px)", 
                overflowY: "auto" 
              }}
            >
              {proposals.loading ? (
                <div style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>Loading proposals...</div>
              ) : proposalsList.length === 0 ? (
                <div style={{ padding: "2rem", textAlign: "center", color: "var(--text-muted)", fontSize: "0.85rem" }}>
                  No active rule proposals from AI Advisor.
                </div>
              ) : (
                proposalsList.map((p) => {
                  const isSelected = p.id === selectedProposalId || p.run_id === selectedProposalId || p._id === selectedProposalId;
                  return (
                    <div
                      key={p.id || p.run_id || p._id}
                      style={{
                        padding: "1rem",
                        cursor: "pointer",
                        borderRadius: "8px",
                        border: isSelected ? "1px solid var(--accent-indigo)" : "1px solid #E2E8F0",
                        background: isSelected ? "rgba(108, 71, 255, 0.08)" : "#FFFFFF",
                        transition: "all 0.2s ease"
                      }}
                      onClick={() => {
                        setSelectedProposalId(p.id || p.run_id);
                        setActionResult(null);
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "0.5rem" }}>
                        <span style={{ fontSize: "0.9rem", fontWeight: 700, color: "var(--text-main)" }}>{p.table_name}</span>
                        <span style={{
                          fontSize: "0.68rem",
                          fontWeight: "bold",
                          padding: "0.15rem 0.4rem",
                          borderRadius: "10px",
                          background: (p.analysis_result?.confidence ?? 0) > 0.8 ? "rgba(16,185,129,0.2)" : "rgba(245,158,11,0.2)",
                          color: (p.analysis_result?.confidence ?? 0) > 0.8 ? "var(--accent-green)" : "var(--accent-amber)"
                        }}>
                          {((p.analysis_result?.confidence ?? 0) * 100).toFixed(0)}% Conf
                        </span>
                      </div>
                      <div style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
                        <div>Run ID: {p.run_id}</div>
                        <div>Date: {new Date(p.proposed_at || p.timestamp).toLocaleString()}</div>
                      </div>
                    </div>
                  )
                })
              )}
            </div>
          </div>

          {/* Right Column: Proposal Detail View */}
          {selectedProposal ? (
            <div className="glass-card animate-in" style={{ padding: "2rem", background: "#FFFFFF", border: "1px solid #E2E8F0" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "start", marginBottom: "1rem" }}>
                <div>
                  <h2 style={{ fontSize: "1.5rem", color: "var(--text-main)" }}>{selectedProposal.table_name} Rule Proposal</h2>
                  <p style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>Run ID: {selectedProposal.run_id}</p>
                </div>
                <div style={{ textAlign: "right" }}>
                  <div style={{ fontSize: "1.25rem", fontWeight: "bold", color: "var(--accent-indigo)" }}>
                    {((selectedProposal.analysis_result?.confidence ?? 0) * 100).toFixed(0)}% Confidence
                  </div>
                  <div style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>Generated by Groq Advisor</div>
                </div>
              </div>

              <div style={{ display: "flex", flexDirection: "column", gap: "1.5rem", marginTop: "1rem" }}>
                {/* Root Cause Analysis */}
                <div style={{ background: "rgba(56,189,248,0.05)", border: "1px solid rgba(56,189,248,0.1)", padding: "1rem", borderRadius: "8px" }}>
                  <h4 style={{ fontSize: "0.9rem", color: "var(--accent-blue)", marginBottom: "0.4rem" }}>🔍 AI Root Cause Analysis</h4>
                  <p style={{ fontSize: "0.85rem", color: "#fff", lineHeight: "1.4" }}>{selectedProposal.analysis_result?.root_cause}</p>
                </div>

                {/* Explanation */}
                <div>
                  <h4 style={{ fontSize: "0.9rem", color: "#fff", marginBottom: "0.4rem" }}>Detailed Explanation</h4>
                  <p style={{ fontSize: "0.85rem", color: "var(--text-muted)", lineHeight: "1.4" }}>{selectedProposal.analysis_result?.explanation}</p>
                </div>

                {/* Recommended Rules list */}
                {selectedProposal.analysis_result?.suggested_rules && selectedProposal.analysis_result?.suggested_rules.length > 0 && (
                  <div>
                    <h4 style={{ fontSize: "0.9rem", color: "#fff", marginBottom: "0.5rem" }}>Suggested Rule Changes</h4>
                    <div style={{ display: "flex", flexDirection: "column", gap: "0.6rem" }}>
                      {selectedProposal.analysis_result.suggested_rules.map((rule, idx) => (
                        <div 
                          key={idx} 
                          style={{ 
                            background: "rgba(255,255,255,0.02)", 
                            border: "1px solid rgba(255,255,255,0.05)", 
                            padding: "0.85rem", 
                            borderRadius: "6px",
                            fontSize: "0.85rem"
                          }}
                        >
                          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "0.25rem" }}>
                            <span style={{ fontWeight: "bold", color: "var(--accent-blue)" }}>{rule.column || rule.rule_path}</span>
                            <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontStyle: "italic" }}>{rule.rule_type || rule.action}</span>
                          </div>
                          <div style={{ color: "#fff", marginBottom: "0.4rem" }}>
                            Parameters: <code>{JSON.stringify(rule.params || rule.value)}</code>
                          </div>
                          <div style={{ fontSize: "0.78rem", color: "var(--text-muted)" }}>
                            Reasoning: {rule.reason}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Recommended Quality Score threshold */}
                {selectedProposal.analysis_result?.recommended_threshold && (
                  <div style={{ display: "flex", alignItems: "center", gap: "10px", background: "rgba(16,185,129,0.05)", border: "1px solid rgba(16,185,129,0.1)", padding: "0.75rem 1rem", borderRadius: "6px" }}>
                    <span style={{ fontSize: "1.25rem" }}>🛡️</span>
                    <span style={{ fontSize: "0.85rem", color: "#fff" }}>
                      AI Recommended Quality Score Threshold: <strong>{selectedProposal.analysis_result.recommended_threshold.toFixed(1)}%</strong>
                    </span>
                  </div>
                )}

                {/* Action Buttons */}
                <div style={{ display: "flex", justifyContent: "flex-end", gap: "0.75rem", marginTop: "1rem" }}>
                  <button
                    disabled={submitting}
                    style={{
                      padding: "0.6rem 1.75rem",
                      backgroundColor: "#10b981",
                      color: "#fff",
                      border: "none",
                      borderRadius: "6px",
                      fontWeight: 600,
                      fontSize: "0.85rem",
                      cursor: "pointer",
                      transition: "all 0.2s ease"
                    }}
                    onClick={() => handleProposalAction(selectedProposal.id || selectedProposal.run_id, "approve")}
                  >
                    {submitting ? "Processing..." : "Approve & Merge Rules"}
                  </button>
                  <button
                    disabled={submitting}
                    style={{
                      padding: "0.6rem 1.75rem",
                      backgroundColor: "#ef4444",
                      color: "#fff",
                      border: "none",
                      borderRadius: "6px",
                      fontWeight: 600,
                      fontSize: "0.85rem",
                      cursor: "pointer",
                      transition: "all 0.2s ease"
                    }}
                    onClick={() => handleProposalAction(selectedProposal.id || selectedProposal.run_id, "reject")}
                  >
                    {submitting ? "Processing..." : "Reject Proposal"}
                  </button>
                </div>
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
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>
              </div>
              <h3 className="section-title">No AI Proposal Selected</h3>
              <p className="section-subtitle" style={{ maxWidth: "320px", margin: "0.5rem auto 0" }}>
                Select a proposal from the list on the left to inspect root cause analyses, suggested rules, and take actions.
              </p>
            </div>
          )}
        </div>
      )}

      {activeTab === "settings" && (
        <div style={{ maxWidth: "600px", margin: "2rem auto 0" }} className="glass-card animate-in">
          <div style={{ borderBottom: "1px solid #F1F5F9", paddingBottom: "15px", marginBottom: "20px" }}>
            <h2 style={{ fontSize: "1.25rem", color: "var(--text-main)", display: "flex", alignItems: "center", gap: "0.5rem" }}>
              Global AI Settings
            </h2>
            <p style={{ fontSize: "0.8rem", color: "var(--text-muted)", marginTop: "0.25rem" }}>
              Configure Groq API Credentials to enable AI-powered data quality profiling and automated rule generation.
            </p>
          </div>

          {settingsLoading ? (
            <div style={{ color: "var(--text-muted)", padding: "2rem", textAlign: "center" }}>Loading settings...</div>
          ) : (
            <form onSubmit={handleSaveSettings} style={{ display: "flex", flexDirection: "column", gap: "1.5rem" }}>
              {settingsMessage && (
                <div className={`alert-box ${settingsMessage.type}`}>
                  {settingsMessage.text}
                </div>
              )}

              <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                <label style={{ fontSize: "0.85rem", fontWeight: 600, color: "var(--text-main)" }}>
                  Groq API Key
                </label>
                <input
                  type="password"
                  placeholder="Enter Groq Key (starts with gsk_)"
                  value={groqApiKey}
                  onChange={(e) => setGroqApiKey(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "0.75rem",
                    borderRadius: "6px",
                    border: "1px solid #E2E8F0",
                    background: "#FFFFFF",
                    color: "var(--text-main)",
                    fontSize: "0.85rem"
                  }}
                />
                <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
                  Your API key is stored securely in Elasticsearch. Supports Groq API keys (starts with gsk_).
                </span>
              </div>

              <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                <label style={{ fontSize: "0.85rem", fontWeight: 600, color: "var(--text-main)" }}>
                  Groq Model Version
                </label>
                <select
                  value={groqModel}
                  onChange={(e) => setGroqModel(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "0.75rem",
                    borderRadius: "6px",
                    border: "1px solid #E2E8F0",
                    background: "#FFFFFF",
                    color: "var(--text-main)",
                    fontSize: "0.85rem"
                  }}
                >
                  <option value="llama-3.3-70b-versatile">Groq Llama 3.3 70B (Recommended - Powerful & Fast)</option>
                  <option value="llama-3.1-8b-instant">Groq Llama 3.1 8B (Super-fast & Lightweight)</option>
                </select>
              </div>

              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", background: "rgba(255,255,255,0.01)", padding: "1rem", borderRadius: "8px", border: "1px solid rgba(255,255,255,0.03)" }}>
                <input
                  type="checkbox"
                  id="groq_enabled"
                  checked={groqEnabled}
                  onChange={(e) => setGroqEnabled(e.target.checked)}
                  style={{ width: "18px", height: "18px", cursor: "pointer" }}
                />
                <div style={{ cursor: "pointer" }}>
                  <label htmlFor="groq_enabled" style={{ fontSize: "0.85rem", color: "#fff", fontWeight: 600, display: "block" }}>
                    Enable Groq AI Advisor
                  </label>
                  <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", display: "block", marginTop: "0.2rem" }}>
                    When disabled, the system automatically degrades gracefully to the zero-dependency Local Heuristic Advisor.
                  </span>
                </div>
              </div>

              <div style={{ display: "flex", justifyContent: "flex-end", marginTop: "1rem" }}>
                <button
                  type="submit"
                  disabled={settingsSaving}
                  className="btn btn-primary"
                  style={{ padding: "0.75rem 2.5rem", borderRadius: "6px", fontSize: "0.85rem", fontWeight: 600 }}
                >
                  {settingsSaving ? "Saving Settings..." : "Save Settings"}
                </button>
              </div>
            </form>
          )}
        </div>
      )}

      {activeTab === "remediations" && (() => {
        const openTickets = remediations.filter(t => t.status === "OPEN");
        const resolvedTickets = remediations.filter(t => t.status === "RESOLVED");
        const totalTicketsCount = remediations.length;
        const resolutionRate = totalTicketsCount ? Math.round((resolvedTickets.length / totalTicketsCount) * 100) : 100;
        
        const criticalCount = openTickets.filter(t => t.severity === "critical").length;
        const warningCount = openTickets.filter(t => t.severity === "warning").length;
        
        const systemCounts = {};
        openTickets.forEach(t => {
          systemCounts[t.target_system] = (systemCounts[t.target_system] || 0) + 1;
        });
        let mostUnstableSystem = "None";
        let maxSystemCount = 0;
        Object.entries(systemCounts).forEach(([sys, count]) => {
          if (count > maxSystemCount) {
            maxSystemCount = count;
            mostUnstableSystem = sys;
          }
        });
        if (mostUnstableSystem !== "None") {
          mostUnstableSystem = `${mostUnstableSystem} (${maxSystemCount} open)`;
        }

        const paginatedRemediations = remediations.slice((remediationPage - 1) * remediationPageSize, remediationPage * remediationPageSize);
        const totalPages = Math.ceil(remediations.length / remediationPageSize) || 1;

        return (
          <div className="glass-card" style={{ padding: "1.5rem" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "1.5rem" }}>
              <div>
                <h2 style={{ fontSize: "1.25rem", fontWeight: 600, color: "#fff", marginBottom: "0.25rem" }}>
                  Upstream Remediation Tickets
                </h2>
                <p style={{ fontSize: "0.85rem", color: "var(--text-muted)", margin: 0 }}>
                  Trace and manage root-cause fixes required at upstream source systems to resolve downstream data drift.
                </p>
              </div>
              <button 
                className="btn btn-secondary" 
                onClick={fetchRemediations} 
                disabled={remediationsLoading}
                style={{ padding: "0.5rem 1rem", fontSize: "0.8rem" }}
              >
                {remediationsLoading ? "Refreshing..." : "🔄 Refresh"}
              </button>
            </div>

            {/* Analytics Summary Scorecards */}
            {remediations.length > 0 && (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: "1rem", marginBottom: "2rem" }}>
                <div className="glass-card" style={{ padding: "1.25rem", background: "rgba(59, 130, 246, 0.05)", border: "1px solid rgba(59, 130, 246, 0.15)", borderRadius: "8px" }}>
                  <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", display: "block", marginBottom: "0.4rem", textTransform: "uppercase" }}>Open Tickets</span>
                  <span style={{ fontSize: "1.75rem", fontWeight: 700, color: "#fff", fontFamily: "monospace" }}>
                    {openTickets.length}
                  </span>
                  <span style={{ fontSize: "0.7rem", color: "var(--text-muted)", display: "block", marginTop: "0.2rem" }}>
                    {criticalCount} critical | {warningCount} warning
                  </span>
                </div>
                <div className="glass-card" style={{ padding: "1.25rem", background: "rgba(16, 185, 129, 0.05)", border: "1px solid rgba(16, 185, 129, 0.15)", borderRadius: "8px" }}>
                  <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", display: "block", marginBottom: "0.4rem", textTransform: "uppercase" }}>Resolution Rate</span>
                  <span style={{ fontSize: "1.75rem", fontWeight: 700, color: "#10b981", fontFamily: "monospace" }}>
                    {resolutionRate}%
                  </span>
                  <span style={{ fontSize: "0.7rem", color: "var(--text-muted)", display: "block", marginTop: "0.2rem" }}>
                    {resolvedTickets.length} of {totalTicketsCount} tickets closed
                  </span>
                </div>
                <div className="glass-card" style={{ padding: "1.25rem", background: "rgba(245, 158, 11, 0.05)", border: "1px solid rgba(245, 158, 11, 0.15)", borderRadius: "8px" }}>
                  <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", display: "block", marginBottom: "0.4rem", textTransform: "uppercase" }}>Most Unstable System</span>
                  <span style={{ fontSize: "0.85rem", fontWeight: 600, color: "#fff", display: "block", minHeight: "2.1rem", marginTop: "0.4rem", lineHeight: "1.3" }}>
                    {mostUnstableSystem}
                  </span>
                </div>
                <div className="glass-card" style={{ padding: "1.25rem", background: "rgba(239, 68, 68, 0.05)", border: "1px solid rgba(239, 68, 68, 0.15)", borderRadius: "8px" }}>
                  <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", display: "block", marginBottom: "0.4rem", textTransform: "uppercase" }}>Remediation Status</span>
                  <span style={{ fontSize: "1.75rem", fontWeight: 700, color: openTickets.length > 0 ? "#f59e0b" : "#10b981", fontFamily: "monospace" }}>
                    {openTickets.length > 0 ? "ACTION REQ" : "HEALTHY"}
                  </span>
                  <span style={{ fontSize: "0.7rem", color: "var(--text-muted)", display: "block", marginTop: "0.2rem" }}>
                    {openTickets.length > 0 ? "Fix upstream anomalies" : "All upstream sources compliant"}
                  </span>
                </div>
              </div>
            )}

            {remediationsLoading && remediations.length === 0 ? (
              <div style={{ padding: "3rem", textAlign: "center", color: "var(--text-muted)" }}>
                Loading remediation tickets...
              </div>
            ) : remediations.length === 0 ? (
              <div style={{ padding: "4rem 2rem", textAlign: "center", background: "rgba(255,255,255,0.01)", borderRadius: "8px", border: "1px dashed rgba(255,255,255,0.08)" }}>
                <span style={{ fontSize: "2rem", display: "block", marginBottom: "1rem" }}>🎉</span>
                <h4 style={{ color: "#fff", fontSize: "0.95rem", marginBottom: "0.25rem" }}>All Clear!</h4>
                <p style={{ color: "var(--text-muted)", fontSize: "0.8rem", margin: 0 }}>No active upstream remediation tickets found.</p>
              </div>
            ) : (
              <>
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", textAlign: "left", fontSize: "0.85rem" }}>
                    <thead>
                      <tr style={{ borderBottom: "1px solid rgba(255,255,255,0.08)", color: "var(--text-muted)" }}>
                        <th style={{ padding: "0.75rem 1rem", fontWeight: 500 }}>Ticket ID</th>
                        <th style={{ padding: "0.75rem 1rem", fontWeight: 500 }}>Table</th>
                        <th style={{ padding: "0.75rem 1rem", fontWeight: 500 }}>Severity</th>
                        <th style={{ padding: "0.75rem 1rem", fontWeight: 500 }}>Upstream Target</th>
                        <th style={{ padding: "0.75rem 1rem", fontWeight: 500 }}>Remediation Action Required</th>
                        <th style={{ padding: "0.75rem 1rem", fontWeight: 500 }}>Status</th>
                        <th style={{ padding: "0.75rem 1rem", fontWeight: 500, textAlign: "right" }}>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {paginatedRemediations.map((t) => (
                        <tr 
                          key={t.ticket_id} 
                          style={{ 
                            borderBottom: "1px solid rgba(255,255,255,0.04)",
                            background: t.status === "RESOLVED" ? "rgba(255,255,255,0.005)" : "transparent"
                          }}
                        >
                          <td style={{ padding: "1rem", color: "#fff", fontWeight: 600 }}>
                            <div style={{ fontSize: "0.8rem", fontFamily: "monospace" }}>{t.ticket_id}</div>
                            <div style={{ fontSize: "0.7rem", color: "var(--text-muted)", marginTop: "0.2rem" }}>
                              {new Date(t.timestamp).toLocaleString()}
                            </div>
                          </td>
                          <td style={{ padding: "1rem" }}>
                            <span className="badge badge-tbl" style={{ background: "rgba(59, 130, 246, 0.15)", color: "#60a5fa", padding: "0.2rem 0.6rem", borderRadius: "12px", fontSize: "0.75rem", fontWeight: 600 }}>
                              {t.table_name}
                            </span>
                          </td>
                          <td style={{ padding: "1rem" }}>
                            <span 
                              style={{ 
                                background: t.severity === "critical" ? "rgba(239, 68, 68, 0.15)" : "rgba(245, 158, 11, 0.15)", 
                                color: t.severity === "critical" ? "#ef4444" : "#f59e0b",
                                padding: "0.2rem 0.6rem", 
                                borderRadius: "12px", 
                                fontSize: "0.75rem", 
                                fontWeight: 600,
                                textTransform: "uppercase"
                              }}
                            >
                              {t.severity}
                            </span>
                          </td>
                          <td style={{ padding: "1rem", color: "#fff", fontWeight: 500 }}>
                            {t.target_system}
                          </td>
                          <td style={{ padding: "1rem", color: "var(--text-muted)", maxWidth: "350px", lineHeight: "1.3" }}>
                            {t.remediation_action}
                          </td>
                          <td style={{ padding: "1rem" }}>
                            <span 
                              style={{ 
                                background: t.status === "RESOLVED" ? "rgba(16, 185, 129, 0.15)" : "rgba(239, 68, 68, 0.15)", 
                                color: t.status === "RESOLVED" ? "#10b981" : "#ef4444",
                                padding: "0.2rem 0.6rem", 
                                borderRadius: "12px", 
                                fontSize: "0.75rem", 
                                fontWeight: 600
                              }}
                            >
                              {t.status}
                            </span>
                          </td>
                          <td style={{ padding: "1rem", textAlign: "right" }}>
                            {t.status === "OPEN" ? (
                              <button
                                className="btn btn-primary"
                                disabled={resolvingTicketId === t.ticket_id}
                                onClick={() => handleResolveRemediation(t.ticket_id)}
                                style={{ 
                                  padding: "0.4rem 0.8rem", 
                                  fontSize: "0.75rem", 
                                  borderRadius: "4px",
                                  background: "rgba(16, 185, 129, 0.85)",
                                  border: "none",
                                  color: "#fff",
                                  fontWeight: 600,
                                  cursor: "pointer"
                                }}
                              >
                                {resolvingTicketId === t.ticket_id ? "Resolving..." : "✓ Resolve"}
                              </button>
                            ) : (
                              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
                                Resolved
                              </span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: "1.5rem", padding: "0 0.5rem" }}>
                  <button 
                    className="btn btn-secondary" 
                    disabled={remediationPage === 1} 
                    onClick={() => setRemediationPage(p => Math.max(p - 1, 1))}
                    style={{ padding: "0.25rem 0.75rem", fontSize: "0.75rem", background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.08)", color: "#fff", cursor: "pointer", borderRadius: "4px" }}
                  >
                    Previous
                  </button>
                  <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
                    Page {remediationPage} of {totalPages}
                  </span>
                  <button 
                    className="btn btn-secondary" 
                    disabled={remediationPage >= totalPages} 
                    onClick={() => setRemediationPage(p => p + 1)}
                    style={{ padding: "0.25rem 0.75rem", fontSize: "0.75rem", background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.08)", color: "#fff", cursor: "pointer", borderRadius: "4px" }}
                  >
                    Next
                  </button>
                </div>
              </>
            )}
          </div>
        );
      })()}

      <ConfirmationModal 
        isOpen={modalOpen}
        title={modalConfig.title}
        message={modalConfig.message}
        onConfirm={modalConfig.onConfirm}
        onCancel={() => setModalOpen(false)}
      />
    </div>
  );
}
