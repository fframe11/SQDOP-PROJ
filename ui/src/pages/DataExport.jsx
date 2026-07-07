import React, { useState, useEffect } from "react";
import { useApi } from "../hooks/useApi";

export default function DataExport() {
  const [activeTab, setActiveTab] = useState("datasets"); // "datasets" or "gold"
  
  // Datasets State
  const [tables, setTables] = useState([]);
  const [tablesLoading, setTablesLoading] = useState(true);
  const [selectedTable, setSelectedTable] = useState("");
  const [selectedLayer, setSelectedLayer] = useState("active");
  const [subreddit, setSubreddit] = useState("python");
  const [redditAvailable, setRedditAvailable] = useState(false);
  
  // Gold BI Reports State
  const [goldMetric, setGoldMetric] = useState("daily-quality");
  const [goldDays, setGoldDays] = useState(14);
  
  // Preview & Export Status
  const [previewData, setPreviewData] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState(null);
  const [exportStatus, setExportStatus] = useState(null);

  // Load available tables
  const fetchTables = async () => {
    setTablesLoading(true);
    try {
      const res = await fetch("/api/v1/export/tables");
      if (res.ok) {
        const data = await res.json();
        setTables(data.tables || []);
        setRedditAvailable(data.reddit_available || false);
        if (data.tables && data.tables.length > 0) {
          setSelectedTable(data.tables[0].name);
        }
      }
    } catch (e) {
      console.error("Failed to load tables", e);
    } finally {
      setTablesLoading(false);
    }
  };

  useEffect(() => {
    fetchTables();
  }, []);

  // Fetch Preview Data when selections change
  useEffect(() => {
    const fetchPreview = async () => {
      let url = "";
      const isGold = activeTab === "gold";

      if (isGold) {
        url = `/api/v1/gold/${goldMetric}?days=${goldDays}`;
      } else {
        if (selectedLayer === "reddit") {
          url = `/api/v1/export/preview/reddit/${subreddit}`;
        } else {
          if (!selectedTable) return;
          url = `/api/v1/export/preview/${selectedLayer}/${selectedTable}`;
        }
      }

      setPreviewLoading(true);
      setPreviewError(null);
      setPreviewData(null);
      try {
        const res = await fetch(url);
        if (res.ok) {
          const data = await res.json();
          if (isGold) {
            let list = [];
            if (Array.isArray(data)) {
              list = data;
            } else if (data.data && Array.isArray(data.data)) {
              list = data.data;
            } else if (data.daily && Array.isArray(data.daily)) {
              list = data.daily;
            }
            
            if (list.length === 0) {
              setPreviewError("No gold metrics found for the selected date range.");
            } else {
              setPreviewData({
                columns: Object.keys(list[0] || {}),
                rows: list.slice(0, 10)
              });
            }
          } else {
            setPreviewData(data);
          }
        } else {
          const errData = await res.json();
          setPreviewError(errData.detail || "No data available in this layer.");
        }
      } catch (err) {
        setPreviewError("Failed to fetch preview data.");
      } finally {
        setPreviewLoading(false);
      }
    };

    fetchPreview();
  }, [selectedTable, selectedLayer, subreddit, goldMetric, goldDays, activeTab]);

  // Handle CSV Download
  const handleDownload = async () => {
    let downloadUrl = "";
    let filename = "";

    if (activeTab === "datasets") {
      if (selectedLayer === "reddit") {
        downloadUrl = `/api/v1/export/reddit?subreddit=${subreddit}`;
        filename = `reddit_${subreddit}.csv`;
      } else {
        if (!selectedTable) return;
        downloadUrl = `/api/v1/export/${selectedLayer}/${selectedTable}`;
        filename = `${selectedTable}_${selectedLayer}.csv`;
      }
    } else {
      downloadUrl = `/api/v1/export/gold/${goldMetric}?days=${goldDays}`;
      filename = `gold_${goldMetric}_${goldDays}d.csv`;
    }

    setExportStatus({ loading: true, message: "Generating CSV export from HDFS raw storage..." });
    try {
      const response = await fetch(downloadUrl);
      if (!response.ok) {
        const err = await response.json();
        throw new Error(err.detail || "Export failed.");
      }
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
      
      setExportStatus({ success: true, message: `Successfully downloaded ${filename}!` });
      setTimeout(() => setExportStatus(null), 5000);
    } catch (err) {
      setExportStatus({ success: false, message: err.message || "Failed to download data." });
    }
  };

  // Find layer support for currently selected table
  const currentTableConfig = tables.find(t => t.name === selectedTable);
  const supportedLayers = currentTableConfig ? currentTableConfig.layers : [];

  return (
    <div className="sdoqap-app">
      <div className="sdoqap-header">
        <div className="logo-area">
          <h1>Data Export Center</h1>
          <p>Download clean, validated datasets directly from HDFS Medallion layers and Elasticsearch reports</p>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: "10px", marginBottom: "5px" }}>
        <button 
          className={`btn ${activeTab === "datasets" ? "btn-primary" : "btn-secondary"}`}
          onClick={() => { setActiveTab("datasets"); setPreviewData(null); }}
          style={{ padding: "8px 16px" }}
        >
          📋 Pipeline Datasets (HDFS)
        </button>
        <button 
          className={`btn ${activeTab === "gold" ? "btn-primary" : "btn-secondary"}`}
          onClick={() => { setActiveTab("gold"); setPreviewData(null); }}
          style={{ padding: "8px 16px" }}
        >
          📊 Gold BI Reports (Elasticsearch)
        </button>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "350px 1fr", gap: "15px", flex: 1, minHeight: 0 }}>
        {/* Left Panel: Control Panel */}
        <div className="glass-card" style={{ display: "flex", flexDirection: "column", gap: "15px", padding: "20px" }}>
          <h2 style={{ fontSize: "16px", fontWeight: "700", borderBottom: "1px solid rgba(255,255,255,0.06)", paddingBottom: "10px" }}>
            Export Configuration
          </h2>

          {activeTab === "datasets" ? (
            <>
              {/* Layer Selection */}
              <div className="form-group">
                <label>Data Layer</label>
                <select 
                  value={selectedLayer} 
                  onChange={(e) => {
                    setSelectedLayer(e.target.value);
                    if (e.target.value !== "reddit" && tables.length > 0 && !selectedTable) {
                      setSelectedTable(tables[0].name);
                    }
                  }}
                  style={{ width: "100%", background: "rgba(0,0,0,0.3)", color: "white", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "6px", padding: "8px" }}
                >
                  <option value="active">Active Layer (Silver/Clean)</option>
                  <option value="raw">Raw Layer (Bronze/Raw CSV)</option>
                  <option value="quarantine">Quarantine Zone (Bad Records)</option>
                  {redditAvailable && <option value="reddit">Reddit Streaming Dataset</option>}
                </select>
              </div>

              {/* Table / Subreddit Selection */}
              {selectedLayer === "reddit" ? (
                <div className="form-group">
                  <label>Subreddit Feed</label>
                  <select 
                    value={subreddit} 
                    onChange={(e) => setSubreddit(e.target.value)}
                    style={{ width: "100%", background: "rgba(0,0,0,0.3)", color: "white", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "6px", padding: "8px" }}
                  >
                    <option value="python">r/python</option>
                    <option value="bigdata">r/bigdata</option>
                    <option value="datascience">r/datascience</option>
                    <option value="machinelearning">r/machinelearning</option>
                    <option value="technology">r/technology</option>
                  </select>
                </div>
              ) : (
                <div className="form-group">
                  <label>Table Source</label>
                  {tablesLoading ? (
                    <div style={{ color: "var(--text-muted)", fontSize: "12px" }}>Loading tables...</div>
                  ) : (
                    <select 
                      value={selectedTable} 
                      onChange={(e) => setSelectedTable(e.target.value)}
                      style={{ width: "100%", background: "rgba(0,0,0,0.3)", color: "white", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "6px", padding: "8px" }}
                    >
                      {tables.map(t => (
                        <option key={t.name} value={t.name}>{t.name}</option>
                      ))}
                    </select>
                  )}
                </div>
              )}

              {/* Layer compatibility message */}
              {selectedLayer !== "reddit" && selectedTable && supportedLayers.length > 0 && (
                <div style={{ fontSize: "11px", color: "var(--text-muted)", background: "rgba(255,255,255,0.02)", padding: "8px", borderRadius: "6px" }}>
                  <strong>Available layers for this table:</strong>
                  <div style={{ display: "flex", gap: "6px", marginTop: "4px" }}>
                    {["raw", "active", "quarantine"].map(l => (
                      <span 
                        key={l}
                        style={{ 
                          padding: "2px 6px", 
                          borderRadius: "4px", 
                          background: supportedLayers.includes(l) ? "rgba(16,185,129,0.1)" : "rgba(244,63,94,0.1)",
                          color: supportedLayers.includes(l) ? "var(--accent-green)" : "var(--accent-red)",
                          fontWeight: "600"
                        }}
                      >
                        {l.toUpperCase()}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </>
          ) : (
            <>
              {/* Gold Metrics Config */}
              <div className="form-group">
                <label>Gold Metric Type</label>
                <select 
                  value={goldMetric} 
                  onChange={(e) => setGoldMetric(e.target.value)}
                  style={{ width: "100%", background: "rgba(0,0,0,0.3)", color: "white", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "6px", padding: "8px" }}
                >
                  <option value="daily-quality">Daily Quality Summaries</option>
                  <option value="error-patterns">Common Error Patterns</option>
                  <option value="financial-impact">Financial Loss Estimates (COPDQ)</option>
                  <option value="schema-drift">Schema Drift History</option>
                </select>
              </div>

              <div className="form-group">
                <label>Date Range (Last N days)</label>
                <input 
                  type="number" 
                  value={goldDays} 
                  onChange={(e) => setGoldDays(parseInt(e.target.value) || 7)}
                  min="1" 
                  max="365"
                  style={{ width: "100%", background: "rgba(0,0,0,0.3)", color: "white", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "6px", padding: "8px" }}
                />
              </div>
            </>
          )}

          {/* Export Action Button */}
          <button 
            className="btn btn-primary"
            onClick={handleDownload}
            disabled={exportStatus?.loading || (activeTab === "datasets" && !selectedTable && selectedLayer !== "reddit")}
            style={{ width: "100%", padding: "12px", marginTop: "auto", display: "flex", justifyContent: "center", alignItems: "center", gap: "8px", fontWeight: "700" }}
          >
            {exportStatus?.loading ? (
              <span>Preparing Export File...</span>
            ) : (
              <>
                <span>📥 Export CSV Attachment</span>
              </>
            )}
          </button>

          {/* Feedback alerts */}
          {exportStatus && (
            <div 
              style={{ 
                padding: "10px", 
                borderRadius: "6px", 
                fontSize: "12px",
                background: exportStatus.loading ? "rgba(56,189,248,0.1)" : exportStatus.success ? "rgba(16,185,129,0.1)" : "rgba(244,63,94,0.1)",
                color: exportStatus.loading ? "var(--accent-blue)" : exportStatus.success ? "var(--accent-green)" : "var(--accent-red)",
                border: `1px solid ${exportStatus.loading ? "rgba(56,189,248,0.2)" : exportStatus.success ? "rgba(16,185,129,0.2)" : "rgba(244,63,94,0.2)"}`
              }}
            >
              {exportStatus.message}
            </div>
          )}
        </div>

        {/* Right Panel: Preview Grid */}
        <div className="glass-card" style={{ display: "flex", flexDirection: "column", padding: "20px", overflow: "hidden" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid rgba(255,255,255,0.06)", paddingBottom: "10px", marginBottom: "15px", flexShrink: 0 }}>
            <h2 style={{ fontSize: "16px", fontWeight: "700" }}>Dataset Preview (First 10 records)</h2>
            {activeTab === "datasets" && selectedLayer !== "reddit" && selectedTable && (
              <span style={{ fontSize: "11px", color: "var(--text-muted)" }}>
                Reading HDFS: <code style={{ color: "var(--accent-blue)" }}>/data/{selectedLayer}/{selectedTable}</code>
              </span>
            )}
            {activeTab === "datasets" && selectedLayer === "reddit" && (
              <span style={{ fontSize: "11px", color: "var(--text-muted)" }}>
                Reading HDFS: <code style={{ color: "var(--accent-blue)" }}>/data/reddit/parquet/subreddit={subreddit}</code>
              </span>
            )}
            {activeTab === "gold" && (
              <span style={{ fontSize: "11px", color: "var(--text-muted)" }}>
                Index: <code style={{ color: "var(--accent-blue)" }}>sdoqap_gold_{goldMetric.replace("-", "_")}</code>
              </span>
            )}
          </div>

          <div style={{ flex: 1, overflow: "auto", display: "flex", flexDirection: "column", minHeight: 0 }}>
            {previewLoading ? (
              <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "var(--text-muted)" }}>
                Loading preview data...
              </div>
            ) : previewError ? (
              <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "100%", color: "var(--accent-red)", textAlign: "center", padding: "20px" }}>
                <span style={{ fontSize: "28px", marginBottom: "10px" }}>⚠️</span>
                <strong>Preview unavailable</strong>
                <span style={{ fontSize: "12px", color: "var(--text-muted)", marginTop: "5px" }}>{previewError}</span>
              </div>
            ) : previewData && previewData.rows && previewData.rows.length > 0 ? (
              <div style={{ overflowX: "auto", width: "100%" }}>
                <table className="runs-table" style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px" }}>
                  <thead>
                    <tr style={{ background: "rgba(255,255,255,0.04)" }}>
                      {previewData.columns.map(col => (
                        <th 
                          key={col} 
                          style={{ padding: "8px 12px", textAlign: "left", color: "var(--accent-blue)", borderBottom: "2px solid rgba(255,255,255,0.1)", whiteSpace: "nowrap" }}
                        >
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {previewData.rows.map((row, idx) => (
                      <tr 
                        key={idx} 
                        style={{ 
                          borderBottom: "1px solid rgba(255,255,255,0.06)", 
                          background: idx % 2 === 0 ? "transparent" : "rgba(255,255,255,0.01)" 
                        }}
                      >
                        {previewData.columns.map(col => (
                          <td 
                            key={col} 
                            style={{ 
                              padding: "8px 12px", 
                              color: "var(--text-main)", 
                              whiteSpace: "nowrap", 
                              overflow: "hidden", 
                              textOverflow: "ellipsis", 
                              maxWidth: "200px" 
                            }}
                            title={String(row[col])}
                          >
                            {row[col] !== null ? String(row[col]) : <em style={{ color: "var(--text-muted)" }}>null</em>}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "var(--text-muted)" }}>
                Select a table and layer to load preview data.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
