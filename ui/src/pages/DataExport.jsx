import React, { useState, useEffect } from "react";
import { useApi } from "../hooks/useApi";
import "./DataExport.css";

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
    <div className="gs-export">
      {/* 1. Page Header */}
      <div className="gs-page-header">
        <div>
          <h1 className="gs-page-title">Export <span>Data Hub</span></h1>
          <p className="gs-page-desc">Download clean datasets from Medallion HDFS and structured BI Elasticsearch indices</p>
        </div>
      </div>

      {/* 2. Export Mode Tabs */}
      <div className="gs-export-tabs" style={{ alignSelf: 'flex-start' }}>
        <button 
          className={`gs-export-btn ${activeTab === "datasets" ? "active" : ""}`}
          onClick={() => { setActiveTab("datasets"); setPreviewData(null); }}
        >
          Pipeline Datasets (HDFS)
        </button>
        <button 
          className={`gs-export-btn ${activeTab === "gold" ? "active" : ""}`}
          onClick={() => { setActiveTab("gold"); setPreviewData(null); }}
        >
          Gold BI Reports (Elasticsearch)
        </button>
      </div>

      {/* 3. Grid Workspace */}
      <div className="gs-export-layout">
        {/* Left Card: Ingestion/Export configuration */}
        <div className="gs-ecard">
          <h3>Export Configuration</h3>

          {activeTab === "datasets" ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
              <div className="gs-input-grp">
                <label>Target Data Layer</label>
                <select 
                  value={selectedLayer} 
                  onChange={(e) => {
                    setSelectedLayer(e.target.value);
                    if (e.target.value !== "reddit" && tables.length > 0 && !selectedTable) {
                      setSelectedTable(tables[0].name);
                    }
                  }}
                >
                  <option value="active">Active Layer (Silver/Clean)</option>
                  <option value="raw">Raw Layer (Bronze/Raw CSV)</option>
                  <option value="quarantine">Quarantine Zone (Bad Records)</option>
                  {redditAvailable && <option value="reddit">Reddit Streaming Dataset</option>}
                </select>
              </div>

              {selectedLayer === "reddit" ? (
                <div className="gs-input-grp">
                  <label>Streaming Subreddit</label>
                  <select value={subreddit} onChange={(e) => setSubreddit(e.target.value)}>
                    <option value="python">r/python</option>
                    <option value="bigdata">r/bigdata</option>
                    <option value="datascience">r/datascience</option>
                    <option value="machinelearning">r/machinelearning</option>
                    <option value="technology">r/technology</option>
                  </select>
                </div>
              ) : (
                <div className="gs-input-grp">
                  <label>Table Source</label>
                  {tablesLoading ? (
                    <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Loading catalog...</span>
                  ) : (
                    <select value={selectedTable} onChange={(e) => setSelectedTable(e.target.value)}>
                      {tables.map(t => (
                        <option key={t.name} value={t.name}>{t.name}</option>
                      ))}
                    </select>
                  )}
                </div>
              )}

              {selectedLayer !== "reddit" && selectedTable && supportedLayers.length > 0 && (
                <div className="gs-layer-status">
                  <strong>Available Medallion Layers</strong>
                  <div className="gs-layer-pills">
                    {["raw", "active", "quarantine"].map(l => {
                      const yes = supportedLayers.includes(l);
                      return (
                        <span key={l} className={`gs-layer-pill ${yes ? 'yes' : 'no'}`}>
                          {l.toUpperCase()}
                        </span>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
              <div className="gs-input-grp">
                <label>Gold Metric Index Type</label>
                <select value={goldMetric} onChange={(e) => setGoldMetric(e.target.value)}>
                  <option value="daily-quality">Daily Quality Summaries</option>
                  <option value="error-patterns">Common Error Patterns</option>
                  <option value="financial-impact">Financial Loss Estimates (COPDQ)</option>
                  <option value="schema-drift">Schema Drift History</option>
                </select>
              </div>

              <div className="gs-input-grp">
                <label>Range (Last N Days)</label>
                <input 
                  type="number" 
                  value={goldDays} 
                  onChange={(e) => setGoldDays(parseInt(e.target.value) || 7)}
                  min="1" 
                  max="365"
                />
              </div>
            </div>
          )}

          {exportStatus && (
            <div className={`gs-toast ${exportStatus.loading ? 'loading' : exportStatus.success ? 'ok' : 'err'}`} style={{ marginTop: '12px' }}>
              {exportStatus.message}
            </div>
          )}

          <button 
            onClick={handleDownload}
            disabled={exportStatus?.loading || (activeTab === "datasets" && !selectedTable && selectedLayer !== "reddit")}
            className="gs-btn-download"
            style={{ marginTop: 'auto' }}
          >
            {exportStatus?.loading ? "Generating export..." : "Export CSV File"}
          </button>
        </div>

        {/* Right Card: Preview Grid */}
        <div className="gs-ecard" style={{ overflow: "hidden" }}>
          <div className="gs-preview-header">
            <h3>Dataset Preview (First 10 Rows)</h3>
            {activeTab === "datasets" ? (
              <span>
                HDFS: <code>/data/{selectedLayer}/{selectedLayer === 'reddit' ? `subreddit=${subreddit}` : selectedTable}</code>
              </span>
            ) : (
              <span>
                Elasticsearch Index: <code>sdoqap_gold_{goldMetric.replace("-", "_")}</code>
              </span>
            )}
          </div>

          <div className="gs-preview-wrap">
            {previewLoading ? (
              <div className="gs-empty">Loading delta preview rows...</div>
            ) : previewError ? (
              <div className="gs-empty" style={{ color: 'var(--accent-red)' }}>
                <span>⚠️</span> {previewError}
              </div>
            ) : previewData && previewData.rows && previewData.rows.length > 0 ? (
              <table className="gs-preview-table">
                <thead>
                  <tr>
                    {previewData.columns.map(col => (
                      <th key={col}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {previewData.rows.map((row, idx) => (
                    <tr key={idx}>
                      {previewData.columns.map(col => (
                        <td key={col} title={String(row[col])}>
                          {row[col] !== null ? String(row[col]) : <em>null</em>}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div className="gs-empty">Select catalog table to preview delta rows</div>
            )}
          </div>
        </div>

      </div>
    </div>
  );
}
