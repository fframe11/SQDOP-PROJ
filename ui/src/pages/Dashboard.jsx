import React, { useState, useMemo, useEffect, useRef } from 'react';
import { useApi, postApi } from '../hooks/useApi';
import { ComposedChart, AreaChart, Area, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Legend } from 'recharts';
import "./Dashboard.css";

const getIngestionSource = (run) => {
  if (!run) return "Unknown Ingest";
  const name = (run.table_name || "").toLowerCase();
  const rid = (run.run_id || "").toLowerCase();
  if (name.includes("reddit") || name.includes("stream") || rid.includes("stream")) {
    return "Reddit Real-time Stream";
  } else if (name.includes("products") || name.includes("api") || rid.includes("api")) {
    return "JSON API Fetcher";
  } else if (name.includes("sales") || name.includes("pg") || rid.includes("pg")) {
    return "RDBMS Database Ingestion";
  } else {
    return "Local CSV Dataset Ingest";
  }
};

const getQualityGrade = (score) => {
  if (score === null || score === undefined) return { grade: "N/A", color: "var(--text-muted)" };
  if (score === 100) return { grade: "A+ Excellent", color: "var(--accent-green)" };
  if (score >= 95) return { grade: "A Healthy", color: "var(--accent-green)" };
  if (score >= 90) return { grade: "B+ Warning", color: "var(--accent-yellow)" };
  if (score >= 85) return { grade: "B Caution", color: "var(--accent-yellow)" };
  return { grade: "F Critical Anomaly", color: "var(--accent-red)" };
};

export default function Dashboard() {
  // 1. Interactive States (Slicers & Filters)
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedSourceFilter, setSelectedSourceFilter] = useState('All');
  const [selectedRun, setSelectedRun] = useState(null);
  const [userSelectedRunId, setUserSelectedRunId] = useState(null);

  const [leftTab, setLeftTab] = useState('Ratio'); // Ratio, Quarantine, Insights
  const [centerTab, setCenterTab] = useState('Trends'); // Trends, RootCause, Impact, Actionable

  // Pagination states
  const [historyPage, setHistoryPage] = useState(1);
  const historyPageSize = 5;

  useEffect(() => {
    setHistoryPage(1);
  }, [searchTerm, selectedSourceFilter]);

  // 2. Fetch real-time metrics from API endpoints
  const kpi = useApi('/kpi/stats', { refreshInterval: 15000 });
  const anomaly = useApi('/anomaly/sources', { refreshInterval: 15000 });
  const services = useApi('/services/status', { refreshInterval: 10000 });
  const isHealthy = services.data && !services.error;
  const activity = useApi('/system/activity?limit=15', { refreshInterval: 15000 });
  const perf = useApi('/performance/metrics', { refreshInterval: 15000 });
  const qualityHistory = useApi('/quality?limit=50', { refreshInterval: 15000 });
  const projection = useApi(selectedSourceFilter === 'All' ? '/analytics/projection' : `/analytics/projection?table_name=${selectedSourceFilter}`, { refreshInterval: 30000 });
  const clustering = useApi('/analytics/clustering', { refreshInterval: 30000 });
  const impact = useApi('/analytics/impact', { refreshInterval: 30000 });
  const recommendations = useApi('/analytics/recommendations', { refreshInterval: 30000 });

  // Automatically select the first pipeline run if none or if tracking the latest
  useEffect(() => {
    if (qualityHistory.data && qualityHistory.data.length > 0) {
      if (!userSelectedRunId) {
        setSelectedRun(qualityHistory.data[0]);
      } else {
        const match = qualityHistory.data.find(r => r.run_id === userSelectedRunId);
        if (match) {
          setSelectedRun(match);
        } else {
          setSelectedRun(qualityHistory.data[0]);
        }
      }
    }
  }, [qualityHistory.data, userSelectedRunId]);

  const terminalEndRef = useRef(null);

  // Auto-scroll terminal logs to bottom on update
  useEffect(() => {
    if (terminalEndRef.current) {
      terminalEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [activity.data]);

  // 3. PowerBI Style CSV Export Utility
  const handleExportCSV = (jsonData, filename) => {
    if (!jsonData || !jsonData.length) {
      alert("No data available to export");
      return;
    }
    const headers = Object.keys(jsonData[0]);
    const csvContent = [
      headers.join(','),
      ...jsonData.map(row =>
        headers.map(field => {
          let val = row[field];
          if (val === null || val === undefined) val = '';
          else if (typeof val === 'object') val = JSON.stringify(val);
          let cleanStr = String(val).replace(/"/g, '""');
          if (cleanStr.includes(',') || cleanStr.includes('\n') || cleanStr.includes('"')) {
            cleanStr = `"${cleanStr}"`;
          }
          return cleanStr;
        }).join(',')
      )
    ].join('\n');

    const blob = new Blob(["\uFEFF" + csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${filename}_${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  // 4. Dynamic unique tables for filter dropdown
  const availableTables = useMemo(() => {
    if (!qualityHistory.data || !Array.isArray(qualityHistory.data)) return [];
    const tables = qualityHistory.data.map(run => run.table_name).filter(Boolean);
    return Array.from(new Set(tables)).sort();
  }, [qualityHistory.data]);

  // 5. Interactive Filters logic
  const filteredRuns = useMemo(() => {
    if (!qualityHistory.data || !Array.isArray(qualityHistory.data)) return [];
    return qualityHistory.data.filter(run => {
      const matchSearch =
        (run.run_id || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
        (run.table_name || '').toLowerCase().includes(searchTerm.toLowerCase());

      const matchSource =
        selectedSourceFilter === 'All' ||
        (run.table_name || '').toLowerCase() === selectedSourceFilter.toLowerCase();

      return matchSearch && matchSource;
    });
  }, [qualityHistory.data, searchTerm, selectedSourceFilter]);

  const paginatedRuns = useMemo(() => {
    const start = (historyPage - 1) * historyPageSize;
    return filteredRuns.slice(start, start + historyPageSize);
  }, [filteredRuns, historyPage]);

  // 5. Data transformers for Recharts area visualization
  const seriesKeys = useMemo(() => {
    if (!anomaly.data || !anomaly.data.series) return [];
    return Object.keys(anomaly.data.series);
  }, [anomaly.data]);

  const qualityTrendData = useMemo(() => {
    if (!anomaly.data || !anomaly.data.timestamps || !anomaly.data.series) return [];
    return anomaly.data.timestamps.map((ts, i) => {
      let pt = { time: ts };
      Object.keys(anomaly.data.series).forEach(k => {
         pt[k] = anomaly.data.series[k][i];
      });
      return pt;
    });
  }, [anomaly.data]);

  const forecastData = useMemo(() => {
    if (!projection.data || !projection.data.projection_days) return [];
    return projection.data.projection_days.map((day, i) => ({
      day: `Day +${day}`,
      Forecast:    parseFloat(projection.data.projected_scores[i]?.toFixed(2) ?? 0),
      Optimistic:  parseFloat(projection.data.ci_high[i]?.toFixed(2) ?? 0),
      Pessimistic: parseFloat(projection.data.ci_low[i]?.toFixed(2) ?? 0),
    }));
  }, [projection.data]);

  // Dynamic Y-axis zoom for forecast chart
  const yForecastDomain = useMemo(() => {
    if (!forecastData.length) return [75, 102];
    const allVals = forecastData.flatMap(d => [d.Forecast, d.Optimistic, d.Pessimistic]).filter(Boolean);
    const minVal = Math.min(...allVals);
    const maxVal = Math.max(...allVals);
    const pad = Math.max((maxVal - minVal) * 1.5, 1.5);
    return [parseFloat((minVal - pad).toFixed(1)), parseFloat((maxVal + pad * 0.5).toFixed(1))];
  }, [forecastData]);

  // 6. Action triggering (Retry)
  const [retrying, setRetrying] = useState(false);
  const triggerPipelineRetry = async (runId) => {
    if (!runId) return;
    setRetrying(true);
    try {
      await postApi(`/pipeline/retry/${runId}`);
      alert("Pipeline retry triggered successfully.");
      qualityHistory.refetch();
      kpi.refetch();
      activity.refetch();
    } catch (err) {
      alert("Failed to trigger pipeline retry: " + err.message);
    } finally {
      setRetrying(false);
    }
  };

  const getQualityBadgeClass = (score) => {
    if (score >= 95) return 'high';
    if (score >= 85) return 'warn';
    return 'low';
  };

  return (
    <div className="gs-dashboard">
      {/* 1. Page Header & Info */}
      <div className="gs-topbar">
        <div>
          <h1 className="gs-title">SDOQAP <span>Data Engine Cockpit</span></h1>
          <p className="gs-subtitle">Continuous quality auditing, schema drift evolutions, and quarantine logs</p>
        </div>
        
        <div className="gs-topbar-right">
          <div className="gs-status-cluster">
            <span className={`gs-status-dot ${isHealthy ? 'online' : 'offline'}`} />
            <span className="gs-status-label">{isHealthy ? 'API ONLINE' : 'API WARNING'}</span>
          </div>
          <select
            className="gs-source-select"
            value={selectedSourceFilter}
            onChange={(e) => setSelectedSourceFilter(e.target.value)}
          >
            <option value="All">All Ingestion Sources</option>
            {availableTables.map(table => (
              <option key={table} value={table}>{table}</option>
            ))}
          </select>
        </div>
      </div>

      {/* 2. KPI Metrics Scorecard Row */}
      <div className="gs-kpi-row">
        <div className="gs-kpi gs-kpi-purple">
          <div className="gs-kpi-icon">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/><path d="M3 12c0 1.66 4 3 9 3s9-1.34 9-3"/></svg>
          </div>
          <div className="gs-kpi-body">
            <span className="gs-kpi-value">
              {kpi.loading ? '...' : (kpi.data ? `${(kpi.data.total_records_ingested / 1000000).toFixed(2)}M` : '0.00M')}
            </span>
            <span className="gs-kpi-label">TOTAL INGESTED</span>
          </div>
        </div>
        <div className="gs-kpi gs-kpi-green">
          <div className="gs-kpi-icon">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
          </div>
          <div className="gs-kpi-body">
            <span className="gs-kpi-value">
              {kpi.loading ? '...' : (kpi.data ? `${kpi.data.global_quality_score}%` : '0%')}
            </span>
            <span className="gs-kpi-label">GLOBAL QUALITY SCORE</span>
          </div>
        </div>
        <div className="gs-kpi gs-kpi-red">
          <div className="gs-kpi-icon">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
          </div>
          <div className="gs-kpi-body">
            <span className="gs-kpi-value">
              {kpi.loading ? '...' : (kpi.data ? (kpi.data.quarantined_records || 0).toLocaleString() : '0')}
            </span>
            <span className="gs-kpi-label">QUARANTINED RECORDS</span>
          </div>
        </div>
        <div className="gs-kpi gs-kpi-amber">
          <div className="gs-kpi-icon">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          </div>
          <div className="gs-kpi-body">
            <span className="gs-kpi-value">
              {kpi.loading ? '...' : (kpi.data ? `${kpi.data.mttd_minutes} mins` : '0m')}
            </span>
            <span className="gs-kpi-label">MTTD ANOMALY</span>
          </div>
        </div>
      </div>

      {/* 3. Product Native Motif: End-to-End Data Lineage Map */}
      <div className="gs-lineage-hero">
        <div className="gs-lineage-header">
          <h2>Medallion Flow Data Lineage</h2>
          <span className="gs-lineage-route">Route: Bronze → Silver → Gold / Serving</span>
        </div>
        {(() => {
          const activeRun = selectedRun || (qualityHistory.data && qualityHistory.data[0]);
          const totalRecs = activeRun?.total_records || 0;
          const quarRecs = activeRun?.quarantined_records || 0;
          const hasError = activeRun && quarRecs > 0;
          const hasClean = activeRun && (totalRecs - quarRecs > 0);
          const isExecutionFailed = activeRun && (activeRun.quality_score === 0 || activeRun.quality_score === null);
          return (
            <div className="gs-lineage-track" style={{ display: 'flex', alignItems: 'center', width: '100%', justifyContent: 'space-between' }}>
              {/* Node 1: Ingest Source */}
              <div className={`gs-node active`}>
                <span className="gs-node-icon" style={{ display: 'flex', alignItems: 'center', color: 'var(--accent-purple)' }}>
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="22 12 16 12 14 15 10 15 8 12 2 12"/><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/></svg>
                </span>
                <div className="gs-node-text">
                  <strong>{activeRun ? activeRun.data_source : 'Ingest Source'}</strong>
                  <small>Bronze Layer Inflow</small>
                  {activeRun && <span className="gs-node-stat">{totalRecs.toLocaleString()} rows</span>}
                </div>
              </div>

              <div className={`gs-connector ${isExecutionFailed ? 'danger' : (activeRun ? 'active' : '')}`}>
                <div className="gs-connector-line"></div>
                <div className="gs-connector-arrow">→</div>
              </div>

              {/* Node 2: Spark QA Audit */}
              <div className={`gs-node ${activeRun ? 'active' : ''} ${isExecutionFailed ? 'danger' : ''}`}>
                <span className="gs-node-icon" style={{ display: 'flex', alignItems: 'center', color: 'var(--accent-purple)' }}>
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
                </span>
                <div className="gs-node-text">
                  <strong>Spark QA Audit</strong>
                  <small>Silver Validation</small>
                </div>
              </div>

              <div className={`gs-connector ${isExecutionFailed ? 'danger' : (activeRun ? 'active' : '')}`}>
                <div className="gs-connector-line"></div>
                <div className="gs-connector-arrow">→</div>
              </div>

              {/* Branch Container (OK vs. Quarantine) */}
              <div className="lineage-branches-container" style={{ display: 'flex', alignItems: 'center', flexShrink: 0 }}>
                {/* Left Fork */}
                <div style={{ display: 'flex', flexDirection: 'column', width: '16px', height: '90px', minWidth: '16px', flexShrink: 0 }}>
                  <div style={{ height: '45px', borderLeft: '2px solid var(--accent-purple)', borderTop: '2px solid var(--accent-purple)', borderTopLeftRadius: '6px' }}></div>
                  <div style={{ height: '45px', borderLeft: `2px solid ${hasError ? 'var(--accent-red)' : 'var(--accent-purple)'}`, borderBottom: `2px solid ${hasError ? 'var(--accent-red)' : 'var(--accent-purple)'}`, borderBottomLeftRadius: '6px' }}></div>
                </div>

                {/* Branches List */}
                <div className="lineage-branches" style={{ display: 'flex', flexDirection: 'column', gap: '8px', margin: '0 8px', flexShrink: 0 }}>
                  {/* Node 3: Active Store (Clean) */}
                  <div className={`gs-node ${hasClean ? 'active' : ''}`}>
                    <span className="gs-node-icon" style={{ display: 'flex', alignItems: 'center', color: 'var(--accent-green)' }}>
                      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
                    </span>
                    <div className="gs-node-text">
                      <strong>Active Store</strong>
                      <small>Clean Delta Lake</small>
                      {activeRun && <span className="gs-node-stat">{(totalRecs - quarRecs).toLocaleString()} rows</span>}
                    </div>
                  </div>

                  {/* Node 4: Quarantine */}
                  <div className={`gs-node ${hasError ? 'danger' : ''}`}>
                    <span className="gs-node-icon" style={{ display: 'flex', alignItems: 'center', color: 'var(--accent-red)' }}>
                      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
                    </span>
                    <div className="gs-node-text">
                      <strong>Quarantine</strong>
                      <small>Bad Data Isolation</small>
                      {activeRun && <span className="gs-node-stat">{quarRecs.toLocaleString()} rows</span>}
                    </div>
                  </div>
                </div>

                {/* Right Fork (Only connects Active Store onwards to Serving API) */}
                <div style={{ display: 'flex', flexDirection: 'column', width: '16px', height: '90px', minWidth: '16px', flexShrink: 0 }}>
                  <div style={{ height: '45px', borderRight: '2px solid var(--accent-purple)', borderTop: '2px solid var(--accent-purple)', borderTopRightRadius: '6px' }}></div>
                  <div style={{ height: '45px', borderRight: '2px solid transparent', borderBottom: '2px solid transparent' }}></div>
                </div>
              </div>

              <div className={`gs-connector ${hasClean ? 'active' : ''}`}>
                <div className="gs-connector-line"></div>
                <div className="gs-connector-arrow">→</div>
              </div>

              {/* Node 5: Serving API */}
              <div className={`gs-node ${hasClean ? 'active' : ''}`}>
                <span className="gs-node-icon" style={{ display: 'flex', alignItems: 'center', color: 'var(--accent-purple)' }}>
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="2"/><path d="M16.24 7.76a6 6 0 0 1 0 8.49m-8.48-.01a6 6 0 0 1 0-8.49m11.31-2.82a10 10 0 0 1 0 14.14m-14.14 0a10 10 0 0 1 0-14.14"/></svg>
                </span>
                <div className="gs-node-text">
                  <strong>Serving API</strong>
                  <small>BI & Analytics</small>
                </div>
              </div>
            </div>
          );
        })()}
      </div>

      {/* 4. Main Observability Grid */}
      <div className="gs-main">
        {/* Left Column */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          
          {/* Card 1: Scorecard History */}
          <div className="gs-card gs-card-tall">
            <div className="gs-card-head">
              <div>
                <h3>Scorecard History</h3>
                <p>Pipeline run history and audit records</p>
              </div>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <input
                  type="text"
                  placeholder="Search table/run..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="gs-search"
                />
                <button
                  className="gs-btn-outline"
                  onClick={() => handleExportCSV(filteredRuns, "SDOQAP_Pipeline_Runs")}
                >
                  Export CSV
                </button>
              </div>
            </div>

            <div className="gs-ptable-wrap" style={{ flexGrow: 1, minHeight: 0 }}>
              {qualityHistory.loading ? (
                <div className="gs-empty">Fetching history logs...</div>
              ) : filteredRuns.length === 0 ? (
                <div className="gs-empty">No run history found</div>
              ) : (
                <table className="gs-ptable">
                  <thead>
                    <tr>
                      <th>TIMESTAMP</th>
                      <th>TABLE</th>
                      <th>RUN ID</th>
                      <th>TOTAL ROWS</th>
                      <th>SCORE</th>
                    </tr>
                  </thead>
                  <tbody>
                    {paginatedRuns.map((run) => (
                      <tr
                        key={run.run_id}
                        className={`gs-run-item ${selectedRun && selectedRun.run_id === run.run_id ? 'selected' : ''}`}
                        onClick={() => {
                          setSelectedRun(run);
                          setUserSelectedRunId(run.run_id === qualityHistory.data[0]?.run_id ? null : run.run_id);
                        }}
                      >
                        <td className="gs-mono">{new Date(run.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false })}</td>
                        <td><strong>{run.table_name}</strong></td>
                        <td className="gs-mono">{(run.run_id || '').slice(0, 12)}...</td>
                        <td className="gs-mono">{(run.total_records || 0).toLocaleString()}</td>
                        <td className="gs-mono" style={{ color: run.quality_score >= 95 ? 'var(--accent-green)' : 'var(--accent-red)', fontWeight: 700 }}>
                          {run.quality_score}%
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="gs-pagination">
              <button 
                disabled={historyPage === 1} 
                onClick={() => setHistoryPage(p => Math.max(p - 1, 1))}
              >
                Prev
              </button>
              <span className="gs-muted">
                Page {historyPage} of {Math.ceil(filteredRuns.length / historyPageSize) || 1}
              </span>
              <button 
                disabled={historyPage >= Math.ceil(filteredRuns.length / historyPageSize)} 
                onClick={() => setHistoryPage(p => p + 1)}
              >
                Next
              </button>
            </div>
          </div>

          {/* Card 2: Selected Run Analysis */}
          <div className="gs-card">
            <div className="gs-card-head">
              <div>
                <h3>Selected Run Details</h3>
                <p>Metrics audit breakdown</p>
              </div>
              <div style={{ display: 'flex', gap: '4px' }}>
                <button className={`gs-btn-outline ${leftTab === 'Ratio' ? 'active' : ''}`} onClick={() => setLeftTab('Ratio')}>Ratio</button>
                <button className={`gs-btn-outline ${leftTab === 'Quarantine' ? 'active' : ''}`} onClick={() => setLeftTab('Quarantine')}>Quarantine</button>
                <button className={`gs-btn-outline ${leftTab === 'Insights' ? 'active' : ''}`} onClick={() => setLeftTab('Insights')}>Insights</button>
              </div>
            </div>

            {selectedRun ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: '16px' }}>
                  {leftTab === 'Ratio' && (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                      <div className="gs-detail-grid">
                        <div className="gs-gauge-container">
                          <svg className="gs-gauge" viewBox="0 0 36 36">
                            <circle cx="18" cy="18" r="15.915" fill="transparent" stroke="var(--border-color)" strokeWidth="3.5" />
                            <circle cx="18" cy="18" r="15.915" fill="transparent" stroke="var(--accent-purple)" strokeWidth="3.5"
                                    strokeDasharray={`${selectedRun.quality_score} ${100 - selectedRun.quality_score}`}
                                    strokeDashoffset="0" />
                            <text x="18" y="20.5" className="gs-gauge-text" textAnchor="middle">{selectedRun.quality_score}%</text>
                          </svg>
                        </div>
                        <div className="gs-detail-stats">
                          <div className="gs-stat">
                            <span className="gs-stat-n">{(selectedRun.total_records - selectedRun.quarantined_records).toLocaleString()}</span>
                            <span className="gs-stat-l">Clean Rows</span>
                          </div>
                          <div className="gs-stat">
                            <span className="gs-stat-n" style={{ color: selectedRun.quarantined_records > 0 ? 'var(--accent-red)' : 'var(--text-main)' }}>{selectedRun.quarantined_records.toLocaleString()}</span>
                            <span className="gs-stat-l">Isolated Rows</span>
                          </div>
                        </div>
                      </div>

                      <div className="gs-rules-grid">
                        <div className={`gs-rule ${selectedRun.quarantined_records > 0 ? 'fail' : 'pass'}`}>
                          <span>{selectedRun.quarantined_records > 0 ? '✕' : '✓'}</span> Null Constraint Check
                        </div>
                        <div className={`gs-rule ${selectedRun.quarantined_records > 0 ? 'fail' : 'pass'}`}>
                          <span>{selectedRun.quarantined_records > 0 ? '✕' : '✓'}</span> Schema Consistency
                        </div>
                      </div>
                    </div>
                  )}

                  {leftTab === 'Quarantine' && (
                    <div style={{ height: '120px', overflowY: 'auto' }}>
                      {selectedRun.quarantine_breakdown && Object.keys(selectedRun.quarantine_breakdown).length > 0 ? (
                        Object.entries(selectedRun.quarantine_breakdown).map(([reason, count]) => (
                          <div key={reason} style={{ fontSize: '11px', display: 'flex', justifyContent: 'space-between', padding: '4px 6px', background: 'var(--bg-primary)', border: '1px solid var(--border-color)', borderRadius: '4px', marginBottom: '4px' }}>
                            <span>[✕] {reason}</span>
                            <strong className="gs-mono">{(count || 0).toLocaleString()} rows</strong>
                          </div>
                        ))
                      ) : (
                        <div className="gs-empty">100% Clean data. No records routed to quarantine.</div>
                      )}
                    </div>
                  )}

                  {leftTab === 'Insights' && (
                    <div style={{ fontSize: '11.5px', color: 'var(--text-secondary)', lineHeight: '1.5' }}>
                      {selectedRun.quality_score === 100 ? (
                        <div>All columns conformed perfectly to constraints. Zero anomalies.</div>
                      ) : (
                        <div>
                          Detected {selectedRun.quarantined_records.toLocaleString()} anomalous rows.
                          Auto-routed to <code>/data/quarantine/{selectedRun.table_name}</code> on HDFS to protect downstream systems.
                        </div>
                      )}
                    </div>
                  )}

                  <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                    <div className="gs-stat" style={{ padding: '6px' }}>
                      <span className="gs-stat-l">INGESTION SOURCE</span>
                      <span className="gs-stat-n" style={{ fontSize: '11px', color: 'var(--accent-purple)' }}>{getIngestionSource(selectedRun)}</span>
                    </div>
                    <div className="gs-stat" style={{ padding: '6px' }}>
                      <span className="gs-stat-l">GRADE RATING</span>
                      <span className="gs-stat-n" style={{ fontSize: '11px', color: getQualityGrade(selectedRun.quality_score).color }}>{getQualityGrade(selectedRun.quality_score).grade}</span>
                    </div>
                  </div>
                </div>

                <div style={{ display: 'flex', gap: '8px', marginTop: '4px' }}>
                  <button
                    className="gs-btn-primary"
                    style={{ flexGrow: 1, padding: '8px' }}
                    onClick={() => triggerPipelineRetry(selectedRun.run_id)}
                    disabled={retrying}
                  >
                    {retrying ? 'Triggering Retry...' : 'Retry Ingestion & Audit'}
                  </button>
                  <button
                    className="gs-btn-outline"
                    onClick={() => handleExportCSV([selectedRun], `Run_${selectedRun.run_id}`)}
                  >
                    Export Single Run
                  </button>
                </div>
              </div>
            ) : (
              <div className="gs-empty">Select a run history log to audit</div>
            )}
          </div>

        </div>

        {/* Right Column */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          
          {/* Card 1: System Health Trend */}
          <div className="gs-card gs-card-tall">
            <div className="gs-card-head">
              <div>
                <h3>Data Quality Trends</h3>
                <p>Continuous validation tracking by table</p>
              </div>
              <button
                className="gs-btn-outline"
                onClick={() => handleExportCSV(qualityTrendData, "SDOQAP_Time_Series_Quality")}
              >
                Export Trend
              </button>
            </div>

            <div className="gs-chart-area">
              {anomaly.loading ? (
                <div className="gs-empty">Loading anomaly data...</div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={qualityTrendData} margin={{ top: 10, right: 10, left: -25, bottom: 0 }}>
                    <defs>
                      {seriesKeys.map((k, idx) => {
                        const colors = ["#6C47FF", "#10B981", "#3B82F6", "#F59E0B", "#EF4444"];
                        const color = colors[idx % colors.length];
                        return (
                          <linearGradient key={k} id={`color-${k}`} x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor={color} stopOpacity={0.15}/>
                            <stop offset="95%" stopColor={color} stopOpacity={0}/>
                          </linearGradient>
                        );
                      })}
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.05)" />
                    <XAxis dataKey="time" stroke="#64748b" tick={{ fontSize: 9, fontFamily: "Inter" }} />
                    <YAxis domain={[0, 100]} stroke="#64748b" tick={{ fontSize: 9, fontFamily: "Inter" }} />
                    <Tooltip contentStyle={{ background: '#FFFFFF', border: '1px solid #E2E8F0', borderRadius: 8 }} />
                    {seriesKeys.map((k, idx) => {
                        const colors = ["#6C47FF", "#10B981", "#3B82F6", "#F59E0B", "#EF4444"];
                        const color = colors[idx % colors.length];
                        return (
                          <Area key={k} type="monotone" dataKey={k} stroke={color} fillOpacity={1} fill={`url(#color-${k})`} strokeWidth={2} />
                        );
                    })}
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

          {/* Card 2: Blueprint Tabs (Forecast, Cause, Impact, Actions) */}
          <div className="gs-card">
            <div className="gs-card-head">
              <div>
                <h3>Global Observability Blueprint</h3>
                <p>Statistical intelligence &amp; closed-loop recommendations</p>
              </div>
            </div>

            <div style={{ display: 'flex', gap: '4px', background: 'var(--bg-primary)', border: '1px solid var(--border-color)', padding: '3px', borderRadius: '8px', marginBottom: '12px' }}>
              <button style={{ flex: 1, padding: '6px', fontSize: '10px' }} className={`gs-btn-outline ${centerTab === 'Trends' ? 'active' : ''}`} onClick={() => setCenterTab('Trends')}>Projection</button>
              <button style={{ flex: 1, padding: '6px', fontSize: '10px' }} className={`gs-btn-outline ${centerTab === 'RootCause' ? 'active' : ''}`} onClick={() => setCenterTab('RootCause')}>Root Cause</button>
              <button style={{ flex: 1, padding: '6px', fontSize: '10px' }} className={`gs-btn-outline ${centerTab === 'Impact' ? 'active' : ''}`} onClick={() => setCenterTab('Impact')}>Impact Map</button>
              <button style={{ flex: 1, padding: '6px', fontSize: '10px' }} className={`gs-btn-outline ${centerTab === 'Actionable' ? 'active' : ''}`} onClick={() => setCenterTab('Actionable')}>Recommendations</button>
            </div>

            <div style={{ height: '140px', overflowY: 'auto' }}>
              {centerTab === 'Trends' && (
                <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 0.8fr', gap: '12px', height: '100%' }}>
                  <div style={{ height: '100%' }}>
                    {projection.loading ? (
                      <div className="gs-empty">Processing model...</div>
                    ) : (
                      <ResponsiveContainer width="100%" height="100%">
                        <ComposedChart data={forecastData} margin={{ top: 5, right: 10, left: -25, bottom: 0 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.04)" />
                          <XAxis dataKey="day" stroke="#64748b" tick={{ fontSize: 8 }} />
                          <YAxis domain={yForecastDomain} stroke="#64748b" tick={{ fontSize: 8 }} />
                          <Tooltip />
                          <ReferenceLine y={95} stroke="var(--accent-yellow)" strokeDasharray="4 2" />
                          <Area type="monotone" dataKey="Optimistic" stroke="var(--accent-green)" fill="rgba(16,185,129,0.05)" />
                          <Area type="monotone" dataKey="Pessimistic" stroke="var(--accent-red)" fill="rgba(239,68,68,0.05)" />
                          <Line type="monotone" dataKey="Forecast" stroke="var(--accent-purple)" strokeWidth={2} dot={{ r: 2 }} />
                        </ComposedChart>
                      </ResponsiveContainer>
                    )}
                  </div>
                  <div style={{ fontSize: '10.5px', display: 'flex', flexDirection: 'column', gap: '4px' }}>
                    <strong>SLA Stability Index:</strong>
                    <span style={{ color: 'var(--accent-green)', fontWeight: 700 }}>{projection.data?.stability_index || 'Calculating...'}</span>
                    <strong>Breach Risk Rate:</strong>
                    <span style={{ color: 'var(--accent-red)', fontWeight: 700 }}>{projection.data?.sla_breach_probability || 'Calculating...'}</span>
                  </div>
                </div>
              )}

              {centerTab === 'RootCause' && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  {clustering.loading ? (
                    <div className="gs-empty">Clustering patterns...</div>
                  ) : clustering.data?.clusters?.slice(0, 2).map((cluster) => (
                    <div key={cluster.id} style={{ padding: '6px 10px', background: 'var(--bg-primary)', border: '1px solid var(--border-color)', borderRadius: '8px' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', fontWeight: 'bold' }}>
                        <span>{cluster.source}</span>
                        <span style={{ color: 'var(--accent-red)' }}>{cluster.percentage}% (N={cluster.errors_count})</span>
                      </div>
                      <div style={{ color: 'var(--text-muted)', fontSize: '10px', marginTop: '2px' }}>Pattern: {cluster.pattern}</div>
                    </div>
                  ))}
                </div>
              )}

              {centerTab === 'Impact' && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '6px' }}>
                    {impact.data?.kpi_connections?.slice(0, 3).map((kpi, i) => (
                      <div key={i} style={{ padding: '8px', background: 'var(--bg-primary)', border: '1px solid var(--border-color)', borderRadius: '8px', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                        <span style={{ fontSize: '9px', color: 'var(--text-muted)', fontWeight: 700 }}>{kpi.kpi_name}</span>
                        <span style={{ fontSize: '14px', fontWeight: 800, color: 'var(--accent-red)', margin: '2px 0' }}>-{kpi.impact_pct}%</span>
                      </div>
                    ))}
                  </div>
                  <div style={{ padding: '6px', background: 'rgba(239,68,68,0.04)', border: '1px solid rgba(239,68,68,0.1)', borderRadius: '6px', fontSize: '11px', display: 'flex', justifyContent: 'space-between' }}>
                    <span>Projected Financial Risk:</span>
                    <strong style={{ color: 'var(--accent-red)' }}>${(impact.data?.total_financial_impact_usd || 0).toLocaleString()} USD</strong>
                  </div>
                </div>
              )}

              {centerTab === 'Actionable' && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  {recommendations.data?.recommendations?.slice(0, 2).map((rec) => (
                    <div key={rec.id} style={{ display: 'flex', justify: 'space-between', alignItems: 'center', padding: '6px', background: 'var(--bg-primary)', border: '1px solid var(--border-color)', borderRadius: '8px' }}>
                      <div style={{ display: 'flex', flexDirection: 'column', fontSize: '10.5px' }}>
                        <strong>{rec.title}</strong>
                        <span style={{ color: 'var(--text-muted)', fontSize: '9.5px' }}>{rec.description}</span>
                      </div>
                      <button className="gs-btn-outline" style={{ padding: '2px 6px', fontSize: '9.5px' }} onClick={() => alert(`Run: ${rec.action_type}`)}>Run</button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Card 3: Activity Log Terminal */}
          <div className="gs-card">
            <div className="gs-card-head">
              <div>
                <h3>System Activity Stream</h3>
                <p>Elasticsearch real-time logs ingestion</p>
              </div>
              <button
                className="gs-btn-outline"
                onClick={() => handleExportCSV(activity.data || [], "SDOQAP_System_Logs")}
              >
                Export Logs
              </button>
            </div>

            <div className="gs-terminal">
              <div className="gs-terminal-bar">
                <div className="gs-terminal-dots">
                  <i></i><i></i><i></i>
                </div>
                <span>sdoqap@observability-node:~</span>
              </div>
              <div className="gs-terminal-body">
                {activity.loading ? (
                  <div className="gs-log">Connecting to stream...</div>
                ) : activity.data && activity.data.length > 0 ? (
                  <>
                    {[...activity.data].reverse().map((act, i) => {
                      let lvlClass = 'info';
                      if (act.level === 'error') lvlClass = 'error';
                      else if (act.level === 'warning') lvlClass = 'warn';

                      return (
                        <div key={i} className="gs-terminal-line">
                          <span className="gs-log-ts">[{new Date(act.timestamp).toLocaleTimeString([], { hour12: false })}]</span>
                          <span className={`gs-log ${lvlClass}`}>{act.message}</span>
                        </div>
                      );
                    })}
                    <div ref={terminalEndRef} />
                  </>
                ) : (
                  <div className="gs-log error">No active logs fetched from node</div>
                )}
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>
  );
}
