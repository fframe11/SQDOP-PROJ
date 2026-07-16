import React, { useState, useMemo, useEffect, useRef } from 'react';
import { useApi, postApi } from '../hooks/useApi';
import { ComposedChart, AreaChart, Area, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Legend } from 'recharts';

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
  const historyPageSize = 8;

  useEffect(() => {
    setHistoryPage(1);
  }, [searchTerm, selectedSourceFilter]);

  // 2. Fetch real-time metrics from API endpoints
  const kpi = useApi('/kpi/stats', { refreshInterval: 15000 });
  const anomaly = useApi('/anomaly/sources', { refreshInterval: 15000 });
  const services = useApi('/services/status', { refreshInterval: 10000 });
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
    <div className="sdoqap-app">
      {/* 1. Page Header & Breadcrumbs (Clerk Style) */}
      <div style={{ marginBottom: "1.5rem", width: "100%" }}>
        <div style={{ fontSize: "11px", color: "var(--text-muted)", marginBottom: "0.4rem", fontFamily: "var(--font-sans)", display: "flex", gap: "6px", alignItems: "center" }}>
          <span>SDOQAP Data Engine</span>
          <span style={{ opacity: 0.5 }}>&gt;</span>
          <span style={{ color: "var(--text-main)", fontWeight: 500 }}>Dashboard</span>
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
          <div>
            <h1 style={{ fontSize: "1.6rem", fontWeight: 700, color: "var(--text-main)", letterSpacing: "-0.02em", margin: 0 }}>Dashboard</h1>
            <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", margin: "4px 0 0 0" }}>View and manage system activities</p>
          </div>
          
          {/* Muted health pills positioned cleanly on the right */}
          <div className="service-hub" style={{ margin: 0, padding: 0, gap: "6px", background: "transparent", border: "none", display: "flex", alignItems: "center" }}>
            {services.data ? (
              Object.entries(services.data).map(([name, info]) => (
                <div key={name} className="service-card" title={info.url || 'Internal Port'} style={{ padding: "4px 8px", background: "#FFFFFF", border: "1px solid #E2E8F0", borderRadius: "20px", display: "flex", alignItems: "center", gap: "4px" }}>
                  <span className={`status-dot ${info.status === 'online' ? 'online' : 'offline'}`} style={{ width: "6px", height: "6px" }} />
                  <span className="service-name" style={{ fontSize: "10px", color: "var(--text-muted)" }}>{name}</span>
                </div>
              ))
            ) : null}
            <div className={`overall-status ${services.error ? 'offline' : ''}`} style={{ padding: "4px 8px", background: "#FFFFFF", border: "1px solid #E2E8F0", borderRadius: "20px", display: "flex", alignItems: "center", gap: "4px" }}>
              <span className={`status-dot ${services.error ? 'offline' : 'online'}`} style={{ width: "6px", height: "6px" }} />
              <span style={{ fontSize: "10px", color: services.error ? "var(--accent-red)" : "var(--text-muted)" }}>{services.error ? 'OFFLINE' : 'ONLINE'}</span>
            </div>
          </div>
        </div>
      </div>

      {/* 2. KPI Metrics Scorecard Row */}
      <div className="kpi-row">
        <div className="kpi-card blue animate-in">
          <div className="kpi-card-left">
            <span className="kpi-val">
              {kpi.data ? `${(kpi.data.total_records_ingested / 1000000).toFixed(2)}M` : '1.52M'}
            </span>
            <span className="kpi-label">TOTAL RECORDS INGESTED</span>
          </div>
          <span className="kpi-icon">Bar</span>
        </div>
        <div className="kpi-card quality-kpi animate-in">
          <div className="kpi-card-left">
            <span className="kpi-val">
              {kpi.data ? `${kpi.data.global_quality_score}%` : '98.4%'}
            </span>
            <span className="kpi-label">GLOBAL QUALITY SCORE</span>
          </div>
          <span className="kpi-icon">Shld</span>
        </div>
        <div className="kpi-card quarantine-kpi animate-in">
          <div className="kpi-card-left">
            <span className="kpi-val">
              {kpi.data ? (kpi.data.quarantined_records || 0).toLocaleString() : '24,320'}
            </span>
            <span className="kpi-label">QUARANTINED RECORDS</span>
          </div>
          <span className="kpi-icon">Warn</span>
        </div>
        <div className="kpi-card amber animate-in">
          <div className="kpi-card-left">
            <span className="kpi-val">
              {kpi.data ? `${kpi.data.mttd_minutes} mins` : '2.4 mins'}
            </span>
            <span className="kpi-label">MTTD (MEAN TIME TO DETECT)</span>
          </div>
          <span className="kpi-icon">Time</span>
        </div>
      </div>

      {/* 3. Main Analytical Grid */}
      <div className="main-grid">

        {/* ================= COLUMN LEFT ================= */}
        <div className="col-left">

          {/* Card 1: Scorecard History */}
          <div className="glass-card animate-in">
            <div className="card-header" style={{ borderBottom: "none", paddingBottom: 0, marginBottom: "0.5rem" }}>
              <div>
                <h3 className="card-title"><span className="icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ display: 'inline-block', verticalAlign: 'middle', marginTop: '-2px' }}><line x1="8" y1="6" x2="21" y2="6" /><line x1="8" y1="12" x2="21" y2="12" /><line x1="8" y1="18" x2="21" y2="18" /><line x1="3" y1="6" x2="3.01" y2="6" /><line x1="3" y1="12" x2="3.01" y2="12" /><line x1="3" y1="18" x2="3.01" y2="18" /></svg></span> Scorecard History</h3>
                <p className="card-subtitle">Pipeline Run History &amp; QA Results</p>
              </div>
            </div>

            {/* Clerk-Style Controls Row */}
            <div className="dashboard-controls-row" style={{ display: "flex", gap: "1rem", alignItems: "center", padding: "0 1.25rem 1rem 1.25rem", borderBottom: "1px solid #F1F5F9", width: "100%", flexWrap: "wrap" }}>
              <div className="search-container" style={{ flex: 1, margin: 0, minWidth: "200px", position: "relative" }}>
                <span className="search-icon" style={{ left: "10px", top: "50%", transform: "translateY(-50%)", position: "absolute", display: "flex", alignItems: "center", color: "#64748B" }}>
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
                </span>
                <input
                  type="text"
                  placeholder="Search by ID or Table..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  style={{ paddingLeft: "2.25rem", width: "100%", height: "34px", borderRadius: "6px", border: "1px solid #E2E8F0" }}
                />
              </div>

              <div style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
                <span style={{ fontSize: "12px", color: "var(--text-muted)", whiteSpace: "nowrap" }}>Filter:</span>
                <select
                  className="filter-select"
                  value={selectedSourceFilter}
                  onChange={(setSelected) => setSelectedSourceFilter(setSelected.target.value)}
                  style={{ width: "auto", minWidth: "150px", padding: "6px 12px", background: "#FFFFFF", border: "1px solid #E2E8F0", borderRadius: "6px", fontSize: "13px", color: "var(--text-main)", height: "34px" }}
                >
                  <option value="All">All Sources</option>
                  {availableTables.map(table => (
                    <option key={table} value={table}>{table}</option>
                  ))}
                </select>
              </div>

              <button
                className="btn btn-primary"
                onClick={() => handleExportCSV(filteredRuns, "SDOQAP_Pipeline_Runs")}
                style={{ padding: "0 1.25rem", borderRadius: "6px", fontSize: "13px", height: "34px" }}
              >
                Export CSV
              </button>
            </div>

            <div className="table-wrapper">
              {qualityHistory.loading ? (
                <div className="loading-state"><span>Fetching history...</span></div>
              ) : filteredRuns.length === 0 ? (
                <div className="loading-state" style={{ color: 'var(--text-muted)' }}>No run history found</div>
              ) : (
                <>
                  <table>
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
                        className={`clickable-row ${selectedRun && selectedRun.run_id === run.run_id ? 'selected' : ''}`}
                        onClick={() => {
                          setSelectedRun(run);
                          setUserSelectedRunId(run.run_id === qualityHistory.data[0]?.run_id ? null : run.run_id);
                        }}
                      >
                        <td>{new Date(run.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false })}</td>
                        <td><strong>{run.table_name}</strong></td>
                        <td style={{ fontFamily: 'var(--font-mono)' }}>{(run.run_id || '').slice(0, 15)}...</td>
                        <td style={{ fontFamily: 'var(--font-mono)' }}>{(run.total_records || 0).toLocaleString()}</td>
                        <td className="score-text" style={{ color: run.quality_score >= 95 ? 'var(--accent-green)' : 'var(--accent-red)' }}>
                          {run.quality_score}%
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '1rem', padding: '0 0.5rem' }}>
                  <button 
                    className="btn btn-secondary" 
                    disabled={historyPage === 1} 
                    onClick={() => setHistoryPage(p => Math.max(p - 1, 1))}
                    style={{ padding: '0.25rem 0.75rem', fontSize: '0.75rem', background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.08)', color: '#fff', cursor: 'pointer', borderRadius: '4px' }}
                  >
                    Previous
                  </button>
                  <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                    Page {historyPage} of {Math.ceil(filteredRuns.length / historyPageSize) || 1}
                  </span>
                  <button 
                    className="btn btn-secondary" 
                    disabled={historyPage >= Math.ceil(filteredRuns.length / historyPageSize)} 
                    onClick={() => setHistoryPage(p => p + 1)}
                    style={{ padding: '0.25rem 0.75rem', fontSize: '0.75rem', background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.08)', color: '#fff', cursor: 'pointer', borderRadius: '4px' }}
                  >
                    Next
                  </button>
                </div>
              </>
            )}
            </div>
          </div>

          {/* Card 2: Selected Run Analysis */}
          <div className="glass-card animate-in">
            <div className="card-header">
              <div>
                <h3 className="card-title"><span className="icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ display: 'inline-block', verticalAlign: 'middle', marginTop: '-2px' }}><circle cx="12" cy="12" r="10" /><circle cx="12" cy="12" r="6" /><circle cx="12" cy="12" r="2" /></svg></span> Selected Run Analysis</h3>
                <p className="card-subtitle">Data Filtering Details (Clean vs Quarantined)</p>
              </div>
              <div className="tab-btn-group">
                <button className={`btn-tab ${leftTab === 'Ratio' ? 'active' : ''}`} onClick={() => setLeftTab('Ratio')}>Ratio</button>
                <button className={`btn-tab ${leftTab === 'Quarantine' ? 'active' : ''}`} onClick={() => setLeftTab('Quarantine')}>Quarantine</button>
                <button className={`btn-tab ${leftTab === 'Insights' ? 'active' : ''}`} onClick={() => setLeftTab('Insights')}>Insights</button>
              </div>
            </div>

            {selectedRun ? (
              <div className="detail-container">
                <div className="detail-charts-panel">
                  <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginBottom: '8px' }}>
                    Dataset: <strong>{selectedRun.table_name}</strong> | ID: <strong>{selectedRun.run_id}</strong>
                  </div>

                  {leftTab === 'Ratio' && (
                    <div style={{ flexGrow: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', marginBottom: '4px' }}>
                        <span style={{ color: 'var(--accent-green)' }}>Clean: {((selectedRun.clean_records / selectedRun.total_records) * 100).toFixed(1)}%</span>
                        <span style={{ color: 'var(--accent-red)' }}>Quarantined: {((selectedRun.quarantined_records / selectedRun.total_records) * 100).toFixed(1)}%</span>
                      </div>
                      <div style={{ height: '14px', width: '100%', backgroundColor: 'rgba(255,255,255,0.05)', borderRadius: '6px', overflow: 'hidden', display: 'flex' }}>
                        <div style={{ width: `${(selectedRun.clean_records / selectedRun.total_records) * 100}%`, backgroundColor: 'var(--accent-green)' }} />
                        <div style={{ width: `${(selectedRun.quarantined_records / selectedRun.total_records) * 100}%`, backgroundColor: 'var(--accent-red)' }} />
                      </div>
                      <div className="stat-card" style={{ marginTop: '12px' }}>
                        <span className="stat-label">QA Status</span>
                        <span className={`quality-badge ${getQualityBadgeClass(selectedRun.quality_score)}`} style={{ textAlign: 'center', marginTop: '4px' }}>
                          {selectedRun.quality_score >= 95 ? 'HEALTHY PIPELINE' : selectedRun.quality_score >= 85 ? 'WARNING DRIFT' : 'CRITICAL ANOMALY'}
                        </span>
                      </div>
                    </div>
                  )}

                  {leftTab === 'Quarantine' && (
                    <div style={{ flexGrow: 1, overflowY: 'auto' }}>
                      <div style={{ fontSize: '11px', fontWeight: 'bold', marginBottom: '6px', color: 'var(--accent-red)' }}>Quarantine Reasons:</div>
                      {selectedRun.quarantine_breakdown && Object.keys(selectedRun.quarantine_breakdown).length > 0 ? (
                        Object.entries(selectedRun.quarantine_breakdown).map(([reason, count]) => (
                          <div key={reason} style={{ fontSize: '11px', padding: '4px 6px', background: 'rgba(244,63,94,0.04)', border: '1px solid rgba(244,63,94,0.12)', borderRadius: '4px', marginBottom: '4px', display: 'flex', justifyContent: 'space-between' }}>
                            <span>[X] {reason}</span>
                            <span style={{ fontFamily: 'var(--font-mono)' }}>{(count || 0).toLocaleString()} rows</span>
                          </div>
                        ))
                      ) : (
                        <div style={{ fontSize: '11px', color: 'var(--text-muted)', textAlign: 'center', marginTop: '10px' }}>100% Clean! No bad records found</div>
                      )}
                    </div>
                  )}

                  {leftTab === 'Insights' && (
                    <div style={{ flexGrow: 1, overflowY: 'auto', fontSize: '11.5px', lineHeight: '1.45', color: 'var(--text-secondary)' }}>
                      {selectedRun.quality_score === 100 ? (
                        <div>100% Clean data. No Type Mismatch or Null Constraint issues detected.</div>
                      ) : (
                        <div>
                          Found bad records at {(((selectedRun.quarantined_records || 0) / (selectedRun.total_records || 1)) * 100).toFixed(1)}% (<strong>{(selectedRun.quarantined_records || 0).toLocaleString()} rows</strong>)
                          Isolated to HDFS Quarantine to protect downstream pipelines.
                          <br /><br />
                          <strong>AI Suggestion:</strong> Check if source API or CSV has Schema Drift.
                        </div>
                      )}
                    </div>
                  )}

                  <div style={{ marginTop: 'auto', display: 'flex', gap: '8px' }}>
                    <button
                      className="btn-tab"
                      style={{ flexGrow: 1, backgroundColor: 'rgba(56, 189, 248, 0.1)', color: 'var(--accent-blue)', borderColor: 'rgba(56, 189, 248, 0.3)', padding: '5px' }}
                      onClick={() => triggerPipelineRetry(selectedRun.run_id)}
                      disabled={retrying}
                    >
                      {retrying ? 'Retrying...' : 'Retry Ingest & Audit'}
                    </button>
                    <button
                      className="btn-tab"
                      style={{ padding: '5px 10px' }}
                      onClick={() => handleExportCSV([selectedRun], `Selected_Run_${selectedRun.run_id}`)}
                    >
                      Export
                    </button>
                  </div>
                </div>

                <div className="detail-stats-panel">
                  <div className="stat-card">
                    <span className="stat-label">DATASET / TABLE</span>
                    <span className="stat-value" style={{ color: 'var(--accent-blue)' }}>{selectedRun.table_name}</span>
                  </div>
                  <div className="stat-card">
                    <span className="stat-label">RUN ID</span>
                    <span className="stat-value" style={{ fontSize: '11px' }}>{selectedRun.run_id}</span>
                  </div>
                  <div className="stat-card">
                    <span className="stat-label">TOTAL RECORDS</span>
                    <span className="stat-value">{(selectedRun.total_records || 0).toLocaleString()}</span>
                  </div>
                  <div className="stat-card">
                    <span className="stat-label">CLEAN (ACTIVE)</span>
                    <span className="stat-value" style={{ color: 'var(--accent-green)' }}>{(selectedRun.clean_records || 0).toLocaleString()}</span>
                  </div>
                  <div className="stat-card">
                    <span className="stat-label">QUARANTINE (BAD)</span>
                    <span className="stat-value" style={{ color: 'var(--accent-red)' }}>{(selectedRun.quarantined_records || 0).toLocaleString()}</span>
                  </div>
                  <div className="stat-card">
                    <span className="stat-label">FRESHNESS LAG</span>
                    <span className="stat-value" style={{ color: 'var(--accent-yellow)' }}>{selectedRun.freshness_lag_hours ? `${selectedRun.freshness_lag_hours.toFixed(2)} hrs` : '0.12 hrs'}</span>
                  </div>
                </div>
              </div>
            ) : (
              <div className="loading-state"><span>Select a run history to inspect</span></div>
            )}
          </div>

        </div>

        {/* ================= COLUMN CENTER ================= */}
        <div className="col-center">

          {/* Card 1: System Health (Quality Trend Chart) */}
          <div className="glass-card animate-in">
            <div className="card-header">
              <div>
                <h3 className="card-title"><span className="icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ display: 'inline-block', verticalAlign: 'middle', marginTop: '-2px' }}><path d="M3 3v18h18" /><path d="m18.7 8-5.1 5.2-2.8-2.7L7 14.3" /></svg></span> System Health: Data Quality Anomaly Detection</h3>
                <p className="card-subtitle">Data Quality Trend by Source</p>
              </div>
              <button
                className="btn-export"
                onClick={() => handleExportCSV(qualityTrendData, "SDOQAP_Time_Series_Quality")}
              >
                Export Trend
              </button>
            </div>

            <div style={{ flexGrow: 1, minHeight: 0, width: '100%', height: '100%' }}>
              {anomaly.loading ? (
                <div className="loading-state"><span>Fetching anomaly stats...</span></div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={qualityTrendData} margin={{ top: 10, right: 10, left: -25, bottom: 0 }}>
                    <defs>
                      {seriesKeys.map((k, idx) => {
                        const colors = ["#38bdf8", "#10b981", "#fbbf24", "#f43f5e", "#a855f7"];
                        const color = colors[idx % colors.length];
                        return (
                          <linearGradient key={k} id={`color-${k}`} x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor={color} stopOpacity={0.15}/>
                            <stop offset="95%" stopColor={color} stopOpacity={0}/>
                          </linearGradient>
                        );
                      })}
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.03)" />
                    <XAxis dataKey="time" stroke="#64748b" tick={{ fontSize: 9.5, fontFamily: "Inter, sans-serif" }} />
                    <YAxis domain={[0, 100]} stroke="#64748b" tick={{ fontSize: 9.5, fontFamily: "Inter, sans-serif" }} />
                    <Tooltip contentClassName="custom-tooltip" wrapperStyle={{ fontFamily: "Inter, sans-serif" }} />
                    {seriesKeys.map((k, idx) => {
                        const colors = ["#38bdf8", "#10b981", "#fbbf24", "#f43f5e", "#a855f7"];
                        const color = colors[idx % colors.length];
                        return (
                          <Area key={k} type="monotone" dataKey={k} stroke={color} fillOpacity={1} fill={`url(#color-${k})`} strokeWidth={1.8} />
                        );
                    })}
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

          {/* Card 2: Live Alerts Log */}
          <div className="glass-card animate-in" style={{ flex: 'none', height: '120px' }}>
            <div className="card-header">
              <div>
                <h3 className="card-title" style={{ color: 'var(--accent-red)' }}><span className="icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ display: 'inline-block', verticalAlign: 'middle', marginTop: '-2px' }}><path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9" /><path d="M10.3 21a1.94 1.94 0 0 0 3.4 0" /></svg></span> Live Alerts Log</h3>
                <p className="card-subtitle">Real-time Alerts (Failures & Schema Drifts)</p>
              </div>
            </div>
            <div className="alerts-log-container">
              {anomaly.data && anomaly.data.anomaly ? (
                <>
                  <div className="alert-item">
                    <span className="alert-time">[{anomaly.data.anomaly.time}]</span>
                    <span>[!] <strong>[Drift Detected - {anomaly.data.anomaly.source}]</strong> {anomaly.data.anomaly.reason} (Score dropped to {anomaly.data.anomaly.score}%)</span>
                  </div>
                  <div className="alert-item info">
                    <span className="alert-time">[{anomaly.data.anomaly.time}]</span>
                    <span>[i] <strong>System Automation:</strong> Isolated problematic raw data to HDFS Quarantine without blocking the main pipeline.</span>
                  </div>
                </>
              ) : (
                <div className="alert-item info">
                  <span className="alert-time">[Stream Normal]</span>
                  <span>[OK] Data streaming consistently. No quality violations or schema drifts detected.</span>
                </div>
              )}
            </div>
          </div>

          {/* Card 3: SDOQAP Analytical Intelligence Blueprint */}
          <div className="glass-card animate-in">
            <div className="card-header" style={{ paddingBottom: '0', borderBottom: 'none' }}>
              <div>
                <h3 className="card-title"><span className="icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ display: 'inline-block', verticalAlign: 'middle', marginTop: '-2px' }}><path d="M9.5 2A2.5 2.5 0 0 1 12 4.5v15a2.5 2.5 0 0 1-4.96-.44 2.5 2.5 0 0 1 0-3.12 3 3 0 0 1 0-3.88 2.5 2.5 0 0 1 0-3.12A2.5 2.5 0 0 1 9.5 2Z" /><path d="M14.5 2A2.5 2.5 0 0 0 12 4.5v15a2.5 2.5 0 0 0 4.96-.44 2.5 2.5 0 0 0 0-3.12 3 3 0 0 0 0-3.88 2.5 2.5 0 0 0 0-3.12A2.5 2.5 0 0 0 14.5 2Z" /></svg></span> Global Analytical Blueprint</h3>
                <p className="card-subtitle">Global Pipeline Overview</p>
              </div>
              <button
                className="btn-export"
                onClick={() => {
                  let dataToExport = [];
                  if (centerTab === 'Trends') dataToExport = forecastData;
                  else if (centerTab === 'RootCause') dataToExport = clustering.data?.clusters || [];
                  else if (centerTab === 'Impact') dataToExport = impact.data?.kpi_connections || [];
                  else dataToExport = recommendations.data?.recommendations || [];
                  handleExportCSV(dataToExport, `Analysis_${centerTab}`);
                }}
              >
                Export Analytics
              </button>
            </div>

            <div className="blueprint-tabs" style={{ marginTop: '8px' }}>
              <button className={`blueprint-tab-btn ${centerTab === 'Trends' ? 'active' : ''}`} onClick={() => setCenterTab('Trends')}>1. Trends & Projection</button>
              <button className={`blueprint-tab-btn ${centerTab === 'RootCause' ? 'active' : ''}`} onClick={() => setCenterTab('RootCause')}>2. Root Cause Diagnostic</button>
              <button className={`blueprint-tab-btn ${centerTab === 'Impact' ? 'active' : ''}`} onClick={() => setCenterTab('Impact')}>3. Business Impact Map</button>
              <button className={`blueprint-tab-btn ${centerTab === 'Actionable' ? 'active' : ''}`} onClick={() => setCenterTab('Actionable')}>4. Actionable Engine</button>
            </div>

            <div className="blueprint-content">
              {centerTab === 'Trends' && (
                <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 0.8fr', gap: '12px', height: '100%', minHeight: 0 }}>
                  <div style={{ height: '100%', minHeight: 0 }}>
                    {projection.loading ? (
                      <div className="loading-state"><span>Processing predictive model...</span></div>
                    ) : (
                      <ResponsiveContainer width="100%" height="100%">
                        <ComposedChart data={forecastData} margin={{ top: 5, right: 20, left: -10, bottom: 0 }}>
                          <defs>
                            <linearGradient id="dashFillHigh" x1="0" y1="0" x2="0" y2="1">
                              <stop offset="0%"  stopColor="#10b981" stopOpacity={0.28}/>
                              <stop offset="100%" stopColor="#10b981" stopOpacity={0.03}/>
                            </linearGradient>
                            <linearGradient id="dashFillLow" x1="0" y1="0" x2="0" y2="1">
                              <stop offset="0%"  stopColor="#f43f5e" stopOpacity={0.18}/>
                              <stop offset="100%" stopColor="#f43f5e" stopOpacity={0.03}/>
                            </linearGradient>
                          </defs>
                          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                          <XAxis dataKey="day" stroke="#64748b" tick={{ fontSize: 9, fontFamily: "Inter, sans-serif" }} />
                          <YAxis
                            domain={yForecastDomain}
                            stroke="#64748b"
                            tick={{ fontSize: 9, fontFamily: "Inter, sans-serif" }}
                            tickFormatter={v => `${v}%`}
                          />
                          <Tooltip
                            contentStyle={{ background: '#0f172a', border: '1px solid rgba(108, 71, 255, 0.25)', borderRadius: 6, fontSize: 11, fontFamily: "Inter, sans-serif" }}
                            formatter={(val, name) => [`${typeof val === 'number' ? val.toFixed(2) : val}%`, name]}
                          />
                          <Legend verticalAlign="top" height={22} wrapperStyle={{ fontSize: 9, fontFamily: "Inter, sans-serif" }} />
                          <ReferenceLine
                            y={95}
                            stroke="#f59e0b"
                            strokeDasharray="5 3"
                            strokeWidth={1.5}
                            label={{ value: 'SLA', position: 'right', fill: '#f59e0b', fontSize: 9, fontFamily: "Inter, sans-serif" }}
                          />
                          <Area type="monotone" dataKey="Optimistic" stroke="#10b981" strokeWidth={2} fill="url(#dashFillHigh)" dot={{ r: 2.5, fill: '#10b981', strokeWidth: 0 }} />
                          <Area type="monotone" dataKey="Pessimistic" stroke="#f43f5e" strokeWidth={2} fill="url(#dashFillLow)" dot={{ r: 2.5, fill: '#f43f5e', strokeWidth: 0 }} />
                          <Line type="monotone" dataKey="Forecast" stroke="#00E5FF" strokeWidth={2.5} dot={{ r: 3.5, fill: '#00E5FF', stroke: '#0f172a', strokeWidth: 1.5 }} activeDot={{ r: 6 }} />
                        </ComposedChart>
                      </ResponsiveContainer>
                    )}
                  </div>
                  <div style={{ fontSize: '11px', display: 'flex', flexDirection: 'column', gap: '6px', overflowY: 'auto' }}>
                    <div style={{ borderBottom: '1px solid rgba(255,255,255,0.05)', paddingBottom: '4px', fontWeight: 'bold', color: 'var(--accent-blue)' }}>7-Day Quality Forecast:</div>
                    <div>• <strong>Data Stability Index:</strong> <span style={{ color: 'var(--accent-green)' }}>{projection.data?.stability_index || '78.4%'}</span></div>
                    <div>• <strong>SLA Breach Probability:</strong> <span style={{ color: 'var(--accent-red)' }}>{projection.data?.sla_breach_probability || '45.2%'}</span></div>
                    <div style={{ color: 'var(--text-muted)', fontSize: '10.5px' }}>{projection.data?.historical_trend}</div>

                    {projection.data?.crisis_forecast && projection.data.crisis_forecast.severity !== 'LOW' && (
                      <div className="bp-alert">
                        <strong>Predicted Quality Crisis Alert</strong>
                        <div>Quality crisis predicted in {projection.data.crisis_forecast.days_until_crisis} days on "{projection.data.crisis_forecast.impacted_component}"</div>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {centerTab === 'RootCause' && (
                <div style={{ overflowY: 'auto', height: '100%', fontSize: '11.5px' }}>
                  <div style={{ fontWeight: 'bold', marginBottom: '6px', color: 'var(--accent-purple)' }}>Error Pattern Clustering:</div>
                  {clustering.loading ? (
                    <div>Clustering root causes...</div>
                  ) : clustering.data?.clusters?.map((cluster) => (
                    <div key={cluster.id} style={{ padding: '6px', background: 'rgba(255,255,255,0.02)', border: '1px solid rgba(255,255,255,0.05)', borderRadius: '6px', marginBottom: '6px' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontWeight: 'bold' }}>
                        <span>Source: {cluster.source}</span>
                        <span style={{ color: 'var(--accent-red)' }}>{cluster.percentage}% (N={cluster.errors_count})</span>
                      </div>
                      <div style={{ color: 'var(--text-muted)', fontSize: '11px', marginTop: '2px' }}>Pattern: {cluster.pattern}</div>
                    </div>
                  ))}
                  <div className="alert-item info" style={{ marginTop: '8px' }}>
                    <strong>Correlation Analysis:</strong> {clustering.data?.correlation_analysis}
                  </div>
                </div>
              )}

              {centerTab === 'Impact' && (
                <div style={{ height: '100%', display: 'flex', flexDirection: 'column', gap: '8px' }}>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '8px' }}>
                    {impact.data?.kpi_connections?.map((kpi, i) => (
                      <div key={i} className={`impact-card ${kpi.status === 'CRITICAL' ? 'crit' : kpi.status === 'WARN' ? 'warn' : 'ok'}`} style={{ borderLeftWidth: '3px' }}>
                        <span className="impact-kpi">{kpi.kpi_name}</span>
                        <span className="impact-val" style={{ color: kpi.status === 'CRITICAL' ? 'var(--accent-red)' : kpi.status === 'WARN' ? 'var(--accent-yellow)' : 'var(--accent-green)' }}>
                          -{kpi.impact_pct}%
                        </span>
                        <span className="impact-desc">${(kpi.monetary_loss_usd || 0).toLocaleString()} Loss</span>
                      </div>
                    ))}
                  </div>
                  <div style={{ padding: '8px', background: 'rgba(244,63,94,0.06)', border: '1px solid rgba(244,63,94,0.12)', borderRadius: '6px', fontSize: '12px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span>Cumulative Projected Business Financial Loss:</span>
                    <strong style={{ color: 'var(--accent-red)', fontSize: '14px', fontFamily: 'var(--font-mono)' }}>
                      ${(impact.data?.total_financial_impact_usd || 0).toLocaleString()} USD
                    </strong>
                  </div>
                </div>
              )}

              {centerTab === 'Actionable' && (
                <div style={{ overflowY: 'auto', height: '100%', display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  {recommendations.data?.recommendations?.map((rec) => {
                    const badgeMap = {
                      PENDING: 'pending',
                      RECOMMENDED: 'recommended',
                      AVAILABLE: 'available'
                    };
                    return (
                      <div key={rec.id} className="actionable-item">
                        <div className="actionable-meta">
                          <span className="actionable-title">{rec.title}</span>
                          <span className="actionable-desc">{rec.description}</span>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span className={`actionable-badge ${badgeMap[rec.status]}`}>{rec.status}</span>
                          <button className="btn-action" onClick={() => alert(`Executing action: ${rec.action_type}`)}>Run</button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>

        </div>

        {/* ================= COLUMN RIGHT ================= */}
        <div className="col-right">

          {/* Card 1: End-to-End Data Lineage Map */}
          <div className="glass-card animate-in">
            <div className="card-header">
              <div>
                <h3 className="card-title"><span className="icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ display: 'inline-block', verticalAlign: 'middle', marginTop: '-2px' }}><circle cx="12" cy="12" r="3" /><circle cx="19" cy="5" r="3" /><circle cx="5" cy="19" r="3" /><circle cx="19" cy="19" r="3" /><circle cx="5" cy="5" r="3" /><line x1="7.5" y1="7.5" x2="16.5" y2="16.5" /><line x1="16.5" y1="7.5" x2="7.5" y2="16.5" /></svg></span> Real-time Data Lineage Map</h3>
                <p className="card-subtitle">Real-time Data Flow & Quarantine Map</p>
              </div>
            </div>
            {(() => {
              const activeRun = selectedRun || (qualityHistory.data && qualityHistory.data[0]);
              const totalRecs = activeRun?.total_records || 0;
              const quarRecs = activeRun?.quarantined_records || 0;
              const hasError = activeRun && quarRecs > 0;
              const hasClean = activeRun && (totalRecs - quarRecs > 0);
              const isExecutionFailed = activeRun && (activeRun.quality_score === 0 || activeRun.quality_score === null);
              return (
            <div className="lineage-wrapper dynamic-lineage">
              <div className="lineage-path">
                {/* Node 1 */}
                <div className="lineage-node source">
                  <div className="node-icon">IN</div>
                  <div className="node-content">
                    <h4>{activeRun ? activeRun.data_source : 'Ingest Source'}</h4>
                    <span className="node-stat">{activeRun ? `${totalRecs.toLocaleString()} rows` : 'Waiting for data...'}</span>
                  </div>
                </div>

                <div className={`lineage-connector ${isExecutionFailed ? 'danger' : (activeRun ? 'active' : '')}`}></div>

                {/* Node 2 */}
                <div className="lineage-node audit">
                  <div className="node-icon">PR</div>
                  <div className="node-content">
                    <h4>Spark QA Audit</h4>
                    <span className="node-stat">Processing Engine</span>
                  </div>
                </div>

                <div className={`lineage-connector ${isExecutionFailed ? 'danger' : (activeRun ? 'active' : '')}`} style={{ flexGrow: 0, minWidth: '35px', width: '35px' }}></div>

                {/* Branches Container */}
                <div className="lineage-branches-container" style={{ display: 'flex', alignItems: 'center', flexShrink: 0, padding: '5px 0' }}>
                  {/* Left Fork (Split for error highlighting) */}
                  <div style={{ display: 'flex', flexDirection: 'column', width: '20px', height: '90px', minWidth: '20px', flexShrink: 0 }}>
                    <div style={{ height: '45px', borderLeft: '2px solid var(--accent-indigo)', borderTop: '2px solid var(--accent-indigo)', borderTopLeftRadius: '6px' }}></div>
                    <div style={{ height: '45px', borderLeft: `2px solid ${hasError ? 'var(--accent-red)' : 'var(--accent-indigo)'}`, borderBottom: `2px solid ${hasError ? 'var(--accent-red)' : 'var(--accent-indigo)'}`, borderBottomLeftRadius: '6px' }}></div>
                  </div>

                  {/* Branch Items */}
                  <div className="lineage-branches" style={{ display: 'flex', flexDirection: 'column', gap: '1.2rem', margin: '0 10px', flexShrink: 0 }}>
                    <div className={`lineage-node hdfs-active ${hasClean ? 'highlight' : ''}`}>
                      <div className="node-icon">OK</div>
                      <div className="node-content">
                        <h4>Active Store</h4>
                        <span className="node-stat">{activeRun ? `${(totalRecs - quarRecs).toLocaleString()} clean` : 'Verified Data'}</span>
                      </div>
                    </div>

                    <div className={`lineage-node hdfs-quarantine ${hasError ? 'highlight-danger' : ''}`}>
                      <div className="node-icon">QA</div>
                      <div className="node-content">
                        <h4>Quarantine</h4>
                        <span className="node-stat">{activeRun ? `${quarRecs.toLocaleString()} isolated` : 'Bad Data'}</span>
                      </div>
                    </div>
                  </div>

                  {/* Right Fork (Only top half connects Active Store to Serving) */}
                  <div style={{ display: 'flex', flexDirection: 'column', width: '20px', height: '90px', minWidth: '20px', flexShrink: 0 }}>
                    <div style={{ height: '45px', borderRight: '2px solid var(--accent-blue)', borderTop: '2px solid var(--accent-blue)', borderTopRightRadius: '6px' }}></div>
                    <div style={{ height: '45px', borderRight: '2px solid transparent', borderBottom: '2px solid transparent' }}></div>
                  </div>
                </div>

                <div className={`lineage-connector ${activeRun ? 'active' : ''}`} style={{ flexGrow: 0, minWidth: '35px', width: '35px' }}></div>

                {/* Node 4 */}
                <div className="lineage-node serving">
                  <div className="node-icon">API</div>
                  <div className="node-content">
                    <h4>Serving API</h4>
                    <span className="node-stat">BI & Analytics</span>
                  </div>
                </div>
              </div>
            </div>
            );
            })()}
          </div>


          {/* Card 2: Performance & Scalability */}
          <div className="glass-card animate-in">
            <div className="card-header">
              <div>
                <h3 className="card-title"><span className="icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ display: 'inline-block', verticalAlign: 'middle', marginTop: '-2px' }}><rect x="4" y="4" width="16" height="16" rx="2" /><rect x="9" y="9" width="6" height="6" /><line x1="9" y1="1" x2="9" y2="4" /><line x1="15" y1="1" x2="15" y2="4" /><line x1="9" y1="20" x2="9" y2="23" /><line x1="15" y1="20" x2="15" y2="23" /><line x1="20" y1="9" x2="23" y2="9" /><line x1="20" y1="15" x2="23" y2="15" /><line x1="1" y1="9" x2="4" y2="9" /><line x1="1" y1="15" x2="4" y2="15" /></svg></span> Performance & Scalability</h3>
                <p className="card-subtitle">Spark Cluster Performance Metrics</p>
              </div>

              <button
                className="btn-export"
                onClick={() => handleExportCSV([perf.data], "SDOQAP_Performance_Metrics")}
              >
                Export Metrics
              </button>
            </div>

            {perf.data ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', flexGrow: 1, justifyContent: 'center' }}>
                {/* Row 1: CPU and Memory */}
                <div style={{ display: 'flex', gap: '12px' }}>
                  <div className="stat-card" style={{ flex: 1, padding: '8px 12px' }}>
                    <span className="stat-label">CPU USAGE</span>
                    <span className="stat-value" style={{ color: 'var(--accent-blue)', fontSize: '18px', margin: '2px 0' }}>
                      {perf.data.current_cpu}%
                    </span>
                    <div style={{ width: '100%', height: '4px', background: '#E2E8F0', borderRadius: '2px', overflow: 'hidden', marginTop: '4px' }}>
                      <div style={{ width: `${perf.data.current_cpu}%`, height: '100%', background: 'var(--accent-blue)', borderRadius: '2px' }} />
                    </div>
                  </div>
                  <div className="stat-card" style={{ flex: 1, padding: '8px 12px' }}>
                    <span className="stat-label">MEMORY USAGE</span>
                    <span className="stat-value" style={{ color: 'var(--accent-purple)', fontSize: '18px', margin: '2px 0' }}>
                      {perf.data.current_memory}%
                    </span>
                    <div style={{ width: '100%', height: '4px', background: '#E2E8F0', borderRadius: '2px', overflow: 'hidden', marginTop: '4px' }}>
                      <div style={{ width: `${perf.data.current_memory}%`, height: '100%', background: 'var(--accent-purple)', borderRadius: '2px' }} />
                    </div>
                  </div>
                </div>

                {/* Row 2: Latency and SLA Limit */}
                <div style={{ display: 'flex', gap: '12px' }}>
                  <div className="stat-card" style={{ flex: 1, padding: '8px 12px' }}>
                    <span className="stat-label">AVG PROCESSING LATENCY</span>
                    <span className="stat-value" style={{ color: 'var(--accent-green)', fontSize: '18px', margin: '2px 0' }}>
                      {perf.data.average_latency_seconds}s
                    </span>
                    <div style={{ width: '100%', height: '4px', background: '#E2E8F0', borderRadius: '2px', overflow: 'hidden', marginTop: '4px' }}>
                      <div style={{ width: `${Math.min(100, (perf.data.average_latency_seconds / perf.data.sla_latency_limit_seconds) * 100)}%`, height: '100%', background: 'var(--accent-green)', borderRadius: '2px' }} />
                    </div>
                  </div>
                  <div className="stat-card" style={{ flex: 1, padding: '8px 12px' }}>
                    <span className="stat-label">SLA LATENCY LIMIT</span>
                    <span className="stat-value" style={{ color: 'var(--accent-red)', fontSize: '18px', margin: '2px 0' }}>
                      {perf.data.sla_latency_limit_seconds}s
                    </span>
                    <div style={{ width: '100%', height: '4px', background: '#E2E8F0', borderRadius: '2px', overflow: 'hidden', marginTop: '4px' }}>
                      <div style={{ width: '100%', height: '100%', background: 'var(--accent-red)', borderRadius: '2px' }} />
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <div className="loading-state"><span>Calculating resource usage...</span></div>
            )}
          </div>

          {/* Card 3: Activity Log & Distribution */}
          <div className="glass-card animate-in">
            <div className="card-header">
              <div>
                <h3 className="card-title"><span className="icon"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ display: 'inline-block', verticalAlign: 'middle', marginTop: '-2px' }}><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /><line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" /><polyline points="10 9 9 9 8 9" /></svg></span> Activity Log & Distribution</h3>
                <p className="card-subtitle">Pipeline Logs (Elasticsearch Stream)</p>
              </div>
              <button
                className="btn-export"
                onClick={() => handleExportCSV(activity.data || [], "SDOQAP_System_Logs")}
              >
                Export Logs
              </button>
            </div>

            <div className="terminal-window">
              <div className="terminal-header">
                <div className="term-controls">
                  <div className="term-dot term-close" />
                  <div className="term-dot term-min" />
                  <div className="term-dot term-max" />
                </div>
                <span className="terminal-title">sdoqap@observability-node:~</span>
              </div>

              <div className="terminal-body">
                {activity.loading ? (
                  <div className="log-line info">Connecting to Log Stream...</div>
                ) : activity.data && activity.data.length > 0 ? (
                  <>
                    {[...activity.data].reverse().map((act, i) => {
                      let logClass = 'info';
                      if (act.level === 'error') logClass = 'error';
                      else if (act.level === 'warning') logClass = 'warning';
                      else if (act.level === 'success') logClass = 'success';

                      return (
                        <div key={i} className={`log-line ${logClass}`}>
                          <span className="log-time">[{new Date(act.timestamp).toLocaleTimeString([], { hour12: false })}]</span>
                          <span>{act.message}</span>
                        </div>
                      );
                    })}
                    <div ref={terminalEndRef} />
                  </>
                ) : (
                  <div className="log-line error">ไม่มีประวัติข้อความ Log ล่าสุดพบในระบบ</div>
                )}
              </div>
            </div>
          </div>

        </div>

      </div>
    </div>
  );
}
