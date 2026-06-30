import React, { useState, useMemo, useEffect } from 'react';
import { useApi, postApi } from '../hooks/useApi';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export default function Dashboard() {
  // 1. Fetch real-time metrics from API endpoints
  const kpi = useApi('/kpi/stats', { refreshInterval: 15000 });
  const anomaly = useApi('/anomaly/sources', { refreshInterval: 15000 });
  const services = useApi('/services/status', { refreshInterval: 10000 });
  const activity = useApi('/system/activity?limit=15', { refreshInterval: 15000 });
  const perf = useApi('/performance/metrics', { refreshInterval: 15000 });
  const qualityHistory = useApi('/quality?limit=50', { refreshInterval: 15000 });
  const projection = useApi('/analytics/projection', { refreshInterval: 30000 });
  const clustering = useApi('/analytics/clustering', { refreshInterval: 30000 });
  const impact = useApi('/analytics/impact', { refreshInterval: 30000 });
  const recommendations = useApi('/analytics/recommendations', { refreshInterval: 30000 });

  // 2. Interactive States (Slicers & Filters)
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedSourceFilter, setSelectedSourceFilter] = useState('All');
  const [selectedRun, setSelectedRun] = useState(null);
  
  const [leftTab, setLeftTab] = useState('Ratio'); // Ratio, Quarantine, Insights
  const [centerTab, setCenterTab] = useState('Trends'); // Trends, RootCause, Impact, Actionable

  // Automatically select the first pipeline run if none is selected
  useEffect(() => {
    if (qualityHistory.data && qualityHistory.data.length > 0 && !selectedRun) {
      setSelectedRun(qualityHistory.data[0]);
    }
  }, [qualityHistory.data, selectedRun]);

  // 3. PowerBI Style CSV Export Utility
  const handleExportCSV = (jsonData, filename) => {
    if (!jsonData || !jsonData.length) {
      alert("ไม่มีข้อมูลสำหรับการดาวน์โหลด");
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

  // 4. Interactive Filters logic
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

  // 5. Data transformers for Recharts area visualization
  const qualityTrendData = useMemo(() => {
    if (!anomaly.data || !anomaly.data.timestamps) return [];
    return anomaly.data.timestamps.map((ts, i) => ({
      time: ts,
      API: anomaly.data.api ? anomaly.data.api[i] : 100,
      Database: anomaly.data.database ? anomaly.data.database[i] : 100,
      CSV: anomaly.data.csv ? anomaly.data.csv[i] : 100,
    }));
  }, [anomaly.data]);

  const forecastData = useMemo(() => {
    if (!projection.data || !projection.data.projection_days) return [];
    return projection.data.projection_days.map((day, i) => ({
      day: `Day +${day}`,
      Forecast: projection.data.projected_scores[i],
      High: projection.data.ci_high[i],
      Low: projection.data.ci_low[i],
    }));
  }, [projection.data]);

  // 6. Action triggering (Retry)
  const [retrying, setRetrying] = useState(false);
  const triggerPipelineRetry = async (runId) => {
    if (!runId) return;
    setRetrying(true);
    try {
      await postApi(`/pipeline/retry/${runId}`);
      alert("🔄 คัดลอกและเริ่มประมวลผลท่อข้อมูลใหม่อีกครั้งสำเร็จ!");
      qualityHistory.refetch();
      kpi.refetch();
      activity.refetch();
    } catch (err) {
      alert("❌ เกิดความล้มเหลวในการส่งข้อมูลเข้าระบบใหม่: " + err.message);
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
      {/* 1. Header Area */}
      <header className="sdoqap-header">
        <div className="logo-area">
          <h1>SDOQAP Observability Portal</h1>
          <p>ระบบสังเกตการณ์คุณภาพและความถูกต้องของข้อมูลขนาดใหญ่แบบรวมศูนย์</p>
        </div>
        
        {/* Service Hub Connection Pills */}
        <div className="service-hub">
          {services.data ? (
            Object.entries(services.data).map(([name, info]) => (
              <div key={name} className="service-card" title={info.url || 'Internal Port'}>
                <span className={`status-dot ${info.status === 'online' ? 'online' : 'offline'}`} />
                <span className="service-name">{name}</span>
              </div>
            ))
          ) : (
            <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>กำลังตรวจวัดสถานะบริการระบบ...</span>
          )}
          
          <div className={`overall-status ${services.error ? 'offline' : ''}`}>
            <span className={`status-dot ${services.error ? 'offline' : 'online'}`} />
            <span>{services.error ? 'SYSTEM CONNECTION LOST' : 'System Connection Active'}</span>
          </div>
        </div>
      </header>

      {/* 2. KPI Metrics Scorecard Row */}
      <div className="kpi-row">
        <div className="kpi-card blue animate-in">
          <div className="kpi-card-left">
            <span className="kpi-val">
              {kpi.data ? `${(kpi.data.total_records_ingested / 1000000).toFixed(2)}M` : '1.52M'}
            </span>
            <span className="kpi-label">TOTAL RECORDS INGESTED</span>
          </div>
          <span className="kpi-icon">📊</span>
        </div>
        <div className="kpi-card quality-kpi animate-in">
          <div className="kpi-card-left">
            <span className="kpi-val">
              {kpi.data ? `${kpi.data.global_quality_score}%` : '98.4%'}
            </span>
            <span className="kpi-label">GLOBAL QUALITY SCORE</span>
          </div>
          <span className="kpi-icon">🛡️</span>
        </div>
        <div className="kpi-card quarantine-kpi animate-in">
          <div className="kpi-card-left">
            <span className="kpi-val">
              {kpi.data ? (kpi.data.quarantined_records || 0).toLocaleString() : '24,320'}
            </span>
            <span className="kpi-label">QUARANTINED RECORDS</span>
          </div>
          <span className="kpi-icon">⚠️</span>
        </div>
        <div className="kpi-card amber animate-in">
          <div className="kpi-card-left">
            <span className="kpi-val">
              {kpi.data ? `${kpi.data.mttd_minutes} mins` : '2.4 mins'}
            </span>
            <span className="kpi-label">MTTD (MEAN TIME TO DETECT)</span>
          </div>
          <span className="kpi-icon">⏱️</span>
        </div>
      </div>

      {/* 3. Main Analytical Grid */}
      <div className="main-grid">
        
        {/* ================= COLUMN LEFT ================= */}
        <div className="col-left">
          
          {/* Card 1: Scorecard History */}
          <div className="glass-card animate-in">
            <div className="card-header">
              <div>
                <h3 className="card-title"><span className="icon">📋</span> Scorecard History</h3>
                <p className="card-subtitle">ประวัติการรันและคัดกรองข้อมูลจาก Elasticsearch</p>
              </div>
              
              <div className="powerbi-actions">
                <select 
                  className="filter-select"
                  value={selectedSourceFilter}
                  onChange={(e) => setSelectedSourceFilter(e.target.value)}
                >
                  <option value="All">ทุกแหล่งข้อมูล</option>
                  <option value="users">users (API)</option>
                  <option value="products">products (DB)</option>
                  <option value="mbti">mbti (CSV)</option>
                  <option value="sales_records">sales (CSV)</option>
                </select>
                <button 
                  className="btn-export" 
                  onClick={() => handleExportCSV(filteredRuns, "SDOQAP_Pipeline_Runs")}
                  title="Export to CSV สำหรับนำไปประมวลผลต่อใน Excel / Pandas"
                >
                  📥 Export
                </button>
              </div>
            </div>

            <div className="search-container">
              <span className="search-icon">🔍</span>
              <input 
                type="text" 
                placeholder="พิมพ์ค้นหา ID หรือตารางประมวลผลข้อมูล..." 
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
              />
            </div>

            <div className="table-wrapper">
              {qualityHistory.loading ? (
                <div className="loading-state"><span>กำลังดึงข้อมูลประวัติ...</span></div>
              ) : filteredRuns.length === 0 ? (
                <div className="loading-state" style={{ color: 'var(--text-muted)' }}>ไม่พบข้อมูลประวัติการตรวจสอบ</div>
              ) : (
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
                    {filteredRuns.map((run) => (
                      <tr 
                        key={run.run_id} 
                        className={`clickable-row ${selectedRun && selectedRun.run_id === run.run_id ? 'selected' : ''}`}
                        onClick={() => setSelectedRun(run)}
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
              )}
            </div>
          </div>

          {/* Card 2: Selected Run Analysis */}
          <div className="glass-card animate-in">
            <div className="card-header">
              <div>
                <h3 className="card-title"><span className="icon">🎯</span> Selected Run Analysis</h3>
                <p className="card-subtitle">ข้อมูลวิเคราะห์เชิงลึกและอัตราการกักกันข้อมูล</p>
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
                        <span className="stat-label">สถานะการคัดกรองข้อมูล</span>
                        <span className={`quality-badge ${getQualityBadgeClass(selectedRun.quality_score)}`} style={{ textAlign: 'center', marginTop: '4px' }}>
                          {selectedRun.quality_score >= 95 ? '🟢 HEALTHY PIPELINE' : selectedRun.quality_score >= 85 ? '🟡 WARNING DRIFT' : '🔴 CRITICAL ANOMALY'}
                        </span>
                      </div>
                    </div>
                  )}

                  {leftTab === 'Quarantine' && (
                    <div style={{ flexGrow: 1, overflowY: 'auto' }}>
                      <div style={{ fontSize: '11px', fontWeight: 'bold', marginBottom: '6px', color: 'var(--accent-red)' }}>สาเหตุการคัดกรองข้อมูลผิดกฎเข้า Quarantine:</div>
                      {selectedRun.quarantine_breakdown && Object.keys(selectedRun.quarantine_breakdown).length > 0 ? (
                        Object.entries(selectedRun.quarantine_breakdown).map(([reason, count]) => (
                          <div key={reason} style={{ fontSize: '11px', padding: '4px 6px', background: 'rgba(244,63,94,0.04)', border: '1px solid rgba(244,63,94,0.12)', borderRadius: '4px', marginBottom: '4px', display: 'flex', justifyContent: 'space-between' }}>
                            <span>❌ {reason}</span>
                            <span style={{ fontFamily: 'var(--font-mono)' }}>{(count || 0).toLocaleString()} แถว</span>
                          </div>
                        ))
                      ) : (
                        <div style={{ fontSize: '11px', color: 'var(--text-muted)', textAlign: 'center', marginTop: '10px' }}>ผ่านฉลุย 100% ไม่มีข้อมูลตกเกณฑ์</div>
                      )}
                    </div>
                  )}

                  {leftTab === 'Insights' && (
                    <div style={{ flexGrow: 1, overflowY: 'auto', fontSize: '11.5px', lineHeight: '1.45', color: 'var(--text-secondary)' }}>
                      {selectedRun.quality_score === 100 ? (
                        <div>🟢 ข้อมูลผ่านการตรวจอย่างสมบูรณ์ ข้อมูลสะอาดครบถ้วน ไม่มีปัญหา Type Mismatch หรือ Null Constraint ตรวจพบใน HDFS Active Store</div>
                      ) : (
                        <div>
                          ⚠️ พบคลิปเปอร์ข้อมูลเสียอัตรา {(((selectedRun.quarantined_records || 0) / (selectedRun.total_records || 1)) * 100).toFixed(1)}% (คิดเป็น <strong>{(selectedRun.quarantined_records || 0).toLocaleString()} rows</strong>)
                          ถูกกักแยกไว้ที่ HDFS Quarantine Store เพื่อป้องกันความสมบูรณ์ของท่อข้อมูลปลายทาง
                          <br /><br />
                          💡 <strong>AI Suggestion:</strong> ตรวจสอบว่าโครงสร้างคอลัมน์จาก API หรือ CSV ต้นทางมี Schema Drift หรือไม่
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
                      {retrying ? 'กำลังดำเนินการ...' : '🔄 Retry Ingest & Audit'}
                    </button>
                    <button 
                      className="btn-tab"
                      style={{ padding: '5px 10px' }}
                      onClick={() => handleExportCSV([selectedRun], `Selected_Run_${selectedRun.run_id}`)}
                    >
                      📥 Export
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
              <div className="loading-state"><span>กรุณาเลือกประวัติรอบการทำงานเพื่อตรวจสอบ</span></div>
            )}
          </div>

        </div>

        {/* ================= COLUMN CENTER ================= */}
        <div className="col-center">
          
          {/* Card 1: System Health (Quality Trend Chart) */}
          <div className="glass-card animate-in">
            <div className="card-header">
              <div>
                <h3 className="card-title"><span className="icon">🛡️</span> System Health: Data Quality Anomaly Detection</h3>
                <p className="card-subtitle">เทรนด์ดัชนีคะแนนตรวจสอบคุณภาพข้อมูลของแต่ละแหล่งส่งเข้าท่อระบายข้อมูล</p>
              </div>
              <button 
                className="btn-export" 
                onClick={() => handleExportCSV(qualityTrendData, "SDOQAP_Time_Series_Quality")}
              >
                📥 Export Trend
              </button>
            </div>

            <div style={{ flexGrow: 1, minHeight: 0, width: '100%', height: '100%' }}>
              {anomaly.loading ? (
                <div className="loading-state"><span>กำลังดึงสถิติ Anomaly...</span></div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={qualityTrendData} margin={{ top: 10, right: 10, left: -25, bottom: 0 }}>
                    <defs>
                      <linearGradient id="cApi" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#38bdf8" stopOpacity={0.15}/>
                        <stop offset="95%" stopColor="#38bdf8" stopOpacity={0}/>
                      </linearGradient>
                      <linearGradient id="cDb" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#10b981" stopOpacity={0.15}/>
                        <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                      </linearGradient>
                      <linearGradient id="cCsv" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#fbbf24" stopOpacity={0.15}/>
                        <stop offset="95%" stopColor="#fbbf24" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.03)" />
                    <XAxis dataKey="time" stroke="#64748b" tick={{ fontSize: 9.5 }} />
                    <YAxis domain={[0, 100]} stroke="#64748b" tick={{ fontSize: 9.5 }} />
                    <Tooltip contentClassName="custom-tooltip" />
                    <Area type="monotone" dataKey="API" stroke="var(--accent-blue)" fillOpacity={1} fill="url(#cApi)" strokeWidth={1.8} />
                    <Area type="monotone" dataKey="Database" stroke="var(--accent-green)" fillOpacity={1} fill="url(#cDb)" strokeWidth={1.8} />
                    <Area type="monotone" dataKey="CSV" stroke="var(--accent-yellow)" fillOpacity={1} fill="url(#cCsv)" strokeWidth={1.8} />
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

          {/* Card 2: Live Alerts Log */}
          <div className="glass-card animate-in" style={{ flex: 'none', height: '120px' }}>
            <div className="card-header">
              <div>
                <h3 className="card-title" style={{ color: 'var(--accent-red)' }}><span className="icon">🔔</span> Live Alerts Log</h3>
                <p className="card-subtitle">แจ้งเตือนความล้มเหลว โครงสร้างดริฟต์ และคอขวดระบบแบบเรียลไทม์</p>
              </div>
            </div>
            <div className="alerts-log-container">
              {anomaly.data && anomaly.data.anomaly ? (
                <>
                  <div className="alert-item">
                    <span className="alert-time">[{anomaly.data.anomaly.time}]</span>
                    <span>⚠️ <strong>[ดริฟต์ขัดข้อง - {anomaly.data.anomaly.source}]</strong> {anomaly.data.anomaly.reason} (คะแนนตกลงที่ {anomaly.data.anomaly.score}%)</span>
                  </div>
                  <div className="alert-item info">
                    <span className="alert-time">[{anomaly.data.anomaly.time}]</span>
                    <span>ℹ️ <strong>System Automation:</strong> ดำเนินการแยกข้อมูลดิบที่มีปัญหาลง HDFS Quarantine โดยไม่หยุดระบบหลัก</span>
                  </div>
                </>
              ) : (
                <div className="alert-item info">
                  <span className="alert-time">[สตรีมปกติ]</span>
                  <span>🟢 ข้อมูลไหลเข้าอย่างสม่ำเสมอ ไม่มีการละเมิดเกณฑ์คุณลักษณะหรือโครงสร้างข้อมูลดริฟต์</span>
                </div>
              )}
            </div>
          </div>

          {/* Card 3: SDOQAP Analytical Intelligence Blueprint */}
          <div className="glass-card animate-in">
            <div className="card-header" style={{ paddingBottom: '0', borderBottom: 'none' }}>
              <div>
                <h3 className="card-title"><span className="icon">🧠</span> Global Analytical Blueprint</h3>
                <p className="card-subtitle">ผลวิเคราะห์เชิงลึกระดับองค์รวมจากทุก Data Pipeline</p>
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
                📥 Export Analytics
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
                      <div className="loading-state"><span>กำลังประมวลผลโมเดลทำนาย...</span></div>
                    ) : (
                      <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={forecastData} margin={{ top: 5, right: 5, left: -25, bottom: 0 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.02)" />
                          <XAxis dataKey="day" stroke="#64748b" tick={{ fontSize: 9 }} />
                          <YAxis domain={[75, 100]} stroke="#64748b" tick={{ fontSize: 9 }} />
                          <Tooltip contentClassName="custom-tooltip" />
                          <Area type="monotone" dataKey="Optimistic" stroke="#10b981" fill="none" strokeWidth={1} strokeDasharray="3 3" />
                          <Area type="monotone" dataKey="Pessimistic" stroke="#f43f5e" fill="none" strokeWidth={1} strokeDasharray="3 3" />
                          <Area type="monotone" dataKey="Forecast" stroke="#3b82f6" fill="rgba(59,130,246,0.05)" strokeWidth={2} />
                        </AreaChart>
                      </ResponsiveContainer>
                    )}
                  </div>
                  <div style={{ fontSize: '11px', display: 'flex', flexDirection: 'column', gap: '6px', overflowY: 'auto' }}>
                    <div style={{ borderBottom: '1px solid rgba(255,255,255,0.05)', paddingBottom: '4px', fontWeight: 'bold', color: 'var(--accent-blue)' }}>🔮 พยากรณ์ระดับคุณภาพ 7 วัน:</div>
                    <div>• <strong>ดัชนีความเสถียรข้อมูล:</strong> <span style={{ color: 'var(--accent-green)' }}>{projection.data?.stability_index || '78.4%'}</span></div>
                    <div>• <strong>โอกาสเสียเกณฑ์ SLA:</strong> <span style={{ color: 'var(--accent-red)' }}>{projection.data?.sla_breach_probability || '45.2%'}</span></div>
                    <div style={{ color: 'var(--text-muted)', fontSize: '10.5px' }}>{projection.data?.historical_trend}</div>
                    
                    {projection.data?.crisis_forecast && projection.data.crisis_forecast.severity !== 'LOW' && (
                      <div className="bp-alert">
                        <strong>Predicted Quality Crisis Alert</strong>
                        <div>วิกฤตคุณภาพใน {projection.data.crisis_forecast.days_until_crisis} วันที่ส่วน "{projection.data.crisis_forecast.impacted_component}"</div>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {centerTab === 'RootCause' && (
                <div style={{ overflowY: 'auto', height: '100%', fontSize: '11.5px' }}>
                  <div style={{ fontWeight: 'bold', marginBottom: '6px', color: 'var(--accent-purple)' }}>วิเคราะห์จับกลุ่มปัญหา (Error Pattern Clustering):</div>
                  {clustering.loading ? (
                    <div>กำลังจัดกลุ่มสาเหตุหลัก...</div>
                  ) : clustering.data?.clusters?.map((cluster) => (
                    <div key={cluster.id} style={{ padding: '6px', background: 'rgba(255,255,255,0.02)', border: '1px solid rgba(255,255,255,0.05)', borderRadius: '6px', marginBottom: '6px' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontWeight: 'bold' }}>
                        <span>แหล่งที่มา: {cluster.source}</span>
                        <span style={{ color: 'var(--accent-red)' }}>{cluster.percentage}% (N={cluster.errors_count})</span>
                      </div>
                      <div style={{ color: 'var(--text-muted)', fontSize: '11px', marginTop: '2px' }}>ลักษณะข้อเสีย: {cluster.pattern}</div>
                    </div>
                  ))}
                  <div className="alert-item info" style={{ marginTop: '8px' }}>
                    💡 <strong>สหสัมพันธ์เชิงลึก (Correlation):</strong> {clustering.data?.correlation_analysis}
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
                    <span>ความสูญเสียทางการเงินเชิงธุรกิจสะสมล่วงหน้า:</span>
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
                          <button className="btn-action" onClick={() => alert(`ดำเนินการคำสั่งฟังก์ชัน: ${rec.action_type}`)}>Run</button>
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
                <h3 className="card-title"><span className="icon">🕸️</span> Real-time Data Lineage Map</h3>
                <p className="card-subtitle">ตรวจสอบทิศทางของข้อมูลและจุดกักกันแบบเรียลไทม์ (Auto-sync)</p>
              </div>
            </div>
            {(() => {
              const activeRun = selectedRun || (qualityHistory.data && qualityHistory.data[0]);
              const totalRecs = activeRun?.total_records || 0;
              const quarRecs = activeRun?.quarantined_records || 0;
              const hasError = activeRun && quarRecs > 0;
              const hasClean = activeRun && (totalRecs - quarRecs > 0);
              return (
            <div className="lineage-wrapper dynamic-lineage">
              <div className="lineage-path">
                {/* Node 1 */}
                <div className="lineage-node source">
                  <div className="node-icon">📡</div>
                  <div className="node-content">
                    <h4>{activeRun ? activeRun.data_source : 'Ingest Source'}</h4>
                    <span className="node-stat">{activeRun ? `${totalRecs.toLocaleString()} rows` : 'Waiting for data...'}</span>
                  </div>
                </div>
                
                <div className={`lineage-connector ${hasError ? 'danger' : (activeRun ? 'active' : '')}`}></div>

                {/* Node 2 */}
                <div className="lineage-node audit">
                  <div className="node-icon">⚡</div>
                  <div className="node-content">
                    <h4>Spark QA Audit</h4>
                    <span className="node-stat">Processing Engine</span>
                  </div>
                </div>

                <div className={`lineage-connector ${hasError ? 'danger' : (activeRun ? 'active' : '')}`} style={{ flexGrow: 0, minWidth: '35px', width: '35px' }}></div>

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
                      <div className="node-icon">✅</div>
                      <div className="node-content">
                        <h4>Active Store</h4>
                        <span className="node-stat">{activeRun ? `${(totalRecs - quarRecs).toLocaleString()} clean` : 'Verified Data'}</span>
                      </div>
                    </div>
                    
                    <div className={`lineage-node hdfs-quarantine ${hasError ? 'highlight-danger' : ''}`}>
                      <div className="node-icon">🚨</div>
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
                  <div className="node-icon">📊</div>
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
                <h3 className="card-title"><span className="icon">⚡</span> Performance & Scalability</h3>
                <p className="card-subtitle">ประสิทธิภาพการประมวลผลและการใช้ทรัพยากร Spark Cluster</p>
              </div>
              
              <button 
                className="btn-export" 
                onClick={() => handleExportCSV([perf.data], "SDOQAP_Performance_Metrics")}
              >
                📥 Export Metrics
              </button>
            </div>
            
            {perf.data ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', flexGrow: 1, justifyContent: 'center' }}>
                <div style={{ display: 'flex', gap: '12px' }}>
                  <div className="stat-card" style={{ flex: 1 }}>
                    <span className="stat-label">CPU USAGE</span>
                    <span className="stat-value" style={{ color: 'var(--accent-blue)' }}>{perf.data.current_cpu}%</span>
                  </div>
                  <div className="stat-card" style={{ flex: 1 }}>
                    <span className="stat-label">SLA LATENCY LIMIT</span>
                    <span className="stat-value" style={{ color: 'var(--accent-red)' }}>{perf.data.sla_latency_limit_seconds}s</span>
                  </div>
                </div>
                <div className="stat-card">
                  <span className="stat-label">AVG PROCESSING LATENCY</span>
                  <span className="stat-value" style={{ color: 'var(--accent-green)' }}>{perf.data.average_latency_seconds} seconds</span>
                </div>
              </div>
            ) : (
              <div className="loading-state"><span>กำลังคำนวณทรัพยากร...</span></div>
            )}
          </div>

          {/* Card 3: Activity Log & Distribution */}
          <div className="glass-card animate-in">
            <div className="card-header">
              <div>
                <h3 className="card-title"><span className="icon">💻</span> Activity Log & Distribution</h3>
                <p className="card-subtitle">ประวัติ Log ล่าสุดของ Pipeline ดึงตรงจาก Elasticsearch</p>
              </div>
              <button 
                className="btn-export" 
                onClick={() => handleExportCSV(activity.data || [], "SDOQAP_System_Logs")}
              >
                📥 Export Logs
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
                  <div className="log-line info">กำลังเชื่อมต่อ Log Stream...</div>
                ) : activity.data && activity.data.length > 0 ? (
                  activity.data.map((act, i) => {
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
                  })
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
