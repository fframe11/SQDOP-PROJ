import React, { useState } from 'react';
import { useApi, postApi } from '../hooks/useApi';
import "./Pipeline.css";

export default function Pipeline() {
  const pipeline = useApi('/pipeline?limit=20', { refreshInterval: 30000 });
  const quality = useApi('/quality?limit=20', { refreshInterval: 30000 });

  const [retrying, setRetrying] = useState({});
  const [retryResult, setRetryResult] = useState(null);
  
  // Gold Layer Rebuild State
  const [goldRebuilding, setGoldRebuilding] = useState(false);
  const [goldResult, setGoldResult] = useState(null);

  const handleRetry = async (runId) => {
    setRetrying(prev => ({ ...prev, [runId]: true }));
    setRetryResult(null);
    try {
      const res = await postApi(`/pipeline/retry/${runId}`);
      setRetryResult({ success: true, message: `Retry triggered successfully (New Run ID: ${res.new_run_id || 'N/A'})` });
      // Refresh listings
      pipeline.refetch();
      quality.refetch();
    } catch (err) {
      setRetryResult({ success: false, message: `Failed to trigger retry: ${err.message}` });
    } finally {
      setRetrying(prev => ({ ...prev, [runId]: false }));
    }
  };

  const handleGoldRebuild = async () => {
    setGoldRebuilding(true);
    setGoldResult(null);
    try {
      const res = await postApi('/gold/rebuild');
      setGoldResult({ success: true, message: res.message || 'Gold Layer rebuild triggered successfully in the background.' });
    } catch (err) {
      setGoldResult({ success: false, message: `Failed to rebuild Gold Layer: ${err.message}` });
    } finally {
      setGoldRebuilding(false);
    }
  };

  const getStatusBadge = (state) => {
    if (state === 'success') return <span className="gs-badge" style={{ background: '#d1fae5', color: '#059669' }}>SUCCESS</span>;
    if (state === 'failed') return <span className="gs-badge" style={{ background: '#fee2e2', color: '#dc2626' }}>FAILED</span>;
    return <span className="gs-badge" style={{ background: '#fff7ed', color: '#d97706' }}>QUARANTINED</span>;
  };

  const getScoreColor = (score) => {
    if (score >= 95) return '#10b981';
    if (score >= 85) return '#f59e0b';
    return '#f43f5e';
  };

  return (
    <div className="gs-pipeline">
      
      {/* Header & Gold Layer Rebuild Panel */}
      <div className="gs-page-header">
        <div style={{ flex: 1, minWidth: '300px' }}>
          <h1 className="gs-page-title">Pipeline <span>Run Control</span></h1>
          <p className="gs-page-desc">Monitor historical ingestion states and trigger medallion aggregations</p>
        </div>
        
        {/* Gold Layer Aggregation Rebuild panel */}
        <div className="gs-gold-panel">
          <div className="gs-gold-info" style={{ flex: 1, minWidth: '200px' }}>
            <h4>Gold Layer Aggregation</h4>
            <p>Re-aggregate Silver active delta tables into pre-aggregated gold tables.</p>
          </div>
          <button
            onClick={handleGoldRebuild}
            disabled={goldRebuilding}
            className="gs-btn-primary gs-btn-sm"
          >
            {goldRebuilding ? 'Rebuilding...' : 'Rebuild Gold'}
          </button>
          
          {goldResult && (
            <div 
              style={{ 
                width: '100%',
                marginTop: '8px', 
                padding: '6px 10px', 
                borderRadius: '6px', 
                fontSize: '11px', 
                background: goldResult.success ? '#d1fae5' : '#fee2e2',
                border: `1px solid ${goldResult.success ? '#10b981' : '#ef4444'}`,
                color: goldResult.success ? '#059669' : '#dc2626',
                fontFamily: 'var(--font-mono)'
              }}
            >
              {goldResult.message}
            </div>
          )}
        </div>
      </div>

      {retryResult && (
        <div 
          style={{ 
            padding: '10px 14px', 
            borderRadius: '8px', 
            fontSize: '12px', 
            background: retryResult.success ? '#eff6ff' : '#fee2e2',
            border: `1px solid ${retryResult.success ? '#3b82f6' : '#ef4444'}`,
            color: retryResult.success ? '#1d4ed8' : '#dc2626',
            fontFamily: 'var(--font-mono)'
          }}
        >
          {retryResult.message}
        </div>
      )}

      <div className="gs-pipeline-grid">
        {/* Pipeline Runs Table */}
        <div className="gs-pcard">
          <h3>Recent Pipeline Ingestion Runs</h3>
          <div className="gs-ptable-wrap">
            {pipeline.loading ? (
              <div className="gs-empty-cell">Fetching pipeline executions...</div>
            ) : pipeline.error ? (
              <div className="gs-empty-cell" style={{ color: 'var(--accent-red)' }}>Failed to load pipeline executions</div>
            ) : (
              <table className="gs-ptable">
                <thead>
                  <tr>
                    <th>Run ID</th>
                    <th>Table Name</th>
                    <th>Status</th>
                    <th>Duration (s)</th>
                    <th>Timestamp</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {pipeline.data && pipeline.data.map((run) => (
                    <tr key={run.run_id}>
                      <td className="gs-mono" style={{ fontWeight: 700 }}>{run.run_id}</td>
                      <td><strong>{run.table_name}</strong></td>
                      <td>{getStatusBadge(run.state)}</td>
                      <td className="gs-mono">{run.duration_seconds ? run.duration_seconds.toFixed(2) : '-'}</td>
                      <td className="gs-muted">{new Date(run.timestamp).toLocaleString()}</td>
                      <td>
                        {(run.state === 'failed' || run.state === 'quarantined') && (
                          <button
                            className="gs-btn-retry"
                            disabled={retrying[run.run_id]}
                            onClick={() => handleRetry(run.run_id)}
                          >
                            {retrying[run.run_id] ? 'Retrying...' : 'Retry'}
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        {/* Quality Audit History Table */}
        <div className="gs-pcard">
          <h3>Quality Audit Logs</h3>
          <div className="gs-ptable-wrap">
            {quality.loading ? (
              <div className="gs-empty-cell">Fetching quality audits...</div>
            ) : quality.error ? (
              <div className="gs-empty-cell" style={{ color: 'var(--accent-red)' }}>Failed to load quality audits</div>
            ) : (
              <table className="gs-ptable">
                <thead>
                  <tr>
                    <th>Table Name</th>
                    <th>Total Records</th>
                    <th>Clean</th>
                    <th>Quarantined</th>
                    <th>Quality Score</th>
                    <th>Timestamp</th>
                  </tr>
                </thead>
                <tbody>
                  {quality.data && quality.data.map((audit, i) => (
                    <tr key={i}>
                      <td><strong>{audit.table_name}</strong></td>
                      <td className="gs-mono">{audit.total_records.toLocaleString()}</td>
                      <td className="gs-mono">{audit.clean_records.toLocaleString()}</td>
                      <td className="gs-mono" style={{ color: audit.quarantined_records > 0 ? 'var(--accent-red)' : 'inherit', fontWeight: audit.quarantined_records > 0 ? 700 : 'normal' }}>
                        {audit.quarantined_records.toLocaleString()}
                      </td>
                      <td>
                        <div className="gs-score-cell">
                          <span style={{ color: getScoreColor(audit.quality_score), fontWeight: 700 }} className="gs-mono">
                            {audit.quality_score.toFixed(1)}%
                          </span>
                          <div className="gs-score-bar-bg">
                            <div className="gs-score-bar" style={{ width: `${audit.quality_score}%`, backgroundColor: getScoreColor(audit.quality_score) }} />
                          </div>
                        </div>
                      </td>
                      <td className="gs-muted">{new Date(audit.timestamp).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
