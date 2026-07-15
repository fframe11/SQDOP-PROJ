import React, { useState } from 'react';
import { useApi, postApi } from '../hooks/useApi';

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
    if (state === 'success') return <span className="badge badge-success">SUCCESS</span>;
    if (state === 'failed') return <span className="badge badge-danger">FAILED</span>;
    return <span className="badge badge-warning">QUARANTINED</span>;
  };

  const getScoreColor = (score) => {
    if (score >= 95) return '#10b981';
    if (score >= 85) return '#f59e0b';
    return '#f43f5e';
  };

  return (
    <div className="page-container">
      
      {/* Header & Gold Layer Rebuild Panel side-by-side */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '1.5rem', marginBottom: '2rem', flexWrap: 'wrap', width: "100%" }}>
        <div style={{ margin: 0, flex: 1, minWidth: '300px' }}>
          <div style={{ fontSize: "11px", color: "var(--text-muted)", marginBottom: "0.4rem", fontFamily: "var(--font-sans)", display: "flex", gap: "6px", alignItems: "center" }}>
            <span>SDOQAP Data Engine</span>
            <span style={{ opacity: 0.5 }}>&gt;</span>
            <span style={{ color: "var(--text-main)", fontWeight: 500 }}>Pipeline Runs</span>
          </div>
          <h1 style={{ fontSize: "1.6rem", fontWeight: 700, color: "var(--text-main)", letterSpacing: "-0.02em", margin: 0 }}>Pipeline Runs</h1>
          <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem', margin: "4px 0 0 0" }}>Monitor and manage Data Pipeline Runs &amp; Business Aggregations</p>
        </div>
        
        {/* Gold Layer Aggregation Rebuild panel */}
        <div 
          className="glass-card animate-in" 
          style={{ 
            margin: 0, 
            padding: '1.25rem', 
            background: '#FFFFFF', 
            border: '1px solid #E2E8F0', 
            borderRadius: '12px',
            maxWidth: '480px',
            flex: 1,
            minWidth: '300px',
            boxShadow: '0 4px 20px rgba(15, 23, 42, 0.02)'
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: '1rem' }}>
            <div>
              <h3 style={{ fontSize: '0.95rem', fontWeight: 700, margin: 0, color: 'var(--text-main)', display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
                Gold Layer Aggregation
              </h3>
              <p style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.25rem', lineHeight: 1.4 }}>
                Pre-aggregate Silver active storage into business-ready gold indices for BI dashboard performance.
              </p>
            </div>
            
            <button
              onClick={handleGoldRebuild}
              disabled={goldRebuilding}
              style={{
                background: 'rgba(108, 71, 255, 0.08)',
                border: '1px solid rgba(108, 71, 255, 0.35)',
                borderRadius: '6px',
                color: '#6C47FF',
                padding: '0.5rem 0.85rem',
                fontSize: '0.78rem',
                fontWeight: 600,
                cursor: 'pointer',
                transition: 'all 0.2s ease',
                whiteSpace: 'nowrap'
              }}
            >
              {goldRebuilding ? 'Rebuilding...' : 'Rebuild Gold'}
            </button>
          </div>
          
          {goldResult && (
            <div 
              style={{ 
                marginTop: '0.75rem', 
                padding: '0.4rem 0.6rem', 
                borderRadius: '4px', 
                fontSize: '0.72rem', 
                background: goldResult.success ? 'rgba(16, 185, 129, 0.1)' : 'rgba(244, 63, 94, 0.1)',
                border: `1px solid ${goldResult.success ? 'rgba(16, 185, 129, 0.3)' : 'rgba(244, 63, 94, 0.3)'}`,
                color: goldResult.success ? '#10b981' : '#f43f5e'
              }}
            >
              {goldResult.message}
            </div>
          )}
        </div>
      </div>

      {retryResult && (
        <div className={`alert-box ${retryResult.success ? 'info' : 'critical'}`} style={{ marginBottom: '1.25rem' }}>
          <span>{retryResult.message}</span>
        </div>
      )}

      {/* Pipeline Runs Table */}
      <div className="glass-card animate-in" style={{ marginBottom: '1.5rem', overflowX: 'auto' }}>
        <h3 className="section-title">Pipeline Runs</h3>
        <p className="section-subtitle">Pipeline execution history and status</p>
        
        {pipeline.loading ? (
          <div className="loading-state">
            <div className="loading-spinner"></div>
            <span>Loading Pipeline Runs...</span>
          </div>
        ) : pipeline.error ? (
          <div className="alert-box critical">Failed to fetch Pipeline history</div>
        ) : (
          <table className="data-table">
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
                  <td style={{ fontFamily: 'var(--font-mono)', fontSize: '0.75rem', fontWeight: 600 }}>{run.run_id}</td>
                  <td>{run.table_name}</td>
                  <td>{getStatusBadge(run.state)}</td>
                  <td style={{ fontFamily: 'var(--font-mono)' }}>{run.duration_seconds ? run.duration_seconds.toFixed(2) : '-'}</td>
                  <td style={{ fontSize: '0.75rem' }}>{new Date(run.timestamp).toLocaleString()}</td>
                  <td>
                    {(run.state === 'failed' || run.state === 'quarantined') && (
                      <button
                        className="btn btn-secondary btn-sm"
                        style={{
                          backgroundColor: run.state === 'quarantined' ? 'rgba(245, 158, 11, 0.1)' : undefined,
                          borderColor: run.state === 'quarantined' ? 'rgba(245, 158, 11, 0.4)' : undefined,
                          color: run.state === 'quarantined' ? 'var(--accent-yellow)' : 'var(--accent-red)',
                          padding: '0.35rem 0.75rem'
                        }}
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

      {/* Quality Audit History Table */}
      <div className="glass-card animate-in" style={{ overflowX: 'auto' }}>
        <h3 className="section-title">Quality Audit Log</h3>
        <p className="section-subtitle">Quality Audit execution history and Quality Score</p>

        {quality.loading ? (
          <div className="loading-state">
            <div className="loading-spinner"></div>
            <span>Loading Quality Audit results...</span>
          </div>
        ) : quality.error ? (
          <div className="alert-box critical">Failed to fetch Quality Audit history</div>
        ) : (
          <table className="data-table">
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
                  <td style={{ fontFamily: 'var(--font-mono)' }}>{audit.total_records.toLocaleString()}</td>
                  <td style={{ fontFamily: 'var(--font-mono)' }}>{audit.clean_records.toLocaleString()}</td>
                  <td style={{ fontFamily: 'var(--font-mono)', color: audit.quarantined_records > 0 ? 'var(--accent-rose)' : 'inherit' }}>
                    {audit.quarantined_records.toLocaleString()}
                  </td>
                  <td>
                    <span style={{ color: getScoreColor(audit.quality_score), fontWeight: 700, fontFamily: 'var(--font-mono)' }}>
                      {audit.quality_score.toFixed(1)}%
                    </span>
                    <div className="progress-bar">
                      <div className="progress-fill" style={{ width: `${audit.quality_score}%`, backgroundColor: getScoreColor(audit.quality_score) }} />
                    </div>
                  </td>
                  <td style={{ fontSize: '0.75rem' }}>{new Date(audit.timestamp).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
