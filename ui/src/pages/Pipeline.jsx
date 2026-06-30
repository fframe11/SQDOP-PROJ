import React, { useState } from 'react';
import { useApi, postApi } from '../hooks/useApi';

export default function Pipeline() {
  const pipeline = useApi('/pipeline?limit=20', { refreshInterval: 30000 });
  const quality = useApi('/quality?limit=20', { refreshInterval: 30000 });

  const [retrying, setRetrying] = useState({});
  const [retryResult, setRetryResult] = useState(null);

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
      <div className="page-header">
        <h1>Pipeline Management</h1>
        <p>Monitor and manage Data Pipeline Runs</p>
      </div>

      {retryResult && (
        <div className={`alert-box ${retryResult.success ? 'info' : 'critical'}`} style={{ marginBottom: '1rem' }}>
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
                    {run.state === 'failed' && (
                      <button
                        className="btn btn-secondary btn-sm btn-danger"
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
