import React from 'react';
import { useApi } from '../hooks/useApi';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, Cell } from 'recharts';

export default function Analytics() {
  const projection = useApi('/analytics/projection', { refreshInterval: 60000 });
  const clustering = useApi('/analytics/clustering', { refreshInterval: 60000 });
  const impact = useApi('/analytics/impact', { refreshInterval: 60000 });
  const recommendations = useApi('/analytics/recommendations', { refreshInterval: 60000 });

  // Transform projection data
  const projectionData = React.useMemo(() => {
    if (!projection.data || !projection.data.projection_days) return [];
    return projection.data.projection_days.map((d, i) => ({
      day: `Day ${d}`,
      Score: projection.data.projected_scores[i],
      High: projection.data.ci_high[i],
      Low: projection.data.ci_low[i],
    }));
  }, [projection.data]);

  // Transform clustering data
  const clusteringData = React.useMemo(() => {
    if (!clustering.data || !clustering.data.clusters) return [];
    return clustering.data.clusters.map(c => ({
      name: c.source,
      pattern: c.pattern,
      count: c.errors_count,
      pct: c.percentage,
    }));
  }, [clustering.data]);

  const clusterColors = ['#f43f5e', '#f59e0b', '#3b82f6', '#8b5cf6', '#64748b'];

  return (
    <div className="page-container">
      <div className="page-header">
        <h1>Analytics</h1>
        <p>Data Quality Analysis, Prediction & Business Impact</p>
      </div>

      {/* Quality Projection */}
      <div className="chart-container animate-in" style={{ marginBottom: '1.5rem' }}>
        <h3 className="chart-title">7-Day Quality Forecast</h3>
        {projection.loading ? (
          <div className="loading-state">
            <div className="loading-spinner"></div>
            <span>Running predictive model...</span>
          </div>
        ) : projection.error ? (
          <div className="alert-box critical">Failed to process predictive model data</div>
        ) : projection.data ? (
          <>
            <div style={{ width: '100%', height: 260 }}>
              <ResponsiveContainer>
                <AreaChart data={projectionData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                  <defs>
                    <linearGradient id="colorConfidence" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.15}/>
                      <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.02}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                  <XAxis dataKey="day" stroke="#64748b" tick={{ fontSize: 11 }} />
                  <YAxis domain={[75, 100]} stroke="#64748b" tick={{ fontSize: 11 }} />
                  <Tooltip contentClassName="custom-tooltip" />
                  {/* Confidence Interval Band */}
                  <Area type="monotone" dataKey="High" stroke="none" fill="url(#colorConfidence)" />
                  <Area type="monotone" dataKey="Low" stroke="none" fill="#0a0e1a" />
                  {/* Forecast Line */}
                  <Area type="monotone" dataKey="Score" stroke="#3b82f6" fill="none" strokeWidth={2.5} />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            <div className="stats-bar" style={{ margin: '1rem 0 0 0' }}>
              <div className="stat-item">
                <div className="stat-value" style={{ color: '#10b981' }}>{projection.data.stability_index}</div>
                <div className="stat-label">Data Stability Index</div>
              </div>
              <div className="stat-item">
                <div className="stat-value" style={{ color: '#f43f5e' }}>{projection.data.sla_breach_probability}</div>
                <div className="stat-label">SLA Breach Probability</div>
              </div>
            </div>

            <p style={{ marginTop: '0.75rem', fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
              <strong>Historical Trend:</strong> {projection.data.historical_trend}
            </p>

            {projection.data.crisis_forecast && projection.data.crisis_forecast.severity !== 'LOW' && (
              <div className={`alert-box ${projection.data.crisis_forecast.severity === 'CRITICAL' ? 'critical' : 'warning'}`} style={{ marginTop: '1rem' }}>
                <span>
                  <strong>Predictive Alert ({projection.data.crisis_forecast.severity}):</strong> System predicts quality will drop below 90% in <strong>{projection.data.crisis_forecast.days_until_crisis} days</strong> on <em>{projection.data.crisis_forecast.impacted_component}</em>. Reason: {projection.data.crisis_forecast.reason}
                </span>
              </div>
            )}
          </>
        ) : null}
      </div>

      <div className="dash-grid">
        
        {/* Error Pattern Clustering */}
        <div className="glass-card animate-in">
          <h3 className="section-title">Error Pattern Clustering</h3>
          <p className="section-subtitle">Frequent Pipeline Error Patterns</p>
          {clustering.loading ? (
            <div className="loading-state">
              <div className="loading-spinner"></div>
              <span>Clustering errors...</span>
            </div>
          ) : clustering.error ? (
            <div className="alert-box critical">Failed to cluster errors</div>
          ) : clusteringData.length > 0 ? (
            <>
              <div style={{ width: '100%', height: 200 }}>
                <ResponsiveContainer>
                  <BarChart data={clusteringData} layout="vertical" margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                    <XAxis type="number" stroke="#64748b" tick={{ fontSize: 11 }} />
                    <YAxis type="category" dataKey="name" stroke="#64748b" tick={{ fontSize: 10 }} width={70} />
                    <Tooltip contentClassName="custom-tooltip" />
                    <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                      {clusteringData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={clusterColors[index % clusterColors.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
              
              <div style={{ marginTop: '1rem' }}>
                {clustering.data.clusters.map((c, i) => (
                  <div key={i} style={{ fontSize: '0.78rem', marginBottom: '0.5rem', display: 'flex', justifyContent: 'space-between', borderBottom: '1px solid rgba(255,255,255,0.04)', paddingBottom: '0.25rem' }}>
                    <span style={{ color: clusterColors[i % clusterColors.length] }}>● <strong>{c.source}</strong>: {c.pattern}</span>
                    <span style={{ color: 'var(--text-secondary)' }}>{c.errors_count.toLocaleString()} events ({c.percentage}%)</span>
                  </div>
                ))}
              </div>

              <div className="alert-box info" style={{ marginTop: '1rem', fontSize: '0.75rem' }}>
                <strong>Correlation Analysis:</strong> {clustering.data.correlation_analysis}
              </div>
            </>
          ) : null}
        </div>

        {/* Business KPI Impact */}
        <div className="glass-card animate-in">
          <h3 className="section-title">Business KPI Impact Analysis</h3>
          <p className="section-subtitle">Business KPI Impact Assessment</p>
          {impact.loading ? (
            <div className="loading-state">
              <div className="loading-spinner"></div>
              <span>Assessing business impact...</span>
            </div>
          ) : impact.error ? (
            <div className="alert-box critical">Failed to assess business impact</div>
          ) : impact.data ? (
            <>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                {impact.data.kpi_connections.map((kpi, i) => {
                  const badgeMap = {
                    OK: 'badge-success',
                    WARN: 'badge-warning',
                    CRITICAL: 'badge-danger'
                  };
                  return (
                    <div key={i} style={{ padding: '0.75rem', background: 'rgba(255,255,255,0.02)', borderRadius: '8px', border: '1px solid var(--border-glass)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <div>
                        <div style={{ fontSize: '0.85rem', fontWeight: 600 }}>{kpi.kpi_name}</div>
                        <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '0.15rem' }}>
                          Impact: {kpi.impact_pct}% | Estimated Loss: <span style={{ color: kpi.monetary_loss_usd > 0 ? 'var(--accent-rose)' : 'inherit' }}>${kpi.monetary_loss_usd.toLocaleString()}</span>
                        </div>
                      </div>
                      <span className={`badge ${badgeMap[kpi.status]}`}>{kpi.status}</span>
                    </div>
                  );
                })}
              </div>

              <div style={{ marginTop: '1rem', padding: '1rem', background: 'rgba(244,63,94,0.06)', borderRadius: '8px', border: '1px solid rgba(244,63,94,0.15)', textAlign: 'center' }}>
                <span style={{ fontSize: '0.78rem', color: 'var(--text-secondary)' }}>Total Estimated Financial Impact:</span>
                <div style={{ fontSize: '1.4rem', fontWeight: 700, color: 'var(--accent-rose)', fontFamily: 'var(--font-mono)', marginTop: '0.25rem' }}>
                  ${impact.data.total_financial_impact_usd.toLocaleString()} USD
                </div>
              </div>

              {impact.data.active_lineage_degradations && impact.data.active_lineage_degradations.length > 0 && (
                <div style={{ marginTop: '1rem' }}>
                  <div style={{ fontSize: '0.78rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: '0.35rem' }}>Data Lineage Impact:</div>
                  {impact.data.active_lineage_degradations.map((deg, i) => (
                    <div key={i} style={{ fontSize: '0.72rem', color: 'var(--accent-amber)', background: 'rgba(245,158,11,0.05)', padding: '0.4rem', borderRadius: '4px', marginBottom: '0.25rem' }}>
                      <strong>{deg.node}:</strong> {deg.impact}
                    </div>
                  ))}
                </div>
              )}
            </>
          ) : null}
        </div>

      </div>

      {/* AI Recommendations */}
      <div className="glass-card animate-in" style={{ marginTop: '1.5rem' }}>
        <h3 className="section-title">AI-Driven Actionable Recommendations</h3>
        <p className="section-subtitle">AI Recommendations</p>
        {recommendations.loading ? (
          <div className="loading-state">
            <div className="loading-spinner"></div>
            <span>Generating recommendations...</span>
          </div>
        ) : recommendations.error ? (
          <div className="alert-box critical">Failed to generate recommendations</div>
        ) : recommendations.data ? (
          <div className="rec-grid">
            {recommendations.data.recommendations.map((rec) => {
              const typeColorMap = {
                NOTIFY_DEV: 'badge-info',
                HALT_INGEST: 'badge-danger',
                RESTORE_BACKUP: 'badge-warning'
              };
              const statusColorMap = {
                PENDING: 'badge-muted',
                RECOMMENDED: 'badge-warning',
                AVAILABLE: 'badge-success'
              };
              return (
                <div key={rec.id} className="rec-card">
                  <div className="rec-header">
                    <span className="rec-id">{rec.id}</span>
                    <span className={`badge ${typeColorMap[rec.action_type] || 'badge-muted'}`}>{rec.action_type}</span>
                  </div>
                  <h4 className="rec-title">{rec.title}</h4>
                  <p className="rec-desc">{rec.description}</p>
                  <div style={{ marginTop: '0.75rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>Status:</span>
                    <span className={`badge ${statusColorMap[rec.status] || 'badge-muted'}`}>{rec.status}</span>
                  </div>
                </div>
              );
            })}
          </div>
        ) : null}
      </div>

    </div>
  );
}
