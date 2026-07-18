import React from 'react';
import { useApi } from '../hooks/useApi';
import { ComposedChart, AreaChart, Area, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, Cell, ReferenceLine, Legend } from 'recharts';
import "./Analytics.css";

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
      Score: parseFloat(projection.data.projected_scores[i]?.toFixed(2) ?? 0),
      High:  parseFloat(projection.data.ci_high[i]?.toFixed(2) ?? 0),
      Low:   parseFloat(projection.data.ci_low[i]?.toFixed(2) ?? 0),
    }));
  }, [projection.data]);

  // Dynamic Y-axis: zoom into range
  const yDomain = React.useMemo(() => {
    if (!projectionData.length) return [75, 102];
    const allVals = projectionData.flatMap(d => [d.Score, d.High, d.Low]).filter(Boolean);
    const minVal = Math.min(...allVals);
    const maxVal = Math.max(...allVals);
    const pad = Math.max((maxVal - minVal) * 1.5, 1.5);
    return [parseFloat((minVal - pad).toFixed(1)), parseFloat((maxVal + pad * 0.5).toFixed(1))];
  }, [projectionData]);

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

  const clusterColors = ['#6C47FF', '#3B82F6', '#10B981', '#F59E0B', '#EF4444'];

  return (
    <div className="gs-analytics">
      {/* 1. Page Header */}
      <div className="gs-page-header">
        <div>
          <h1 className="gs-page-title">Live <span>Analytics Hub</span></h1>
          <p className="gs-page-desc">Data quality forecasting models, anomaly clustering, and business SLA impacts</p>
        </div>
      </div>

      {/* 2. 7-Day Quality Forecast */}
      <div className="gs-acard gs-acard-wide">
        <div className="gs-acard-head">
          <h3>7-Day Quality Forecast Model</h3>
          <p>Predictive regression analysis of pipeline quality metrics</p>
        </div>

        {projection.loading ? (
          <div className="gs-empty">Running quality forecasting models...</div>
        ) : projection.error ? (
          <div className="gs-toast err">Failed to load forecasting metrics</div>
        ) : projection.data ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            <div className="gs-achart">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={projectionData} margin={{ top: 10, right: 20, left: -25, bottom: 0 }}>
                  <defs>
                    <linearGradient id="anHigh" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--accent-green)" stopOpacity={0.15}/>
                      <stop offset="95%" stopColor="var(--accent-green)" stopOpacity={0}/>
                    </linearGradient>
                    <linearGradient id="anLow" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--accent-red)" stopOpacity={0.1}/>
                      <stop offset="95%" stopColor="var(--accent-red)" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.05)" />
                  <XAxis dataKey="day" stroke="#64748b" tick={{ fontSize: 9.5 }} />
                  <YAxis domain={yDomain} stroke="#64748b" tick={{ fontSize: 9.5 }} tickFormatter={v => `${v}%`} />
                  <Tooltip contentStyle={{ background: '#FFFFFF', border: '1px solid #E2E8F0', borderRadius: 8 }} />
                  <ReferenceLine y={95} stroke="var(--accent-yellow)" strokeDasharray="5 3" label={{ value: 'SLA Limit (95%)', position: 'right', fill: 'var(--accent-yellow)', fontSize: 9 }} />
                  <Area type="monotone" dataKey="High" stroke="var(--accent-green)" fill="url(#anHigh)" strokeWidth={1.5} dot={{ r: 2 }} />
                  <Area type="monotone" dataKey="Low" stroke="var(--accent-red)" fill="url(#anLow)" strokeWidth={1.5} dot={{ r: 2 }} />
                  <Line type="monotone" dataKey="Score" stroke="var(--accent-purple)" strokeWidth={2.5} dot={{ r: 3.5 }} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>

            <div className="gs-analytics-kpis">
              <div className="gs-akpi">
                <span className="gs-akpi-label">Stability Index</span>
                <span className="gs-akpi-value" style={{ color: 'var(--accent-green)' }}>{projection.data.stability_index}</span>
              </div>
              <div className="gs-akpi">
                <span className="gs-akpi-label">SLA Breach Prob</span>
                <span className="gs-akpi-value" style={{ color: 'var(--accent-red)' }}>{projection.data.sla_breach_probability}</span>
              </div>
            </div>

            <p style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
              <strong>Historical Trend Summary:</strong> {projection.data.historical_trend}
            </p>

            {projection.data.crisis_forecast && projection.data.crisis_forecast.severity !== 'LOW' && (
              <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: '8px', padding: '12px', fontSize: '12px', color: 'var(--accent-red)' }}>
                <strong>Predictive Quality Crisis Alert:</strong> Expected breach in {projection.data.crisis_forecast.days_until_crisis} days on {projection.data.crisis_forecast.impacted_component} (Reason: {projection.data.crisis_forecast.reason}).
              </div>
            )}
          </div>
        ) : null}
      </div>

      {/* 3. Main Analytical Splitted Grid */}
      <div className="gs-analytics-grid">
        
        {/* Error Pattern Clustering */}
        <div className="gs-acard">
          <div className="gs-acard-head">
            <h3>Error Pattern Clustering</h3>
            <p>Identified anomalies aggregated by source pattern</p>
          </div>

          {clustering.loading ? (
            <div className="gs-empty">Aggregating pattern anomalies...</div>
          ) : clustering.error ? (
            <div className="gs-toast err">Failed to load error patterns</div>
          ) : clusteringData.length > 0 ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <div className="gs-achart-sm">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={clusteringData} layout="vertical" margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(0,0,0,0.04)" />
                    <XAxis type="number" stroke="#64748b" tick={{ fontSize: 9.5 }} />
                    <YAxis type="category" dataKey="name" stroke="#64748b" tick={{ fontSize: 9.5 }} width={70} />
                    <Tooltip />
                    <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                      {clusteringData.map((entry, idx) => (
                        <Cell key={`cell-${idx}`} fill={clusterColors[idx % clusterColors.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                {clustering.data.clusters.map((c, i) => (
                  <div key={i} style={{ fontSize: '11px', display: 'flex', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '4px' }}>
                    <span style={{ color: clusterColors[i % clusterColors.length], fontWeight: 700 }}>● {c.source}</span>
                    <span style={{ color: 'var(--text-secondary)' }}>{c.errors_count} events ({c.percentage}%)</span>
                  </div>
                ))}
              </div>

              <div style={{ background: 'var(--bg-primary)', border: '1px solid var(--border-color)', borderRadius: '8px', padding: '10px', fontSize: '11px', color: 'var(--text-secondary)', marginTop: '8px' }}>
                <strong>Correlation:</strong> {clustering.data.correlation_analysis}
              </div>
            </div>
          ) : null}
        </div>

        {/* Business KPI Impact */}
        <div className="gs-acard">
          <div className="gs-acard-head">
            <h3>Business KPI Impact Assessment</h3>
            <p>Estimated downstream degradation of analytical indicators</p>
          </div>

          {impact.loading ? (
            <div className="gs-empty">Assessing downstream SLA degradations...</div>
          ) : impact.error ? (
            <div className="gs-toast err">Failed to load business impacts</div>
          ) : impact.data ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <div className="gs-impact-list">
                {impact.data.kpi_connections.map((kpi, i) => (
                  <div key={i} className="gs-impact-item">
                    <span className="gs-impact-name">{kpi.kpi_name}</span>
                    <div className="gs-impact-bar-bg">
                      <div className="gs-impact-bar" style={{ width: `${kpi.impact_pct}%`, background: kpi.status === 'CRITICAL' ? 'var(--accent-red)' : kpi.status === 'WARN' ? 'var(--accent-yellow)' : 'var(--accent-green)' }} />
                    </div>
                    <span className="gs-impact-score" style={{ color: kpi.status === 'CRITICAL' ? 'var(--accent-red)' : 'var(--text-main)' }}>-{kpi.impact_pct}%</span>
                  </div>
                ))}
              </div>

              <div style={{ padding: '16px', background: 'rgba(239,68,68,0.04)', borderRadius: '8px', border: '1px solid rgba(239,68,68,0.1)', textAlign: 'center', marginTop: '10px' }}>
                <span style={{ fontSize: '11.5px', color: 'var(--text-muted)' }}>Estimated Cumulative Business Losses:</span>
                <div style={{ fontSize: '1.6rem', fontWeight: 800, color: 'var(--accent-red)', fontFamily: 'var(--font-mono)', marginTop: '4px' }}>
                  ${impact.data.total_financial_impact_usd.toLocaleString()} USD
                </div>
              </div>

              {impact.data.active_lineage_degradations && impact.data.active_lineage_degradations.length > 0 && (
                <div style={{ marginTop: '4px' }}>
                  {impact.data.active_lineage_degradations.map((deg, i) => (
                    <div key={i} style={{ fontSize: '11px', color: 'var(--accent-yellow)', background: '#fffbeb', border: '1px solid #fde68a', padding: '6px', borderRadius: '4px', marginBottom: '4px' }}>
                      <strong>{deg.node}:</strong> {deg.impact}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ) : null}
        </div>

      </div>

      {/* AI Recommendations */}
      <div className="gs-acard">
        <div className="gs-acard-head">
          <h3>AI-Driven Actionable Recommendations</h3>
          <p>Closed-loop action vectors generated by observability intelligence</p>
        </div>

        {recommendations.loading ? (
          <div className="gs-empty">Synthesizing action recommendations...</div>
        ) : recommendations.error ? (
          <div className="gs-toast err">Failed to generate AI proposals</div>
        ) : recommendations.data ? (
          <div className="gs-rec-list">
            {recommendations.data.recommendations.map((rec) => (
              <div key={rec.id} className={`gs-rec ${rec.status === 'RECOMMENDED' ? 'medium' : ''}`}>
                <span className="gs-rec-badge">{rec.action_type}</span>
                <div className="gs-rec-body">
                  <strong>{rec.title}</strong>
                  <p>{rec.description}</p>
                </div>
                <div style={{ marginLeft: 'auto', display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '6px' }}>
                  <span style={{ fontSize: '9px', fontWeight: 700, textTransform: 'uppercase', color: 'var(--text-muted)' }}>{rec.status}</span>
                </div>
              </div>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}
