import React from 'react';
import { Link } from 'react-router-dom';
import { useApi } from '../hooks/useApi';
import "./Home.css";

export default function Home() {
  const { data: services, loading, error } = useApi('/services/status', { refreshInterval: 30000 });
  const kpis = useApi('/kpi/stats', { refreshInterval: 30000 });

  const kafkaOnline = services?.['Kafka Broker']?.status === 'online';
  const postgresOnline = services?.['Postgres DB']?.status === 'online';
  const restOnline = services?.['REST Ingestion API']?.status === 'online';

  let overallStatus = "OFFLINE";
  let statusClass = "offline";

  if (kafkaOnline && postgresOnline && restOnline) {
    overallStatus = "ACTIVE";
    statusClass = "online";
  } else if (kafkaOnline || postgresOnline || restOnline) {
    overallStatus = "PARTIAL";
    statusClass = "warning";
  }

  const [activeCategory, setActiveCategory] = React.useState("observability");
  const [activeSubItem, setActiveSubItem] = React.useState("static");

  const handleCategoryClick = (category) => {
    setActiveCategory(category);
    if (category === "observability") setActiveSubItem("static");
    if (category === "drift") setActiveSubItem("auto-evolve");
    if (category === "remediation") setActiveSubItem("n8n");
  };

  const renderVisualPreviewCard = (subItem) => {
    switch (subItem) {
      case "static":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span className="preview-card-title">Static Quality Rules Check</span>
              <span className="preview-badge success">99.8% Passed</span>
            </div>
            <table className="preview-table">
              <thead>
                <tr>
                  <th>Field</th>
                  <th>Rule Constraint</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td><code>id</code></td>
                  <td>NOT NULL</td>
                  <td style={{ color: '#10B981', fontWeight: 600 }}>✓ PASS</td>
                </tr>
                <tr>
                  <td><code>price</code></td>
                  <td>MIN_VALUE &gt;= 0.0</td>
                  <td style={{ color: '#10B981', fontWeight: 600 }}>✓ PASS</td>
                </tr>
                <tr>
                  <td><code>device_id</code></td>
                  <td>LENGTH == 16</td>
                  <td style={{ color: '#10B981', fontWeight: 600 }}>✓ PASS</td>
                </tr>
              </tbody>
            </table>
          </div>
        );
      case "dynamic":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span className="preview-card-title">Dynamic Profiling Insights</span>
              <span className="preview-badge success">Anomaly Score: 0.12 (Low)</span>
            </div>
            <div style={{ background: '#F8FAFC', border: '1px solid #E2E8F0', borderRadius: '8px', padding: '12px', fontSize: '12px', color: '#475569', display: 'flex', flexDirection: 'column', gap: '8px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>Daily Volume Mean:</span>
                <strong style={{ color: '#0F172A' }}>1,240,500 recs</strong>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>Standard Deviation (σ):</span>
                <strong style={{ color: '#0F172A' }}>12,400 recs</strong>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>Today's Total Volume:</span>
                <strong style={{ color: '#10B981' }}>1,245,200 (Normal)</strong>
              </div>
            </div>
            <div style={{ fontSize: '11px', color: '#64748B', marginTop: '6px', display: 'flex', alignItems: 'center', gap: '4px' }}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ color: 'var(--accent-blue)', flexShrink: 0 }}><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
              <span>Volume is within ±2σ bounds. Ingestion succeeded.</span>
            </div>
          </div>
        );
      case "ai":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span className="preview-card-title">AI Profiling & Anomaly Detection</span>
              <span className="preview-badge warning">1 Suspicious Attribute</span>
            </div>
            <div style={{ background: '#FFFBEB', border: '1px solid #FDE68A', borderRadius: '8px', padding: '12px', fontSize: '12px', color: '#78350F' }}>
              <strong>Attribute 'session_duration' Anomaly:</strong>
              <p style={{ marginTop: '4px', fontSize: '11px', lineHeight: '1.4' }}>
                AI detected 98.4% of values are zero within the last 1 hour. Typical rate is &lt; 5%.
              </p>
            </div>
            <div className="preview-code" style={{ marginTop: '8px' }}>
              {`Proposal: Set 'session_duration' quality score to 45%.\nStatus: Flagged for active review.`}
            </div>
          </div>
        );
      case "auto-evolve":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span className="preview-card-title">Safe Schema Drift Auto-Evolution</span>
              <span className="preview-badge success">Drift Severity: 1 (Evolved)</span>
            </div>
            <table className="preview-table">
              <thead>
                <tr>
                  <th>Column</th>
                  <th>Action</th>
                  <th>HDFS Metadata</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td><code>middle_name</code></td>
                  <td style={{ color: '#10B981', fontWeight: 600 }}>[NEW] string</td>
                  <td>Evolved safely (Score &lt;= 4)</td>
                </tr>
              </tbody>
            </table>
            <div style={{ background: '#F8FAFC', border: '1px solid #E2E8F0', borderRadius: '8px', padding: '10px', fontSize: '11px', color: '#475569', fontFamily: 'monospace', marginTop: '8px' }}>
              hdfs dfs -cat /data/active/users/_delta_log/00001.json
            </div>
          </div>
        );
      case "mitigation":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span className="preview-card-title">Dangerous Drift Mitigation</span>
              <span className="preview-badge danger">Drift Severity: 6 (Blocked)</span>
            </div>
            <div style={{ background: '#FEF2F2', border: '1px solid #FCA5A5', borderRadius: '8px', padding: '12px', fontSize: '12px', color: '#991B1B', marginBottom: '8px' }}>
              <strong>Error: Column type changed</strong>
              <p style={{ marginTop: '4px', fontSize: '11px' }}>
                Ingested column <code>price</code> is <code>String</code>, but Delta schema requires <code>Float</code>.
              </p>
            </div>
            <div style={{ fontSize: '11px', color: '#64748B', display: 'flex', flexDirection: 'column', gap: '4px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ color: 'var(--accent-red)' }}><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
                <span>Ingestion blocked. Records routed to Quarantine:</span>
              </div>
              <code>hdfs://namenode:9000/data/quarantine/sales/</code>
            </div>
          </div>
        );
      case "registry":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span className="preview-card-title">Elasticsearch Logs Registry</span>
              <span className="preview-badge success">Elasticsearch Connected</span>
            </div>
            <div className="preview-code">
              {JSON.stringify({
                "@timestamp": new Date().toISOString(),
                "event": "schema_drift",
                "table": "users",
                "severity_score": 5,
                "ingested_cols": 8,
                "active_cols": 7,
                "status": "quarantined"
              }, null, 2)}
            </div>
          </div>
        );
      case "n8n":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span className="preview-card-title">n8n Closed-loop Workflow</span>
              <span className="preview-badge success">n8n Workflow Active</span>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', fontSize: '12px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '8px', background: '#F8FAFC', border: '1px solid #E2E8F0', borderRadius: '6px' }}>
                <span style={{ background: '#3B82F6', color: '#fff', padding: '2px 6px', borderRadius: '4px', fontSize: '10px' }}>Webhook</span>
                <span style={{ color: '#475569' }}>Triggered by Drift Severity Score &gt; 4</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '8px', background: '#F8FAFC', border: '1px solid #E2E8F0', borderRadius: '6px' }}>
                <span style={{ background: '#A855F7', color: '#fff', padding: '2px 6px', borderRadius: '4px', fontSize: '10px' }}>Slack Node</span>
                <span style={{ color: '#475569' }}>Post alert notification payload</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '8px', background: '#F8FAFC', border: '1px solid #E2E8F0', borderRadius: '6px' }}>
                <span style={{ background: '#10B981', color: '#fff', padding: '2px 6px', borderRadius: '4px', fontSize: '10px' }}>API Call</span>
                <span style={{ color: '#475569' }}>Create Schema Proposal log in Portal</span>
              </div>
            </div>
          </div>
        );
      case "slack":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span className="preview-card-title">Slack Notification Alert</span>
              <span className="preview-badge warning">Slack Webhook Active</span>
            </div>
            <div style={{ border: '1px solid #E2E8F0', borderRadius: '8px', padding: '12px', fontSize: '12px', background: '#F8FAFC' }}>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center', marginBottom: '8px' }}>
                <div style={{ width: '20px', height: '20px', borderRadius: '4px', background: '#6C47FF', color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 'bold', fontSize: '10px' }}>S</div>
                <strong>SDOQAP Observability Bot</strong>
                <span style={{ color: '#64748B', fontSize: '10px' }}>12:00 PM</span>
              </div>
              <p style={{ color: '#334155', fontSize: '11px', lineHeight: '1.4' }}>
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px', marginBottom: '2px' }}>
                  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ color: 'var(--accent-yellow)', flexShrink: 0 }}><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
                  <strong>Schema Drift detected in table telemetry_raw:</strong>
                </span>
                <br />
                Columns mismatched: <code>session_id</code> is missing. Severity: 5. Ingestion paused.
              </p>
              <button style={{ marginTop: '8px', background: '#FFF', border: '1px solid #CBD5E1', borderRadius: '4px', padding: '4px 8px', fontSize: '10px', cursor: 'pointer', fontWeight: 600, color: '#334155' }}>
                View Proposal
              </button>
            </div>
          </div>
        );
      case "portal":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <span className="preview-card-title">Central Proposal Approval</span>
              <span className="preview-badge warning">Pending Review</span>
            </div>
            <div style={{ background: '#F8FAFC', border: '1px solid #E2E8F0', borderRadius: '8px', padding: '12px', fontSize: '11px', color: '#475569', display: 'flex', flexDirection: 'column', gap: '4px' }}>
              <span><strong>Table Name:</strong> <code>telemetry_raw</code></span>
              <span><strong>Drift Type:</strong> Missing Ingested Column (session_id)</span>
              <span><strong>Severity Score:</strong> 5 (Dangerous)</span>
            </div>
            <div style={{ display: 'flex', gap: '8px', marginTop: '10px' }}>
              <button style={{ flex: 1, background: '#6C47FF', color: '#fff', border: 'none', borderRadius: '8px', padding: '6px', fontSize: '11px', fontWeight: 600, cursor: 'pointer' }}>
                Approve Evolution
              </button>
              <button style={{ flex: 1, background: '#fff', border: '1px solid #E2E8F0', color: '#475569', borderRadius: '8px', padding: '6px', fontSize: '11px', fontWeight: 600, cursor: 'pointer' }}>
                Quarantine Bad Data
              </button>
            </div>
          </div>
        );
      default:
        return null;
    }
  };

  const architectureFeatures = [
    {
      icon: (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="22 12 16 12 14 15 10 15 8 12 2 12" />
          <path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z" />
        </svg>
      ),
      title: "Data Ingestion Stage",
      desc: "Real-time ingestion pipelines running on Kafka topics, REST APIs, and file-based connections."
    },
    {
      icon: (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
        </svg>
      ),
      title: "Distributed Processing",
      desc: "Quality analytics computation and automated metrics updates powered by Apache Spark Cluster."
    },
    {
      icon: (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <ellipse cx="12" cy="5" rx="9" ry="3" />
          <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
          <path d="M3 12c0 1.66 4 3 9 3s9-1.34 9-3" />
        </svg>
      ),
      title: "Storage & Ingestion Logs",
      desc: "Massive scale unstructured files stored in HDFS nodes alongside Elasticsearch schema logs."
    },
    {
      icon: (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
        </svg>
      ),
      title: "Quarantine Routing",
      desc: "Automated routing of dirty/corrupted records straight to HDFS quarantine zones for zero impact."
    },
    {
      icon: (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <line x1="18" y1="20" x2="18" y2="10" />
          <line x1="12" y1="20" x2="12" y2="4" />
          <line x1="6" y1="20" x2="6" y2="14" />
        </svg>
      ),
      title: "Monitoring Console",
      desc: "Consolidated system performance tracking and telemetry rules indexing via Grafana dashboards."
    },
    {
      icon: (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <line x1="12" y1="3" x2="12" y2="21" />
          <line x1="6" y1="12" x2="18" y2="12" />
          <path d="M6 12a3 3 0 0 0 6 0M18 12a3 3 0 0 0-6 0" />
        </svg>
      ),
      title: "Governance & Catalog",
      desc: "Schema drift severity assessment, audit history logs, and database schema diff tracking."
    }
  ];

  const stakeholders = [
    {
      icon: (
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z" />
        </svg>
      ),
      name: "Data Engineers",
      desc: "Pipeline orchestration, ingestion retry, and schema drift evolutions."
    },
    {
      icon: (
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M6 3h12" />
          <path d="M19 17A7 7 0 1 1 5 17V3h14v14z" />
          <path d="M8.5 12h7" />
        </svg>
      ),
      name: "Data Scientists",
      desc: "Delta table data export preview, metadata indexing, and cleanliness levels."
    },
    {
      icon: (
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="23 6 13.5 15.5 8.5 10.5 1 18" />
          <polyline points="17 6 23 6 23 12" />
        </svg>
      ),
      name: "Business Analysts",
      desc: "Downstream reporting assurance, data quality KPIs, and trust metrics."
    },
    {
      icon: (
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
        </svg>
      ),
      name: "Governance & Compliance",
      desc: "SLA rule configurations, quarantine records, and policy audit trails."
    },
    {
      icon: (
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="12" cy="12" r="3" />
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
        </svg>
      ),
      name: "Operations & DevOps",
      desc: "Live cluster resource gauges, n8n workflow logs, and webhook triggers."
    }
  ];

  return (
    <div className="gs-home">
      {/* Centered Hero Section */}
      <section className="gs-hero">
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start' }}>
          <span className="gs-hero-badge">SDOQAP Observability Platform</span>
          <h1 className="gs-hero-title">
            More than observability.<br />
            <span className="gs-hero-accent">Complete Data Quality</span> Management.
          </h1>
          <p className="gs-hero-desc">
            SDOQAP bridges the gap between raw data lakes and reliable downstream analytics. Run multi-layered quality rules checks, monitor schema evolutions, and route anomalies to quarantine automatically.
          </p>
          <div className="gs-hero-actions">
            <Link to="/dashboard" className="gs-btn-primary gs-btn-lg">
              Start monitoring
            </Link>
            <Link to="/analytics" className="gs-btn-ghost">
              <span>Explore analytics</span>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" style={{ marginLeft: '6px' }}><polyline points="6 9 12 15 18 9"/></svg>
            </Link>
          </div>
        </div>

        <div className="gs-hero-visual">
          <div className="gs-hero-panel">
            <div className="gs-panel-header">
              <span className="gs-panel-title">LIVE INGESTION CHANNELS</span>
              <span className={`gs-panel-status ${statusClass}`}>{overallStatus}</span>
            </div>
            <div className="gs-panel-row">
              <span className={`gs-panel-dot ${services?.['Kafka Broker']?.status === 'online' ? 'online' : 'offline'}`}></span>
              <span className="gs-panel-name">Kafka reddit_streaming</span>
              <span className={`gs-panel-badge ${services?.['Kafka Broker']?.status === 'online' ? 'online' : 'offline'}`}>
                {services?.['Kafka Broker']?.status === 'online' ? 'Ingesting' : 'Offline'}
              </span>
            </div>
            <div className="gs-panel-row">
              <span className={`gs-panel-dot ${services?.['Postgres DB']?.status === 'online' ? 'online' : 'offline'}`}></span>
              <span className="gs-panel-name">Postgres JDBC Ingest</span>
              <span className={`gs-panel-badge ${services?.['Postgres DB']?.status === 'online' ? 'online' : 'offline'}`}>
                {services?.['Postgres DB']?.status === 'online' ? 'Idle' : 'Offline'}
              </span>
            </div>
            <div className="gs-panel-row">
              <span className={`gs-panel-dot ${services?.['REST Ingestion API']?.status === 'online' ? 'online' : 'offline'}`}></span>
              <span className="gs-panel-name">REST telemetry_api</span>
              <span className={`gs-panel-badge ${services?.['REST Ingestion API']?.status === 'online' ? 'online' : 'offline'}`}>
                {services?.['REST Ingestion API']?.status === 'online' ? 'Active' : 'Offline'}
              </span>
            </div>
          </div>
          <div className="gs-hero-kpis">
            <div className="gs-mini-kpi">
              <span className="gs-mini-val">
                {kpis.data ? `${kpis.data.global_quality_score.toFixed(1)}%` : '---'}
              </span>
              <span className="gs-mini-label">AVG Quality Score</span>
            </div>
            <div className="gs-mini-kpi">
              <span className="gs-mini-val">
                {kpis.data
                  ? kpis.data.total_records_ingested >= 1000000
                    ? `${(kpis.data.total_records_ingested / 1000000).toFixed(1)}M`
                    : kpis.data.total_records_ingested.toLocaleString()
                  : '---'}
              </span>
              <span className="gs-mini-label">Records Checked</span>
            </div>
            <div className="gs-mini-kpi">
              <span className="gs-mini-val">
                {kpis.data && kpis.data.total_records_ingested > 0
                  ? `${(kpis.data.quarantined_records / kpis.data.total_records_ingested * 100).toFixed(3)}%`
                  : '0.000%'}
              </span>
              <span className="gs-mini-label">Quarantine Rate</span>
            </div>
          </div>
        </div>
      </section>

      {/* Trusted Partners Bar (Real System Stack) */}
      <div className="gs-tech-bar">
        <span className="gs-tech-label">Built on:</span>
        <div className="gs-tech-logos">
          <span className="gs-tech-item">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--accent-purple)" strokeWidth="2.5"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
            Apache Spark
          </span>
          <span className="gs-tech-item">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--accent-purple)" strokeWidth="2.5"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>
            Apache Kafka
          </span>
          <span className="gs-tech-item">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--accent-purple)" strokeWidth="2.5"><polygon points="12 2 2 22 22 22"/></svg>
            Delta Lake
          </span>
          <span className="gs-tech-item">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--accent-purple)" strokeWidth="2.5"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
            Elasticsearch
          </span>
          <span className="gs-tech-item">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--accent-purple)" strokeWidth="2.5"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
            FastAPI
          </span>
          <span className="gs-tech-item">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--accent-purple)" strokeWidth="2.5"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>
            Grafana
          </span>
        </div>
      </div>

      {/* Live Service Status */}
      <div style={{ padding: '60px 48px 0' }}>
        <div style={{ background: '#ffffff', border: '1px solid var(--border-color)', borderRadius: '12px', padding: '24px', boxShadow: 'var(--shadow-sm)' }}>
          <h3 className="gs-section-title" style={{ fontSize: '18px', margin: '0 0 6px 0', textTransform: 'uppercase' }}>Live Infrastructure Connections</h3>
          <p className="gs-section-desc" style={{ margin: '0 0 20px 0', fontSize: '12.5px', color: 'var(--text-muted)' }}>Real-time service nodes configuration checks</p>
          {loading ? (
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '13px', color: 'var(--text-muted)' }}>
              <span>Checking health...</span>
            </div>
          ) : error ? (
            <div style={{ background: '#fef2f2', border: '1px solid #fecaca', color: 'var(--accent-red)', padding: '10px 14px', borderRadius: '8px', fontSize: '13px' }}>
              Failed to connect to health check API service.
            </div>
          ) : services ? (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: '12px' }}>
              {Object.entries(services).map(([name, info]) => (
                <div key={name} style={{ display: 'flex', alignItems: 'center', gap: '10px', padding: '10px 14px', background: 'var(--bg-primary)', border: '1px solid var(--border-color)', borderRadius: '8px' }}>
                  <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: info.status === 'online' ? 'var(--accent-green)' : 'var(--accent-red)' }} />
                  <strong style={{ fontSize: '12.5px', textTransform: 'capitalize', color: 'var(--text-main)' }}>{name}</strong>
                  <span style={{ fontSize: '10px', color: 'var(--text-muted)', marginLeft: 'auto', fontFamily: 'var(--font-mono)', textTransform: 'uppercase' }}>{info.status}</span>
                </div>
              ))}
            </div>
          ) : null}
        </div>
      </div>

      {/* SDOQAP Capabilities split showcase */}
      <div className="gs-features">
        <div className="gs-section-header">
          <span className="gs-section-tag">SDOQAP Components</span>
          <h2 className="gs-section-title">Core System Capabilities</h2>
          <p className="gs-section-desc">Drop-in data governance, automated lineage, and quarantine routing for modern medallion data lakes.</p>
        </div>

        <div className="showcase-container">
          <div className="showcase-left">
            <div className={`showcase-acc-group ${activeCategory === "observability" ? "active" : ""}`}>
              <div className="showcase-acc-header" onClick={() => handleCategoryClick("observability")}>
                <span className={`showcase-bullet ${activeCategory === "observability" ? "active" : ""}`}></span>
                <span className="showcase-acc-title">DATA OBSERVABILITY & QUALITY</span>
                <span style={{ marginLeft: "auto" }}>
                  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" style={{ transform: activeCategory === "observability" ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.2s' }}><polyline points="6 9 12 15 18 9"/></svg>
                </span>
              </div>
              {activeCategory === "observability" && (
                <div className="showcase-acc-body">
                  <p>Automatic 3-layer data validation (Static, Dynamic, and AI-driven rules) running on Spark &amp; HDFS nodes.</p>
                  <div className="showcase-acc-links">
                    <span className={`showcase-acc-link ${activeSubItem === "static" ? "active" : ""}`} onClick={() => setActiveSubItem("static")}>&lt;StaticRules /&gt;</span>
                    <span className={`showcase-acc-link ${activeSubItem === "dynamic" ? "active" : ""}`} onClick={() => setActiveSubItem("dynamic")}>&lt;DynamicMetrics /&gt;</span>
                    <span className={`showcase-acc-link ${activeSubItem === "ai" ? "active" : ""}`} onClick={() => setActiveSubItem("ai")}>&lt;AIAnomalyProfiling /&gt;</span>
                  </div>
                </div>
              )}
            </div>

            <div className={`showcase-acc-group ${activeCategory === "drift" ? "active" : ""}`}>
              <div className="showcase-acc-header" onClick={() => handleCategoryClick("drift")}>
                <span className={`showcase-bullet ${activeCategory === "drift" ? "active" : ""}`}></span>
                <span className="showcase-acc-title">SCHEMA DRIFT GOVERNANCE</span>
                <span style={{ marginLeft: "auto" }}>
                  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" style={{ transform: activeCategory === "drift" ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.2s' }}><polyline points="6 9 12 15 18 9"/></svg>
                </span>
              </div>
              {activeCategory === "drift" && (
                <div className="showcase-acc-body">
                  <p>Calculate schema drift severity scores and auto-evolve schema or route to quarantine based on business SLAs.</p>
                  <div className="showcase-acc-links">
                    <span className={`showcase-acc-link ${activeSubItem === "auto-evolve" ? "active" : ""}`} onClick={() => setActiveSubItem("auto-evolve")}>&lt;AutoEvolve /&gt;</span>
                    <span className={`showcase-acc-link ${activeSubItem === "mitigation" ? "active" : ""}`} onClick={() => setActiveSubItem("mitigation")}>&lt;DriftMitigation /&gt;</span>
                    <span className={`showcase-acc-link ${activeSubItem === "registry" ? "active" : ""}`} onClick={() => setActiveSubItem("registry")}>&lt;ElasticsearchLogs /&gt;</span>
                  </div>
                </div>
              )}
            </div>

            <div className={`showcase-acc-group ${activeCategory === "remediation" ? "active" : ""}`}>
              <div className="showcase-acc-header" onClick={() => handleCategoryClick("remediation")}>
                <span className={`showcase-bullet ${activeCategory === "remediation" ? "active" : ""}`}></span>
                <span className="showcase-acc-title">UPSTREAM REMEDIATION</span>
                <span style={{ marginLeft: "auto" }}>
                  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" style={{ transform: activeCategory === "remediation" ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.2s' }}><polyline points="6 9 12 15 18 9"/></svg>
                </span>
              </div>
              {activeCategory === "remediation" && (
                <div className="showcase-acc-body">
                  <p>Automated closed-loop feedback notifying data source owners and proposing schema fixes to resolve dirty data at the root.</p>
                  <div className="showcase-acc-links">
                    <span className={`showcase-acc-link ${activeSubItem === "n8n" ? "active" : ""}`} onClick={() => setActiveSubItem("n8n")}>&lt;n8nWorkflows /&gt;</span>
                    <span className={`showcase-acc-link ${activeSubItem === "slack" ? "active" : ""}`} onClick={() => setActiveSubItem("slack")}>&lt;SlackAlerts /&gt;</span>
                    <span className={`showcase-acc-link ${activeSubItem === "portal" ? "active" : ""}`} onClick={() => setActiveSubItem("portal")}>&lt;ApprovalDashboard /&gt;</span>
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="showcase-right">
            {renderVisualPreviewCard(activeSubItem)}
          </div>
        </div>
      </div>

      {/* Architecture features grid */}
      <div style={{ background: '#ffffff', padding: '64px 48px', borderTop: '1px solid var(--border-color)', borderBottom: '1px solid var(--border-color)' }}>
        <div className="gs-section-header" style={{ marginBottom: '32px' }}>
          <span className="gs-section-tag">System Map</span>
          <h2 className="gs-section-title">Core Architecture Layers</h2>
        </div>
        <div className="gs-features-grid">
          {architectureFeatures.map((feat, idx) => (
            <div className="gs-feature-card" key={idx}>
              <div className="gs-feature-icon">{feat.icon}</div>
              <h3>{feat.title}</h3>
              <p>{feat.desc}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Stakeholders grid */}
      <section className="gs-stakeholders">
        <div className="gs-section-header" style={{ marginBottom: '32px' }}>
          <span className="gs-section-tag">Stakeholders</span>
          <h2 className="gs-section-title">Unified Observability Cockpit</h2>
          <p className="gs-section-desc">One central portal concurrently serving all engineering, operations, and governance teams.</p>
        </div>
        <div className="gs-stakeholder-grid">
          {stakeholders.map((st, idx) => (
            <div className="gs-stakeholder-card" key={idx}>
              <span className="gs-stakeholder-icon">{st.icon}</span>
              <h4>{st.name}</h4>
              <p>{st.desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Bottom CTA */}
      <div className="gs-cta">
        <h2>Start Monitoring Your Pipelines</h2>
        <p>Achieve complete data validation, SLA monitoring, and roots remediation.</p>
        <Link to="/dashboard" className="gs-btn-primary">
          Enter Dashboard
        </Link>
      </div>
    </div>
  );
}
