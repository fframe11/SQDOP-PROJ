import React from 'react';
import { Link } from 'react-router-dom';
import { useApi } from '../hooks/useApi';

export default function Home() {
  const { data: services, loading, error } = useApi('/services/status', { refreshInterval: 30000 });

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
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
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
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span className="preview-card-title">Dynamic Profiling Insights</span>
              <span className="preview-badge success">Anomaly Score: 0.12 (Low)</span>
            </div>
            <div style={{ background: '#F8FAFC', border: '1px solid #E2E8F0', borderRadius: '8px', padding: '12px', fontSize: '12px', color: '#475569' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                <span>Daily Volume Mean:</span>
                <strong style={{ color: '#0F172A' }}>1,240,500 recs</strong>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                <span>Standard Deviation (σ):</span>
                <strong style={{ color: '#0F172A' }}>12,400 recs</strong>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>Today's Total Volume:</span>
                <strong style={{ color: '#10B981' }}>1,245,200 recs (Normal)</strong>
              </div>
            </div>
            <div style={{ fontSize: '11px', color: '#64748B' }}>
              ℹ️ Dynamic rule: Volume is within ±2σ bounds. Ingestion proceeded.
            </div>
          </div>
        );
      case "ai":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span className="preview-card-title">AI-driven Profiling & Anomaly Detection</span>
              <span className="preview-badge warning">1 Suspicious Attribute</span>
            </div>
            <div style={{ background: '#FFFBEB', border: '1px solid #FDE68A', borderRadius: '8px', padding: '12px', fontSize: '12px', color: '#78350F' }}>
              <strong>Attribute 'session_duration' Anomaly:</strong>
              <p style={{ marginTop: '4px', fontSize: '11px', lineHeight: '1.4' }}>
                AI detected 98.4% of values are zero within the last 1 hour. Typical zero rate is &lt; 5%.
              </p>
            </div>
            <div className="preview-code" style={{ background: '#FFFDF5', borderColor: '#FDE68A' }}>
              {`Proposal: Set 'session_duration' quality score to 45%.\nStatus: Flagged for active review.`}
            </div>
          </div>
        );
      case "auto-evolve":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
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
            <div style={{ background: '#F8FAFC', border: '1px solid #E2E8F0', borderRadius: '6px', padding: '10px', fontSize: '11px', color: '#475569', fontFamily: 'monospace' }}>
              hdfs dfs -cat /data/active/users/_delta_log/00001.json
            </div>
          </div>
        );
      case "mitigation":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span className="preview-card-title">Dangerous Drift Mitigation</span>
              <span className="preview-badge danger">Drift Severity: 6 (Blocked)</span>
            </div>
            <div style={{ background: '#FEF2F2', border: '1px solid #FCA5A5', borderRadius: '8px', padding: '12px', fontSize: '12px', color: '#991B1B' }}>
              <strong>Error: Column type changed</strong>
              <p style={{ marginTop: '4px', fontSize: '11px' }}>
                Ingested column <code>price</code> type is <code>String</code>, but active Delta schema requires <code>Float</code>.
              </p>
            </div>
            <div style={{ fontSize: '11px', color: '#64748B' }}>
              🛡️ Ingestion blocked. Records successfully routed to Quarantine:
              <br />
              <code>hdfs://namenode:9000/data/quarantine/sales/</code>
            </div>
          </div>
        );
      case "registry":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
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
                "missing_cols": ["age"],
                "status": "quarantined"
              }, null, 2)}
            </div>
          </div>
        );
      case "n8n":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
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
                <span style={{ color: '#475569' }}>Construct Alert Payload & Post Message</span>
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
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
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
                ⚠️ <strong>Schema Drift detected in table telemetry_raw:</strong>
                <br />
                Columns mismatched: <code>session_id</code> is missing. Severity: 5 (Dangerous). Ingestion paused.
              </p>
              <button style={{ marginTop: '8px', background: '#FFF', border: '1px solid #CBD5E1', borderRadius: '4px', padding: '4px 8px', fontSize: '10px', cursor: 'pointer', fontWeight: 600, color: '#334155' }}>
                View Proposal in Portal
              </button>
            </div>
          </div>
        );
      case "portal":
        return (
          <div className="preview-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span className="preview-card-title">Central Proposal Approval</span>
              <span className="preview-badge warning">Pending Review</span>
            </div>
            <div style={{ background: '#F8FAFC', border: '1px solid #E2E8F0', borderRadius: '8px', padding: '12px', fontSize: '11px', color: '#475569' }}>
              <strong>Table Name:</strong> <code>telemetry_raw</code>
              <br />
              <strong>Drift Type:</strong> Missing Ingested Column (session_id)
              <br />
              <strong>Severity Score:</strong> 5 (Dangerous)
            </div>
            <div style={{ display: 'flex', gap: '8px', marginTop: '10px' }}>
              <button style={{ flex: 1, background: '#6C47FF', color: '#fff', border: 'none', borderRadius: '4px', padding: '6px', fontSize: '11px', fontWeight: 600, cursor: 'pointer' }}>
                Approve Evolution
              </button>
              <button style={{ flex: 1, background: '#fff', border: '1px solid #E2E8F0', color: '#475569', borderRadius: '4px', padding: '6px', fontSize: '11px', fontWeight: 600, cursor: 'pointer' }}>
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
      icon: "IN",
      color: "blue",
      title: "Data Ingestion",
      desc: "Real-time data ingestion via Kafka and API Gateway, supporting diverse Data Sources."
    },
    {
      icon: "PR",
      color: "purple",
      title: "Processing Engine",
      desc: "Distributed processing powered by Apache Spark Cluster."
    },
    {
      icon: "ST",
      color: "cyan",
      title: "Storage Layer",
      desc: "Massive data storage in HDFS and Elasticsearch for efficient querying and indexing."
    },
    {
      icon: "QA",
      color: "emerald",
      title: "Quality Assurance",
      desc: "Automated Data Quality checks with Bad Data Quarantine routing."
    },
    {
      icon: "MO",
      color: "amber",
      title: "Monitoring & Alerting",
      desc: "Centralized monitoring for Metrics and Logs via Grafana Stack."
    },
    {
      icon: "DG",
      color: "rose",
      title: "Data Governance",
      desc: "Data Catalog, Lineage, and schema tracking via OpenMetadata."
    }
  ];

  return (
    <div className="page-container">
      {/* Centered Hero Section */}
      <section className="hero">
        <h1>
          More than observability,<br />
          <span className="gradient-text">Complete Data Quality Management</span>
        </h1>
        <p className="hero-subtitle">
          Need more than quality checks? SDOQAP gives you full stack data quality monitoring and governance — so you can ingest faster, process safer, and stay confident in your downstream analytics.
        </p>
        <div className="hero-actions">
          <Link to="/dashboard" className="btn btn-primary" style={{ padding: "0.85rem 2rem", borderRadius: "30px", fontSize: "0.95rem" }}>
            Start monitoring for free
          </Link>
          <Link to="/analytics" className="btn btn-secondary" style={{ padding: "0.85rem 2rem", borderRadius: "30px", fontSize: "0.95rem", display: "inline-flex", gap: "6px", alignItems: "center" }}>
            <span>Explore analytics</span>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
          </Link>
        </div>
      </section>

      {/* Trusted Partners Bar (Real System Stack) */}
      <div className="trusted-bar">
        <div className="trusted-title">
          Built on enterprise-grade data infrastructure and observability stacks.
        </div>
        <div className="trusted-logos">
          <div className="trusted-logo-item" title="Apache Spark - Distributed Processing">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: '4px', color: '#64748b' }}>
              <circle cx="12" cy="12" r="4"/>
              <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/>
            </svg>
            <span style={{ fontFamily: 'Inter, sans-serif', fontWeight: 700, color: '#0F172A', fontSize: '14px' }}>Apache Spark</span>
          </div>
          <div className="trusted-logo-item" title="Apache Kafka - Real-time Streaming Ingestion">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" style={{ marginRight: '4px', color: '#64748b' }}>
              <circle cx="6" cy="12" r="3"/>
              <circle cx="18" cy="6" r="3"/>
              <circle cx="18" cy="18" r="3"/>
              <line x1="9" y1="10.5" x2="15" y2="7.5"/>
              <line x1="9" y1="13.5" x2="15" y2="16.5"/>
            </svg>
            <span style={{ fontFamily: 'Inter, sans-serif', fontWeight: 700, color: '#0F172A', fontSize: '14px' }}>Apache Kafka</span>
          </div>
          <div className="trusted-logo-item" title="Delta Lake - ACID Transactions & Storage">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: '4px', color: '#64748b' }}>
              <polygon points="12 2 2 22 22 22"/>
            </svg>
            <span style={{ fontFamily: 'Inter, sans-serif', fontWeight: 700, color: '#0F172A', fontSize: '14px' }}>Delta Lake</span>
          </div>
          <div className="trusted-logo-item" title="Elasticsearch - Query Indexing & Analytics">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: '4px', color: '#64748b' }}>
              <circle cx="11" cy="11" r="8"/>
              <line x1="21" y1="21" x2="16.65" y2="16.65"/>
            </svg>
            <span style={{ fontFamily: 'Inter, sans-serif', fontWeight: 700, color: '#0F172A', fontSize: '14px' }}>Elasticsearch</span>
          </div>
          <div className="trusted-logo-item" title="FastAPI - Ingestion Gateways & APIs">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: '4px', color: '#64748b' }}>
              <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
            </svg>
            <span style={{ fontFamily: 'Inter, sans-serif', fontWeight: 700, color: '#0F172A', fontSize: '14px' }}>FastAPI</span>
          </div>
          <div className="trusted-logo-item" title="Grafana - Metrics Dashboarding">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: '4px', color: '#64748b' }}>
              <line x1="18" y1="20" x2="18" y2="10"/>
              <line x1="12" y1="20" x2="12" y2="4"/>
              <line x1="6" y1="20" x2="6" y2="14"/>
            </svg>
            <span style={{ fontFamily: 'Inter, sans-serif', fontWeight: 700, color: '#0F172A', fontSize: '14px' }}>Grafana</span>
          </div>
        </div>
      </div>

      {/* Innovation Highlights */}
      <div className="stats-bar">
        <div className="home-stat-item">
          <div className="home-stat-value">6 Layers</div>
          <div className="home-stat-label">Core Architecture</div>
        </div>
        <div className="home-stat-item">
          <div className="home-stat-value">18+</div>
          <div className="home-stat-label">API Endpoints</div>
        </div>
        <div className="home-stat-item">
          <div className="home-stat-value">10+</div>
          <div className="home-stat-label">Microservices</div>
        </div>
        <div className="home-stat-item">
          <div className="home-stat-value">100%</div>
          <div className="home-stat-label">Automated QA</div>
        </div>
      </div>

      {/* Live Service Status */}
      <div className="glass-card animate-in" style={{ marginBottom: '2rem' }}>
        <h3 className="section-title">Live Infrastructure Status</h3>
        <p className="section-subtitle" style={{ marginBottom: '1.25rem' }}>Real-time service connection health</p>
        {loading ? (
          <div className="loading-state">
            <div className="loading-spinner"></div>
            <span>Checking service health...</span>
          </div>
        ) : error ? (
          <div className="alert-box critical">Failed to connect to health check API service</div>
        ) : services ? (
          <div className="status-grid">
            {Object.entries(services).map(([name, info]) => (
              <div key={name} className="status-item">
                <div className={`status-dot ${info.status === 'online' ? 'online' : 'offline'}`} style={{ color: info.status === 'online' ? '#10b981' : '#f43f5e' }} />
                <span className="status-name">{name}</span>
                <span className="status-text">{info.status}</span>
              </div>
            ))}
          </div>
        ) : null}
      </div>

      {/* SDOQAP Capabilities split showcase */}
      <div style={{ marginTop: '2.5rem', textAlign: 'center' }}>
        <span style={{ fontSize: '11px', fontWeight: 700, color: 'var(--accent-indigo)', letterSpacing: '1px', textTransform: 'uppercase' }}>SDOQAP Components</span>
        <h2 className="section-title" style={{ marginTop: '0.4rem', marginBottom: '0.25rem' }}>Core System Capabilities</h2>
        <p className="section-subtitle" style={{ marginBottom: '2rem' }}>Drop-in data governance, lineage tracking, and quarantine routing for modern data lakes.</p>
      </div>

      <div className="showcase-container">
        <div className="showcase-left">
          <div className={`showcase-acc-group ${activeCategory === "observability" ? "active" : ""}`}>
            <div className="showcase-acc-header" onClick={() => handleCategoryClick("observability")}>
              <span className={`showcase-bullet ${activeCategory === "observability" ? "active" : ""}`}></span>
              <span className="showcase-acc-title">DATA OBSERVABILITY & QUALITY</span>
              <span className="showcase-acc-chevron">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ transform: activeCategory === "observability" ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.2s' }}><polyline points="6 9 12 15 18 9"/></svg>
              </span>
            </div>
            {activeCategory === "observability" && (
              <div className="showcase-acc-body">
                <p>Automatic 3-layer data validation (Static, Dynamic, and AI-driven rules) running on Spark &amp; HDFS.</p>
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
              <span className="showcase-acc-chevron">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ transform: activeCategory === "drift" ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.2s' }}><polyline points="6 9 12 15 18 9"/></svg>
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
              <span className="showcase-acc-chevron">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ transform: activeCategory === "remediation" ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.2s' }}><polyline points="6 9 12 15 18 9"/></svg>
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
  );
}
