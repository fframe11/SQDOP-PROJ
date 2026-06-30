import React from 'react';
import { Link } from 'react-router-dom';
import { useApi } from '../hooks/useApi';

export default function Home() {
  const { data: services, loading, error } = useApi('/services/status', { refreshInterval: 30000 });

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
      {/* Hero Section */}
      <section className="hero">
        <div className="hero-badge">Scalable Data Observability & Quality Assurance Platform</div>
        <h1>
          Welcome to <span className="gradient-text">SDOQAP</span>
        </h1>
        <p className="hero-subtitle">
          Large-scale Data Observability platform designed to monitor and guarantee data quality for complex pipelines.
        </p>
        <div className="hero-actions">
          <Link to="/dashboard" className="btn btn-primary">
            Open System Dashboard
          </Link>
          <Link to="/analytics" className="btn btn-secondary">
            Deep Analytics Insight
          </Link>
        </div>
      </section>

      {/* Innovation Highlights */}
      <div className="stats-bar">
        <div className="home-stat-item">
          <div className="home-stat-value">3 เดือน</div>
          <div className="home-stat-label">Development Time</div>
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

      {/* Architecture Overview */}
      <h2 className="section-title">System Architecture</h2>
      <p className="section-subtitle">Core Components of Our Data Pipeline</p>
      <div className="card-grid">
        {architectureFeatures.map((f, i) => (
          <div key={i} className="glass-card animate-in">
            <div className={`card-icon ${f.color}`}>{f.icon}</div>
            <h3>{f.title}</h3>
            <p>{f.desc}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
