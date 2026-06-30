import React from 'react';
import { Link } from 'react-router-dom';
import { useApi } from '../hooks/useApi';

export default function Home() {
  const { data: services, loading, error } = useApi('/services/status', { refreshInterval: 30000 });

  const architectureFeatures = [
    {
      icon: "📡",
      color: "blue",
      title: "Data Ingestion",
      desc: "เชื่อมต่อข้อมูลแบบ Real-time ด้วย Apache Kafka และ API Gateway รองรับ Data Sources หลากหลาย"
    },
    {
      icon: "⚡",
      color: "purple",
      title: "Processing Engine",
      desc: "ประมวลผลข้อมูลด้วย Apache Spark Master/Worker Cluster แบบกระจายศูนย์ (Distributed Engine)"
    },
    {
      icon: "💾",
      color: "cyan",
      title: "Storage Layer",
      desc: "จัดเก็บข้อมูลปริมาณมากใน HDFS และ Elasticsearch เพื่อการสืบค้นและทำดัชนีที่มีประสิทธิภาพ"
    },
    {
      icon: "🛡️",
      color: "emerald",
      title: "Quality Assurance",
      desc: "ตรวจสอบคุณภาพข้อมูลอัตโนมัติด้วย Audit Engine คัดกรองข้อมูลผิดปกติเข้าสู่ HDFS Quarantine"
    },
    {
      icon: "📊",
      color: "amber",
      title: "Monitoring & Alerting",
      desc: "ติดตามสถานะระบบและความผิดปกติผ่าน Prometheus, Grafana และ Loki แบบรวมศูนย์"
    },
    {
      icon: "🗂️",
      color: "rose",
      title: "Data Governance",
      desc: "สร้าง Data Catalog, Lineage และติดตามโครงสร้างข้อมูลผ่าน OpenMetadata"
    }
  ];

  return (
    <div className="page-container">
      {/* Hero Section */}
      <section className="hero">
        <div className="hero-badge">⚡ Scalable Data Observability & Quality Assurance Platform</div>
        <h1>
          ยินดีต้อนรับสู่ <span className="gradient-text">SDOQAP</span>
        </h1>
        <p className="hero-subtitle">
          นวัตกรรมแพลตฟอร์มสังเกตการณ์และรับประกันคุณภาพข้อมูลขนาดใหญ่ ออกแบบและพัฒนาเพื่อแก้ปัญหาระบบ Data Pipeline ขนาดใหญ่ที่มีความซับซ้อน ได้ในระยะเวลาจำกัดอย่างมีประสิทธิภาพสูงสุด
        </p>
        <div className="hero-actions">
          <Link to="/dashboard" className="btn btn-primary">
            🚀 เปิด Dashboard ระบบ
          </Link>
          <Link to="/analytics" className="btn btn-secondary">
            🔍 วิเคราะห์ข้อมูลเชิงลึก
          </Link>
        </div>
      </section>

      {/* Innovation Highlights */}
      <div className="stats-bar">
        <div className="home-stat-item">
          <div className="home-stat-value">3 เดือน</div>
          <div className="home-stat-label">ระยะเวลาพัฒนา</div>
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
        <h3 className="section-title">🔌 Live Infrastructure Status</h3>
        <p className="section-subtitle" style={{ marginBottom: '1.25rem' }}>สถานะการเชื่อมต่อบริการระบบทั้งหมดแบบ Real-time</p>
        {loading ? (
          <div className="loading-state">
            <div className="loading-spinner"></div>
            <span>กำลังตรวจสอบสถานะการเชื่อมต่อ...</span>
          </div>
        ) : error ? (
          <div className="alert-box critical">⚠️ ไม่สามารถเชื่อมต่อกับบริการ API ตรวจสอบสถานะได้</div>
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
      <h2 className="section-title">สถาปัตยกรรมระบบนวัตกรรม</h2>
      <p className="section-subtitle">ภาพรวมของ Component หลักที่เชื่อมต่ออยู่ใน Data Pipeline ของเรา</p>
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
