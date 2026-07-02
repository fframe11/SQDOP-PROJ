import React from "react";

export default function Grafana() {
  const grafanaPort = "3002";
  const grafanaUrl = `http://${window.location.hostname}:${grafanaPort}`;

  return (
    <div className="page-container">
      <div className="service-page">
        <div className="service-icon-large card-icon emerald" style={{ width: 80, height: 80, fontSize: "2.5rem" }}>
          📈
        </div>
        <h1>Grafana</h1>
        <p className="service-desc">
          Grafana ใช้สำหรับ Monitoring & Visualization — ดู metrics, สร้าง dashboard, ตั้ง alert rule, และตรวจสอบ system health แบบ real-time
        </p>
        <div className="service-url">🌐 {grafanaUrl}</div>
        <a
          href={grafanaUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="btn btn-primary"
        >
          🚀 เปิด Grafana
        </a>
      </div>
    </div>
  );
}
