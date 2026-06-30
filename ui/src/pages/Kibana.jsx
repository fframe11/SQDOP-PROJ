import React from "react";

export default function Kibana() {
  return (
    <div className="page-container">
      <div className="service-page">
        <div className="service-icon-large card-icon cyan" style={{ width: 80, height: 80, fontSize: "2.5rem" }}>
          🔍
        </div>
        <h1>Kibana</h1>
        <p className="service-desc">
          Kibana ใช้สำหรับ Log Analytics — ค้นหา, วิเคราะห์, และ visualize log จาก Elasticsearch เพื่อ debug และ monitor ระบบ
        </p>
        <div className="service-url">🌐 http://localhost:5601</div>
        <a
          href="http://localhost:5601"
          target="_blank"
          rel="noopener noreferrer"
          className="btn btn-primary"
        >
          🚀 เปิด Kibana
        </a>
      </div>
    </div>
  );
}
