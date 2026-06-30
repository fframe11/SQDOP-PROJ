import React from "react";

export default function Metadata() {
  return (
    <div className="page-container">
      <div className="service-page">
        <div className="service-icon-large card-icon purple" style={{ width: 80, height: 80, fontSize: "2.5rem" }}>
          🗂️
        </div>
        <h1>OpenMetadata</h1>
        <p className="service-desc">
          OpenMetadata ใช้สำหรับ Data Catalog & Governance — จัดการ metadata, data lineage, data quality, และ collaboration
        </p>
        <div className="service-url">🌐 http://localhost:8585</div>
        <a
          href="http://localhost:8585"
          target="_blank"
          rel="noopener noreferrer"
          className="btn btn-primary"
        >
          🚀 เปิด OpenMetadata
        </a>
      </div>
    </div>
  );
}
