import React from "react";

export default function Metadata() {
  return (
    <div className="page-container">
      <div className="service-page" style={{ position: "relative" }}>
        <div style={{
          position: "absolute",
          top: 20,
          right: 20,
          background: "rgba(245, 158, 11, 0.15)",
          color: "#f59e0b",
          border: "1px solid rgba(245, 158, 11, 0.3)",
          borderRadius: "12px",
          padding: "4px 12px",
          fontSize: "0.85rem",
          fontWeight: 600
        }}>
          ⚠️ บริการทางเลือก (Optional)
        </div>
        <div className="service-icon-large card-icon purple" style={{ width: 80, height: 80, fontSize: "2.5rem" }}>
          🗂️
        </div>
        <h1>OpenMetadata</h1>
        <p className="service-desc">
          OpenMetadata ใช้สำหรับ Data Catalog & Governance — จัดการ metadata, data lineage, data quality, และ collaboration
        </p>
        <div className="service-url" style={{ color: "#94a3b8" }}>
          สถานะ: ไม่ได้ติดตั้งในระบบ Local Development (รองรับการเปิดใช้งานบน Production Swarm)
        </div>
        <a
          href="http://localhost:8585"
          target="_blank"
          rel="noopener noreferrer"
          className="btn btn-secondary"
          style={{ opacity: 0.6, cursor: "not-allowed", pointerEvents: "none" }}
        >
          🔒 ไม่เปิดบริการ (ปิดอยู่)
        </a>
      </div>
    </div>
  );
}
