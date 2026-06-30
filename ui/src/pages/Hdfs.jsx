import React from "react";

export default function Hdfs() {
  return (
    <div className="page-container">
      <div className="service-page">
        <div className="service-icon-large card-icon amber" style={{ width: 80, height: 80, fontSize: "2.5rem" }}>
          💾
        </div>
        <h1>HDFS UI</h1>
        <p className="service-desc">
          HDFS NameNode UI — จัดการไฟล์บน Hadoop Distributed File System, ดู storage usage, ตรวจสอบสถานะ DataNode และ block reports
        </p>
        <div className="service-url">🌐 http://localhost:9870</div>
        <a
          href="http://localhost:9870"
          target="_blank"
          rel="noopener noreferrer"
          className="btn btn-primary"
        >
          🚀 เปิด HDFS UI
        </a>
      </div>
    </div>
  );
}
