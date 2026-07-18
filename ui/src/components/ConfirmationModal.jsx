import React from "react";

export default function ConfirmationModal({ isOpen, title, message, onConfirm, onCancel }) {
  if (!isOpen) return null;

  return (
    <div 
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        width: "100vw",
        height: "100vh",
        backgroundColor: "rgba(0, 0, 0, 0.6)",
        backdropFilter: "blur(4px)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 10000
      }}
    >
      <div 
        className="gs-modal-card" 
        style={{
          width: "420px",
          padding: "24px",
          background: "#FFFFFF",
          border: "1px solid #E2E8F0",
          borderRadius: "14px",
          boxShadow: "0 20px 50px rgba(15, 23, 42, 0.15)",
          textAlign: "center"
        }}
      >
        <h3 style={{ color: "#0F172A", fontSize: "15px", fontWeight: 700, marginBottom: "8px", margin: 0 }}>
          {title}
        </h3>
        <p style={{ color: "#64748B", fontSize: "12px", marginBottom: "20px", lineHeight: "1.5", marginTop: "4px" }}>
          {message}
        </p>
        <div style={{ display: "flex", gap: "10px", justifyContent: "center" }}>
          <button 
            className="gs-btn-outline" 
            onClick={onCancel}
            style={{ padding: "8px 16px", fontSize: "12px", borderRadius: "8px" }}
          >
            Cancel
          </button>
          <button 
            className="gs-btn-primary" 
            onClick={onConfirm}
            style={{ 
              padding: "8px 16px", 
              fontSize: "12px", 
              borderRadius: "8px",
              background: "linear-gradient(135deg, #6C47FF, #8B5CF6)", 
              border: "none", 
              color: "#fff",
              cursor: "pointer",
              fontWeight: 600
            }}
          >
            Confirm
          </button>
        </div>
      </div>
    </div>
  );
}
