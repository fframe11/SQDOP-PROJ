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
        className="glass-card" 
        style={{
          width: "400px",
          padding: "2rem",
          background: "rgba(10, 15, 30, 0.85)",
          border: "1px solid rgba(255, 255, 255, 0.1)",
          boxShadow: "0 10px 25px rgba(0, 0, 0, 0.5)",
          textAlign: "center"
        }}
      >
        <h3 style={{ color: "#fff", fontSize: "1.1rem", fontWeight: 600, marginBottom: "1rem" }}>
          {title}
        </h3>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", marginBottom: "2rem", lineHeight: "1.4" }}>
          {message}
        </p>
        <div style={{ display: "flex", gap: "1rem", justifyContent: "center" }}>
          <button 
            className="btn btn-secondary" 
            onClick={onCancel}
            style={{ padding: "0.5rem 1.5rem", fontSize: "0.85rem", cursor: "pointer" }}
          >
            Cancel
          </button>
          <button 
            className="btn btn-primary" 
            onClick={onConfirm}
            style={{ 
              padding: "0.5rem 1.5rem", 
              fontSize: "0.85rem", 
              background: "var(--accent-blue)", 
              border: "none", 
              cursor: "pointer" 
            }}
          >
            Confirm
          </button>
        </div>
      </div>
    </div>
  );
}
