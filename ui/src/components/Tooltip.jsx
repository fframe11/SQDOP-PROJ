import React, { useState } from "react";

export default function Tooltip({ text }) {
  const [visible, setVisible] = useState(false);

  return (
    <span 
      style={{ 
        position: "relative", 
        display: "inline-flex", 
        alignItems: "center", 
        justifyContent: "center",
        marginLeft: "6px",
        cursor: "help",
        color: "var(--accent-blue)",
        fontSize: "0.8rem",
        fontWeight: "bold",
        width: "14px",
        height: "14px",
        borderRadius: "50%",
        border: "1px solid var(--accent-blue)",
        userSelect: "none"
      }}
      onMouseEnter={() => setVisible(true)}
      onMouseLeave={() => setVisible(false)}
    >
      ?
      {visible && (
        <span 
          style={{
            position: "absolute",
            bottom: "120%",
            left: "50%",
            transform: "translateX(-50%)",
            background: "rgba(10, 15, 30, 0.95)",
            backdropFilter: "blur(8px)",
            border: "1px solid rgba(255, 255, 255, 0.12)",
            color: "#e2e8f0",
            padding: "8px 12px",
            borderRadius: "6px",
            fontSize: "0.75rem",
            fontWeight: "normal",
            whiteSpace: "normal",
            width: "220px",
            boxShadow: "0 4px 12px rgba(0, 0, 0, 0.4)",
            zIndex: 1000,
            pointerEvents: "none",
            lineHeight: "1.3"
          }}
        >
          {text}
        </span>
      )}
    </span>
  );
}
