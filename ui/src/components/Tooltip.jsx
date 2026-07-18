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
        color: "var(--accent-purple)",
        fontSize: "11px",
        fontWeight: "bold",
        width: "14px",
        height: "14px",
        borderRadius: "50%",
        border: "1px solid var(--accent-purple)",
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
            background: "#1E293B",
            color: "#FFFFFF",
            padding: "8px 12px",
            borderRadius: "8px",
            fontSize: "11px",
            fontWeight: "normal",
            whiteSpace: "normal",
            width: "220px",
            boxShadow: "0 10px 25px rgba(15, 23, 42, 0.15)",
            zIndex: 1000,
            pointerEvents: "none",
            lineHeight: "1.4"
          }}
        >
          {text}
        </span>
      )}
    </span>
  );
}
