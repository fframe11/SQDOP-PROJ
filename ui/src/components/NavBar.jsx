import React from "react";
import { Link, useLocation } from "react-router-dom";

export default function NavBar() {
  const location = useLocation();

  const links = [
    { to: "/", label: "Home" },
    { to: "/dashboard", label: "Dashboard" },
    { to: "/analytics", label: "Analytics" },
    { to: "/pipeline", label: "Pipeline" },
    { to: "/schema", label: "Schema" },
    { to: "/rules", label: "Rules Hub" },
    { to: "/ingestion", label: "Ingestion" },
    { to: "/export", label: "Data Export" },
  ];

  return (
    <nav className="navbar">
      <Link to="/" className="navbar-brand">
        <span className="brand-icon">S</span>
        <span>SDOQAP</span>
        <span className="brand-sub">&nbsp;Data Observability Platform</span>
      </Link>
      
      <div className="navbar-links">
        {links.map((link) => (
          <Link
            key={link.to}
            to={link.to}
            className={`nav-link${location.pathname === link.to ? " active" : ""}`}
          >
            {link.label}
          </Link>
        ))}
      </div>
    </nav>
  );
}
