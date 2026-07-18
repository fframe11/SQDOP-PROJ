import React, { useState, useEffect, useRef } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { useApi } from "../hooks/useApi";
import "./NavBar.css";

// --- SVG Icons ---
const HomeIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
);

const DashboardIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="3" width="7" height="9"/><rect x="14" y="3" width="7" height="5"/><rect x="14" y="12" width="7" height="9"/><rect x="3" y="16" width="7" height="5"/></svg>
);

const AnalyticsIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
);

const PipelineIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
);

const SchemaIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/><path d="M3 12c0 1.66 4 3 9 3s9-1.34 9-3"/></svg>
);

const RulesIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/><line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/><line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/><line x1="1" y1="14" x2="7" y2="14"/><line x1="9" y1="8" x2="15" y2="8"/><line x1="17" y1="16" x2="23" y2="16"/></svg>
);

const IngestionIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
);

const ExportIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
);

const SearchIcon = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
);

export default function NavBar({ isOpen, toggleSidebar, isSidebarOpen }) {
  const location = useLocation();
  const navigate = useNavigate();
  
  // Fetch services status for bottom panel
  const services = useApi('/services/status', { refreshInterval: 15000 });
  const isHealthy = !services.error && services.data && Object.values(services.data).every(s => s.status === 'online');

  // Categories and their links
  const menuGroups = [
    {
      title: "General",
      key: "general",
      links: [
        { to: "/", label: "Home", icon: <HomeIcon /> },
        { to: "/dashboard", label: "Dashboard", icon: <DashboardIcon /> }
      ]
    },
    {
      title: "Observability",
      key: "observability",
      links: [
        { to: "/analytics", label: "Live Analytics", icon: <AnalyticsIcon /> },
        { to: "/pipeline", label: "Pipeline Runs", icon: <PipelineIcon /> }
      ]
    },
    {
      title: "Governance",
      key: "governance",
      links: [
        { to: "/schema", label: "Schema Drift", icon: <SchemaIcon /> },
        { to: "/rules", label: "Rules Hub", icon: <RulesIcon /> }
      ]
    },
    {
      title: "Data Pipeline",
      key: "pipeline_stages",
      links: [
        { to: "/ingestion", label: "Ingestion Stage", icon: <IngestionIcon /> },
        { to: "/export", label: "Export Hub", icon: <ExportIcon /> }
      ]
    }
  ];

  // Command Palette states
  const [showSearchModal, setShowSearchModal] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedIndex, setSelectedIndex] = useState(0);
  const searchInputRef = useRef(null);

  // Flatten all links for search
  const allLinks = menuGroups.reduce((acc, group) => {
    return [...acc, ...group.links.map(l => ({ ...l, category: group.title }))];
  }, []);

  const filteredLinks = allLinks.filter(link =>
    link.label.toLowerCase().includes(searchQuery.toLowerCase()) ||
    link.category.toLowerCase().includes(searchQuery.toLowerCase())
  );

  // Toggle Command Palette shortcuts
  useEffect(() => {
    const handleKeyDown = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setShowSearchModal(prev => !prev);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  // Autofocus input when modal opens
  useEffect(() => {
    if (showSearchModal) {
      setSearchQuery("");
      setSelectedIndex(0);
      setTimeout(() => {
        if (searchInputRef.current) searchInputRef.current.focus();
      }, 50);
    }
  }, [showSearchModal]);

  // Keyboard navigation inside Command Palette
  const handleModalKeyDown = (e) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIndex(prev => (prev + 1) % filteredLinks.length);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIndex(prev => (prev - 1 + filteredLinks.length) % filteredLinks.length);
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (filteredLinks[selectedIndex]) {
        navigate(filteredLinks[selectedIndex].to);
        setShowSearchModal(false);
      }
    } else if (e.key === "Escape") {
      setShowSearchModal(false);
    }
  };

  const navOpen = isSidebarOpen !== undefined ? isSidebarOpen : isOpen;

  return (
    <>
      <aside className={`gs-nav ${navOpen ? "open" : "closed"}`}>
        {/* Sidebar Header */}
        <div className="gs-nav-logo">
          <Link to="/" className="gs-nav-brand">
            <div className="gs-nav-logo-icon">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
              </svg>
            </div>
            {navOpen && (
              <div className="gs-nav-logo-text">
                <span className="gs-nav-logo-name">SDOQAP</span>
                <span className="gs-nav-logo-sub">Observability</span>
              </div>
            )}
          </Link>
          {navOpen && (
            <button className="gs-nav-toggle" onClick={toggleSidebar} title="Collapse Sidebar">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
              </svg>
            </button>
          )}
        </div>

        {/* Search Bar */}
        {navOpen ? (
          <div className="gs-nav-search" onClick={() => setShowSearchModal(true)}>
            <SearchIcon />
            <span>Search page...</span>
            <kbd>Ctrl K</kbd>
          </div>
        ) : (
          <div className="gs-nav-search" style={{ justifyContent: "center", padding: "8px 0" }} onClick={() => setShowSearchModal(true)}>
            <SearchIcon />
          </div>
        )}

        {/* Navigation Categories */}
        <div className="gs-nav-groups">
          {menuGroups.map((group) => (
            <div className="gs-nav-group" key={group.key}>
              {navOpen && <span className="gs-nav-group-label">{group.title}</span>}
              <div className="gs-nav-items">
                {group.links.map((link) => {
                  const isActive = location.pathname === link.to;
                  return (
                    <Link
                      key={link.to}
                      to={link.to}
                      className={`gs-nav-item ${isActive ? "active" : ""}`}
                      title={link.label}
                    >
                      <span className="gs-nav-icon">{link.icon}</span>
                      {navOpen && <span className="gs-nav-label">{link.label}</span>}
                    </Link>
                  );
                })}
              </div>
            </div>
          ))}
        </div>

        {/* Bottom System Status */}
        <div className="gs-nav-bottom">
          {navOpen && (
            <div className="gs-nav-status">
              <span className={`gs-nav-status-dot ${isHealthy ? "online" : "offline"}`} />
              <span className="gs-nav-status-text">
                {isHealthy ? "API: HEALTHY" : "API: OFFLINE"}
              </span>
            </div>
          )}
          <div className="gs-nav-version" style={{ justifyContent: navOpen ? "space-between" : "center" }}>
            {navOpen && (
              <a href="https://github.com/ohmiler/gridgeist" target="_blank" rel="noreferrer" style={{ color: "var(--text-muted)", display: "flex" }}>
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/></svg>
              </a>
            )}
            <span style={{ fontSize: "9px" }}>v1.1.0</span>
          </div>
        </div>
      </aside>

      {/* Command Palette Modal */}
      {showSearchModal && (
        <div className="gs-search-overlay" onClick={() => setShowSearchModal(false)}>
          <div className="gs-search-modal" onClick={(e) => e.stopPropagation()}>
            <div style={{ display: "flex", alignItems: "center" }}>
              <input
                ref={searchInputRef}
                type="text"
                placeholder="Type page name to navigate..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setSelectedIndex(0);
                }}
                onKeyDown={handleModalKeyDown}
              />
            </div>

            <div style={{ maxHeight: "300px", overflowY: "auto" }}>
              {filteredLinks.length > 0 ? (
                filteredLinks.map((link, idx) => (
                  <div
                    key={link.to}
                    className={`gs-search-result`}
                    style={{
                      background: selectedIndex === idx ? "var(--accent-purple-light)" : "transparent",
                      color: selectedIndex === idx ? "var(--accent-purple)" : "var(--text-main)"
                    }}
                    onClick={() => {
                      navigate(link.to);
                      setShowSearchModal(false);
                    }}
                    onMouseEnter={() => setSelectedIndex(idx)}
                  >
                    <span>{link.label}</span>
                    <span style={{ marginLeft: "auto", fontSize: "10px", color: "var(--text-muted)" }}>{link.category}</span>
                  </div>
                ))
              ) : (
                <div className="gs-search-empty">
                  No pages found matching "{searchQuery}"
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </>
  );
}
