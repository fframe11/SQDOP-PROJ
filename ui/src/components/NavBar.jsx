import React, { useState, useEffect, useRef } from "react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { useApi } from "../hooks/useApi";

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

const ChevronIcon = ({ isOpen }) => (
  <svg
    width="12"
    height="12"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    style={{
      transform: isOpen ? "rotate(0deg)" : "rotate(-90deg)",
      transition: "transform 0.2s ease",
      opacity: 0.5,
    }}
  >
    <polyline points="6 9 12 15 18 9" />
  </svg>
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

  // Accordion open/close state
  const [openSections, setOpenSections] = useState({
    general: true,
    observability: true,
    governance: true,
    pipeline_stages: true
  });

  const toggleSection = (key) => {
    setOpenSections(prev => ({
      ...prev,
      [key]: !prev[key]
    }));
  };

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

  return (
    <>
      <aside className="sidebar">
        {/* Brand Header */}
        <div className="sidebar-brand">
          <Link to="/" className="brand-logo">
            <span className="brand-icon">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
              </svg>
            </span>
            <div className="brand-text">
              <span className="brand-name">sdoqap</span>
              <span className="brand-badge">OBSERVABILITY</span>
            </div>
          </Link>
          <button className="sidebar-close-btn" onClick={toggleSidebar} title="Collapse Sidebar">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>

        {/* Quick Search Trigger */}
        <div className="sidebar-search" onClick={() => setShowSearchModal(true)}>
          <span className="search-input-icon"><SearchIcon /></span>
          <span className="search-placeholder">Search page...</span>
          <span className="shortcut-badge">Ctrl K</span>
        </div>

        {/* Navigation Categories */}
        <div className="sidebar-menu">
          {menuGroups.map((group) => {
            const isOpen = openSections[group.key];
            return (
              <div key={group.key} className="menu-group">
                <div className="menu-group-header" onClick={() => toggleSection(group.key)}>
                  <ChevronIcon isOpen={isOpen} />
                  <span>{group.title}</span>
                </div>
                {isOpen && (
                  <div className="menu-group-items">
                    {group.links.map((link) => {
                      const isActive = location.pathname === link.to;
                      return (
                        <Link
                          key={link.to}
                          to={link.to}
                          className={`menu-item${isActive ? " active" : ""}`}
                        >
                          <span className="item-icon">{link.icon}</span>
                          <span className="item-label">{link.label}</span>
                        </Link>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Bottom System Status */}
        <div className="sidebar-footer">
          <div className="system-health">
            <span className={`health-dot ${isHealthy ? "healthy" : "warning"}`} />
            <span className="health-label">
              {isHealthy ? "API Status: Healthy" : "API Connected with Warnings"}
            </span>
          </div>
          <div className="footer-links">
            <a href="https://github.com" target="_blank" rel="noreferrer" className="footer-icon-link" title="GitHub Repository">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/></svg>
            </a>
            <div className="footer-version">v0.1.0</div>
          </div>
        </div>
      </aside>

      {/* Command Palette Modal */}
      {showSearchModal && (
        <div className="command-palette-overlay" onClick={() => setShowSearchModal(false)}>
          <div className="command-palette-container" onClick={(e) => e.stopPropagation()}>
            <div className="command-palette-header">
              <span className="modal-search-icon"><SearchIcon /></span>
              <input
                ref={searchInputRef}
                type="text"
                placeholder="Type a page name or category to navigate..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setSelectedIndex(0);
                }}
                onKeyDown={handleModalKeyDown}
                className="modal-search-input"
              />
              <span className="esc-badge" onClick={() => setShowSearchModal(false)}>ESC</span>
            </div>

            <div className="command-palette-results">
              {filteredLinks.length > 0 ? (
                filteredLinks.map((link, idx) => (
                  <div
                    key={link.to}
                    className={`result-item${selectedIndex === idx ? " selected" : ""}`}
                    onClick={() => {
                      navigate(link.to);
                      setShowSearchModal(false);
                    }}
                    onMouseEnter={() => setSelectedIndex(idx)}
                  >
                    <span className="result-icon">{link.icon}</span>
                    <div className="result-details">
                      <span className="result-label">{link.label}</span>
                      <span className="result-category">{link.category}</span>
                    </div>
                    <span className="arrow-enter-indicator">↵ Navigate</span>
                  </div>
                ))
              ) : (
                <div className="no-results-state">
                  No pages found matching "{searchQuery}"
                </div>
              )}
            </div>
            
            <div className="command-palette-footer">
              <span className="help-pills">
                <kbd>↑↓</kbd> to navigate
              </span>
              <span className="help-pills">
                <kbd>Enter</kbd> to select
              </span>
              <span className="help-pills">
                <kbd>ESC</kbd> to close
              </span>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
