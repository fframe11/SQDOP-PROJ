import React, { useState } from "react";
import { BrowserRouter, Routes, Route, useLocation } from "react-router-dom";
import NavBar from "./components/NavBar";
import ErrorBoundary from "./components/ErrorBoundary";
import Home from "./pages/Home";
import Dashboard from "./pages/Dashboard";
import Analytics from "./pages/Analytics";
import Pipeline from "./pages/Pipeline";
import Schema from "./pages/Schema";
import Ingestion from "./pages/Ingestion";
import DataExport from "./pages/DataExport";
import RulesConfig from "./pages/RulesConfig";
import "./App.css";

function AppContent({ isSidebarOpen, toggleSidebar }) {
  const location = useLocation();
  const isHome = location.pathname === "/";

  return (
    <>
      <div className="app-bg" />
      
      {/* Top Black Banner (Only rendered on Home page) */}
      {isHome && (
        <div className="top-banner">
          <span>SDOQAP Platform <span className="banner-badge">v2.0</span> is now live</span>
        </div>
      )}
      
      {/* Clerk White Header has been removed completely as requested */}

      <div className={`app-layout ${isSidebarOpen ? "sidebar-visible" : "sidebar-hidden"} ${isHome ? "layout-with-banner" : "layout-full-height"}`}>
        <NavBar
          isOpen={isSidebarOpen}
          toggleSidebar={toggleSidebar}
          isSidebarOpen={isSidebarOpen}
        />
        <main className="app-main-content">
          {!isSidebarOpen && (
            <button
              className="sidebar-toggle-btn"
              onClick={toggleSidebar}
              title="Open Sidebar"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <line x1="4" y1="12" x2="20" y2="12" />
                <line x1="4" y1="6" x2="20" y2="6" />
                <line x1="4" y1="18" x2="20" y2="18" />
              </svg>
            </button>
          )}
          <ErrorBoundary>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/dashboard" element={<Dashboard />} />
              <Route path="/analytics" element={<Analytics />} />
              <Route path="/pipeline" element={<Pipeline />} />
              <Route path="/schema" element={<Schema />} />
              <Route path="/rules" element={<RulesConfig />} />
              <Route path="/rules-config" element={<RulesConfig />} />
              <Route path="/rules_config" element={<RulesConfig />} />
              <Route path="/rules config" element={<RulesConfig />} />
              <Route path="/ingestion" element={<Ingestion />} />
              <Route path="/export" element={<DataExport />} />
            </Routes>
          </ErrorBoundary>
        </main>
      </div>
    </>
  );
}

export default function App() {
  const [isSidebarOpen, setIsSidebarOpen] = useState(false); // Collapsed by default

  const toggleSidebar = () => {
    setIsSidebarOpen(prev => !prev);
  };

  return (
    <BrowserRouter>
      <AppContent isSidebarOpen={isSidebarOpen} toggleSidebar={toggleSidebar} />
    </BrowserRouter>
  );
}

