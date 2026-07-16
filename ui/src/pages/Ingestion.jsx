import React, { useState, useEffect, useRef } from "react";
import { postApi } from "../hooks/useApi";

export default function Ingestion() {
  // CSV Upload State
  const [csvTableName, setCsvTableName] = useState("");
  const [csvFile, setCsvFile] = useState(null);
  const [csvStatus, setCsvStatus] = useState(null);
  const [csvDragging, setCsvDragging] = useState(false);

  // API Ingest State
  const [apiTableName, setApiTableName] = useState("");
  const [apiUrl, setApiUrl] = useState("");
  const [apiMethod, setApiMethod] = useState("GET");
  const [apiInterval, setApiInterval] = useState(10);
  const [apiStatus, setApiStatus] = useState(null);
  const [apiKey, setApiKey] = useState("");

  // RDBMS Ingest State
  const [rdbmsTableName, setRdbmsTableName] = useState("");
  const [dbType, setDbType] = useState("postgresql");
  const [dbHost, setDbHost] = useState("");
  const [dbPort, setDbPort] = useState(5432);
  const [dbUser, setDbUser] = useState("");
  const [dbPass, setDbPass] = useState("");
  const [dbName, setDbName] = useState("");
  const [dbQuery, setDbQuery] = useState("");
  const [rdbmsStatus, setRdbmsStatus] = useState(null);
  const [isRdbmsUnlocked, setIsRdbmsUnlocked] = useState(false);
  const [showRdbmsLearnMore, setShowRdbmsLearnMore] = useState(false);
  const [hasAcknowledgedRdbms, setHasAcknowledgedRdbms] = useState(false);

  // Reddit Streaming State
  const [redditTopic, setRedditTopic] = useState("#Technology, #AI");
  const [redditDuration, setRedditDuration] = useState(40);
  const [redditSubreddits, setRedditSubreddits] = useState("python,bigdata");
  const [redditStatus, setRedditStatus] = useState(null);
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamInfo, setStreamInfo] = useState({
    status: "idle",
    remaining: 0,
    elapsed: 0,
    logs: []
  });

  const terminalEndRef = useRef(null);

  // Poll ingestion / streaming status continuously on mount
  useEffect(() => {
    const checkStatus = async () => {
      try {
        const response = await fetch("/api/v1/pipeline/ingest/reddit/status");
        if (response.ok) {
          const data = await response.json();
          setStreamInfo(data);
          setIsStreaming(data.status === "running");
        }
      } catch (err) {
        console.error("Failed to fetch stream status:", err);
      }
    };

    checkStatus(); // immediate check
    const intervalId = setInterval(checkStatus, 2000);

    return () => {
      clearInterval(intervalId);
    };
  }, []);

  // Scroll to bottom of terminal
  useEffect(() => {
    if (terminalEndRef.current) {
      terminalEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [streamInfo.logs]);

  // CSV Drag and Drop Handlers
  const handleDragOver = (e) => {
    e.preventDefault();
    setCsvDragging(true);
  };

  const handleDragLeave = () => {
    setCsvDragging(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setCsvDragging(false);
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      const file = e.dataTransfer.files[0];
      if (file.name.endsWith(".csv")) {
        setCsvFile(file);
        if (!csvTableName) {
          setCsvTableName(file.name.replace(".csv", "").replace(/[^a-zA-Z0-9_]/g, "_"));
        }
      } else {
        setCsvStatus({ success: false, message: "Only CSV files are supported." });
      }
    }
  };

  const handleFileChange = (e) => {
    if (e.target.files && e.target.files.length > 0) {
      const file = e.target.files[0];
      setCsvFile(file);
      if (!csvTableName) {
        setCsvTableName(file.name.replace(".csv", "").replace(/[^a-zA-Z0-9_]/g, "_"));
      }
    }
  };

  // Submit CSV Ingestion
  const handleCsvSubmit = async (e) => {
    e.preventDefault();
    if (!csvTableName || !csvFile) {
      setCsvStatus({ success: false, message: "Please provide both filename and CSV file." });
      return;
    }

    setCsvStatus({ loading: true, message: "Uploading HDFS raw store and running Spark quality engine..." });
    try {
      const formData = new FormData();
      formData.append("table_name", csvTableName);
      formData.append("file", csvFile);

      const response = await fetch("/api/v1/pipeline/ingest/csv", {
        method: "POST",
        body: formData
      });
      const res = await response.json();

      if (response.ok) {
        setCsvStatus({ success: true, message: res.message });
        setCsvFile(null);
        setCsvTableName("");
      } else {
        setCsvStatus({ success: false, message: res.detail || "Upload failed." });
      }
    } catch (err) {
      setCsvStatus({ success: false, message: `Error: ${err.message}` });
    }
  };

  // Submit API Ingestion
  const handleApiSubmit = async (e) => {
    e.preventDefault();
    if (!apiTableName || !apiUrl) {
      setApiStatus({ success: false, message: "Please provide table name and API URL." });
      return;
    }

    setApiStatus({ loading: true, message: "Fetching REST endpoint data and triggering Spark..." });
    try {
      const res = await postApi("/pipeline/ingest/api", {
        table_name: apiTableName,
        url: apiUrl,
        api_key: apiKey || null
      });
      setApiStatus({ success: true, message: res.message });
      setApiTableName("");
      setApiUrl("");
      setApiKey("");
    } catch (err) {
      setApiStatus({ success: false, message: `Error: ${err.message}` });
    }
  };

  // Toggle Reddit Ingestion
  const handleRedditToggle = async () => {
    if (isStreaming) {
      // stop
      try {
        const res = await postApi("/pipeline/ingest/reddit/stop");
        setRedditStatus({ success: true, message: res.message });
        setIsStreaming(false);
      } catch (err) {
        setRedditStatus({ success: false, message: `Error: ${err.message}` });
      }
    } else {
      // start
      setRedditStatus(null);
      try {
        const res = await postApi("/pipeline/ingest/reddit", {
          subreddits: redditSubreddits,
          duration: parseInt(redditDuration)
        });
        setIsStreaming(true);
        setRedditStatus({ success: true, message: res.message });
      } catch (err) {
        setRedditStatus({ success: false, message: `Error: ${err.message}` });
      }
    }
  };

  const handleRdbmsSubmit = async (e) => {
    e.preventDefault();
    if (!rdbmsTableName || !dbHost || !dbPort || !dbUser || !dbName || !dbQuery) {
      setRdbmsStatus({ success: false, message: "Please fill in all required fields." });
      return;
    }

    setRdbmsStatus({ loading: true, message: "Connecting to database and running Spark ingestion..." });
    try {
      const res = await postApi("/pipeline/ingest/rdbms", {
        table_name: rdbmsTableName,
        db_type: dbType,
        host: dbHost,
        port: parseInt(dbPort),
        username: dbUser,
        password: dbPass,
        database: dbName,
        query: dbQuery
      });
      setRdbmsStatus({ success: true, message: res.message });
      setRdbmsTableName("");
      setDbHost("");
      setDbUser("");
      setDbPass("");
      setDbName("");
      setDbQuery("");
    } catch (err) {
      setRdbmsStatus({ success: false, message: `Error: ${err.message}` });
    }
  };

  const getPercentage = () => {
    if (streamInfo.duration === 0) return 0;
    return (streamInfo.elapsed / streamInfo.duration) * 100;
  };

  return (
    <div className="page-container animate-in" style={{ paddingBottom: "2rem" }}>
      {/* Dynamic Styling Overrides to achieve the premium glassmorphism look from mockup */}
      <style>{`
        .custom-glass-card {
          background: #FFFFFF;
          border: 1px solid #E2E8F0;
          border-radius: 16px;
          padding: 1.5rem;
          box-shadow: 0 4px 20px rgba(15, 23, 42, 0.02);
          transition: all 0.3s ease;
          display: flex;
          flex-direction: column;
          justify-content: space-between;
          height: 100%;
        }
        .custom-glass-card:hover {
          border-color: rgba(108, 71, 255, 0.35);
          box-shadow: 0 4px 20px rgba(108, 71, 255, 0.05);
          transform: translateY(-2px);
        }
        .custom-input {
          width: 100%;
          padding: 0.65rem;
          border-radius: 8px;
          background: #FFFFFF;
          border: 1px solid #E2E8F0;
          color: var(--text-main);
          font-family: inherit;
          font-size: 0.85rem;
          transition: all 0.2s ease;
          margin-top: 0.25rem;
        }
        .custom-input:focus {
          border-color: var(--accent-indigo);
          outline: none;
          box-shadow: 0 0 8px rgba(108, 71, 255, 0.2);
        }
        .custom-label {
          display: block;
          font-size: 0.8rem;
          color: var(--text-muted);
          font-weight: 500;
        }
        .custom-button {
          width: 100%;
          background: rgba(108, 71, 255, 0.08);
          border: 1px solid rgba(108, 71, 255, 0.35);
          border-radius: 8px;
          color: #6C47FF;
          padding: 0.75rem;
          font-weight: 600;
          font-size: 0.85rem;
          cursor: pointer;
          transition: all 0.2s ease;
          margin-top: 1rem;
        }
        .custom-button:hover:not(:disabled) {
          background: rgba(108, 71, 255, 0.15);
          border-color: rgba(108, 71, 255, 0.7);
          box-shadow: 0 0 10px rgba(108, 71, 255, 0.15);
        }
        .custom-button:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }
        .custom-slider {
          width: 100%;
          accent-color: #6C47FF;
          background: #E2E8F0;
          height: 5px;
          border-radius: 3px;
          outline: none;
          margin-top: 0.5rem;
        }
        /* Toggle Switch styling */
        .switch-container {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-top: 1rem;
        }
        .switch-label {
          font-size: 0.85rem;
          font-weight: 600;
          color: var(--text-main);
        }
        .switch {
          position: relative;
          display: inline-block;
          width: 44px;
          height: 22px;
        }
        .switch input {
          opacity: 0;
          width: 0;
          height: 0;
        }
        .slider {
          position: absolute;
          cursor: pointer;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          background-color: #E2E8F0;
          border: 1px solid #CBD5E1;
          transition: .4s;
          border-radius: 34px;
        }
        .slider:before {
          position: absolute;
          content: "";
          height: 14px;
          width: 14px;
          left: 3px;
          bottom: 3px;
          background-color: var(--text-muted);
          transition: .4s;
          border-radius: 50%;
        }
        input:checked + .slider {
          background-color: rgba(108, 71, 255, 0.2);
          border-color: #6C47FF;
        }
        input:checked + .slider:before {
          transform: translateX(22px);
          background-color: #6C47FF;
        }
        .terminal-header {
          background: #F1F5F9;
          border: 1px solid #E2E8F0;
          border-bottom: none;
          border-radius: 12px 12px 0 0;
          padding: 0.6rem 1rem;
          display: flex;
          align-items: center;
          gap: 6px;
        }
        .terminal-dot {
          width: 11px;
          height: 11px;
          border-radius: 50%;
          display: inline-block;
        }
        .terminal-body {
          background: #0F172A;
          border: 1px solid #E2E8F0;
          border-radius: 0 0 12px 12px;
          height: 320px;
          overflow-y: auto;
          padding: 1.25rem;
          font-family: var(--font-mono);
          font-size: 0.8rem;
          line-height: 1.5;
          color: #cbd5e1;
          box-shadow: inset 0 8px 24px rgba(0,0,0,0.15);
        }
      `}</style>

      <div style={{ marginBottom: "2rem", width: "100%" }}>
        <div style={{ fontSize: "11px", color: "var(--text-muted)", marginBottom: "0.4rem", fontFamily: "var(--font-sans)", display: "flex", gap: "6px", alignItems: "center" }}>
          <span>SDOQAP Data Engine</span>
          <span style={{ opacity: 0.5 }}>&gt;</span>
          <span style={{ color: "var(--text-main)", fontWeight: 500 }}>Ingestion Stage</span>
        </div>
        <h1 style={{ fontSize: "1.6rem", fontWeight: 700, color: "var(--text-main)", letterSpacing: "-0.02em", margin: 0 }}>Ingestion Stage</h1>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", margin: "4px 0 0 0" }}>Drag &amp; drop datasets, fetch external APIs, or scrape live real-time feeds into the platform</p>
      </div>

      <div className="card-grid" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: "1.5rem", marginBottom: "2rem" }}>
        
        {/* Local CSV Dataset Card */}
        <div className="custom-glass-card">
          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
            <h3 style={{ fontSize: "1.05rem", fontWeight: 700, display: "flex", alignItems: "center", gap: "0.5rem" }}>
              Local CSV Dataset
            </h3>
            
            <form onSubmit={handleCsvSubmit} style={{ display: "flex", flexDirection: "column", gap: "0.85rem", marginTop: "0.5rem" }}>
              {/* Drag and Drop Zone */}
              <div
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
                style={{
                  border: csvDragging ? "1.5px dashed var(--accent-indigo)" : "1px dashed #CBD5E1",
                  borderRadius: "12px",
                  padding: "1.75rem 1rem",
                  textAlign: "center",
                  background: csvDragging ? "rgba(108, 71, 255, 0.05)" : "#FAFAFC",
                  cursor: "pointer",
                  transition: "all 0.2s ease"
                }}
                onClick={() => document.getElementById("csvFileSelect").click()}
              >
                <input
                  type="file"
                  id="csvFileSelect"
                  accept=".csv"
                  onChange={handleFileChange}
                  style={{ display: "none" }}
                />
                
                {/* Cloud Upload Icon */}
                <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke={csvFile ? "#10b981" : "var(--accent-indigo)"} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" style={{ marginBottom: "0.5rem" }}>
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                  <polyline points="17 8 12 3 7 8" />
                  <line x1="12" y1="3" x2="12" y2="15" />
                </svg>

                {csvFile ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
                    <span style={{ color: "#10b981", fontWeight: 700, fontSize: "0.85rem" }}>{csvFile.name}</span>
                    <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>Ready to validate</span>
                  </div>
                ) : (
                  <div style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
                    <span style={{ fontSize: "0.85rem", fontWeight: 600 }}>Drag &amp; Drop CSV file or <span style={{ color: "var(--accent-indigo)", textDecoration: "underline" }}>browse</span></span>
                    <span style={{ fontSize: "0.7rem", color: "var(--text-muted)" }}>supports .csv, .tsv, max 500MB</span>
                  </div>
                )}
              </div>

              <div style={{ marginTop: "0.25rem" }}>
                <label className="custom-label">Filename</label>
                <input
                  type="text"
                  placeholder="e.g. sales_data"
                  value={csvTableName}
                  onChange={(e) => setCsvTableName(e.target.value.replace(/[^a-zA-Z0-9_]/g, "_"))}
                  className="custom-input"
                  required
                />
              </div>

              {csvStatus && (
                <div
                  className={`alert-box ${csvStatus.loading ? "info" : csvStatus.success ? "success" : "critical"}`}
                  style={{ fontSize: "0.8rem", padding: "0.5rem", borderRadius: "6px" }}
                >
                  {csvStatus.message}
                </div>
              )}

              <button
                type="submit"
                className="custom-button"
                disabled={csvStatus?.loading || !csvFile || !csvTableName}
              >
                {csvStatus?.loading ? "Validating..." : "Validate Data"}
              </button>
            </form>
          </div>
        </div>

        {/* JSON API Fetcher Card */}
        <div className="custom-glass-card">
          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
            <h3 style={{ fontSize: "1.05rem", fontWeight: 700, display: "flex", alignItems: "center", gap: "0.5rem" }}>
              JSON API Fetcher
            </h3>
            
            <form onSubmit={handleApiSubmit} style={{ display: "flex", flexDirection: "column", gap: "0.75rem", marginTop: "0.5rem" }}>
              <div>
                <label className="custom-label">Table Name</label>
                <input
                  type="text"
                  placeholder="e.g. api_products"
                  value={apiTableName}
                  onChange={(e) => setApiTableName(e.target.value.replace(/[^a-zA-Z0-9_]/g, "_"))}
                  className="custom-input"
                  required
                />
              </div>

              <div>
                <label className="custom-label">API URL</label>
                <input
                  type="url"
                  placeholder="https://api.store.com/v1/products"
                  value={apiUrl}
                  onChange={(e) => setApiUrl(e.target.value)}
                  className="custom-input"
                  required
                />
              </div>

              {/* API Key (Optional) Field */}
              <div>
                <label className="custom-label">API Key (Optional)</label>
                <input
                  type="password"
                  placeholder="Enter API Key / Token"
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                  className="custom-input"
                />
              </div>

              {/* Method Selector */}
              <div>
                <label className="custom-label">Method</label>
                <select
                  value={apiMethod}
                  onChange={(e) => setApiMethod(e.target.value)}
                  className="custom-input"
                  style={{ appearance: "none", backgroundImage: "linear-gradient(45deg, transparent 50%, var(--accent-indigo) 50%), linear-gradient(135deg, var(--accent-indigo) 50%, transparent 50%)", backgroundPosition: "calc(100% - 18px) calc(1em + 2px), calc(100% - 13px) calc(1em + 2px)", backgroundSize: "5px 5px, 5px 5px", backgroundRepeat: "no-repeat" }}
                >
                  <option value="GET">GET</option>
                  <option value="POST">POST</option>
                  <option value="PUT">PUT</option>
                </select>
              </div>

              {/* Refresh Interval Slider */}
              <div style={{ marginTop: "0.25rem" }}>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <label className="custom-label">Refresh Interval</label>
                  <span style={{ fontSize: "0.75rem", color: "var(--accent-indigo)", fontWeight: 700 }}>{apiInterval}s</span>
                </div>
                <input
                  type="range"
                  min="5"
                  max="60"
                  value={apiInterval}
                  onChange={(e) => setApiInterval(e.target.value)}
                  className="custom-slider"
                />
              </div>

              {apiStatus && (
                <div
                  className={`alert-box ${apiStatus.loading ? "info" : apiStatus.success ? "success" : "critical"}`}
                  style={{ fontSize: "0.8rem", padding: "0.5rem", borderRadius: "6px" }}
                >
                  {apiStatus.message}
                </div>
              )}

              <button
                type="submit"
                className="custom-button"
                disabled={apiStatus?.loading || !apiUrl || !apiTableName}
                style={{ marginTop: "0.5rem" }}
              >
                {apiStatus?.loading ? "Connecting..." : "Connect & Fetch"}
              </button>
            </form>
          </div>
        </div>

        {/* Reddit Real-time Stream Card */}
        <div className="custom-glass-card">
          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
            <h3 style={{ fontSize: "1.05rem", fontWeight: 700, display: "flex", alignItems: "center", gap: "0.5rem" }}>
              Reddit Real-time Stream
            </h3>
            
            <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem", marginTop: "0.5rem" }}>
              <div>
                <label className="custom-label">Topic/Keyword</label>
                <input
                  type="text"
                  placeholder="#Technology, #AI"
                  value={redditTopic}
                  onChange={(e) => setRedditTopic(e.target.value)}
                  className="custom-input"
                  disabled={isStreaming}
                />
              </div>

              {/* Duration Slider */}
              <div>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <label className="custom-label">Duration Slider</label>
                  <span style={{ fontSize: "0.75rem", color: "var(--accent-blue)", fontWeight: 700 }}>{redditDuration} seconds</span>
                </div>
                <input
                  type="range"
                  min="10"
                  max="120"
                  step="5"
                  value={redditDuration}
                  onChange={(e) => setRedditDuration(e.target.value)}
                  className="custom-slider"
                  disabled={isStreaming}
                />
              </div>

              <div>
                <label className="custom-label">Subreddits to follow</label>
                <input
                  type="text"
                  placeholder="Subreddits to follow (comma separated)"
                  value={redditSubreddits}
                  onChange={(e) => setRedditSubreddits(e.target.value)}
                  className="custom-input"
                  disabled={isStreaming}
                />
              </div>

              {redditStatus && (
                <div
                  className={`alert-box ${redditStatus.success ? "success" : "critical"}`}
                  style={{ fontSize: "0.8rem", padding: "0.5rem", borderRadius: "6px" }}
                >
                  {redditStatus.message}
                </div>
              )}

              {/* Toggle switch row */}
              <div className="switch-container">
                <span className="switch-label">Start Ingestion</span>
                <label className="switch">
                  <input
                    type="checkbox"
                    checked={isStreaming}
                    onChange={handleRedditToggle}
                  />
                  <span className="slider"></span>
                </label>
              </div>
            </div>
          </div>
        </div>

        {/* RDBMS Database Ingest Card */}
        <div className="custom-glass-card" style={{ position: "relative", overflow: "hidden" }}>
          {!isRdbmsUnlocked && (
            <div style={{
              position: "absolute",
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              background: "rgba(255, 255, 255, 0.8)",
              backdropFilter: "blur(5px)",
              display: "flex",
              flexDirection: "column",
              justifyContent: "center",
              alignItems: "center",
              padding: "1.5rem",
              textAlign: "center",
              zIndex: 10
            }}>
              {/* Padlock Icon */}
              <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="var(--accent-indigo)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" style={{ marginBottom: "1rem" }}>
                <rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
                <path d="M7 11V7a5 5 0 0 1 10 0v4" />
              </svg>
              <h3 style={{ fontSize: "1.05rem", fontWeight: 700, marginBottom: "0.5rem" }}>
                RDBMS Connection Locked
              </h3>
              <p style={{ fontSize: "0.8rem", color: "var(--text-muted)", marginBottom: "1.5rem", lineHeight: "1.5" }}>
                Direct database connections require network route validation (VPN or Intranet). Please review configuration requirements to unlock.
              </p>
              <button
                onClick={() => setShowRdbmsLearnMore(true)}
                className="custom-button"
                style={{ width: "auto", padding: "0.5rem 1.5rem", margin: 0 }}
              >
                Learn More &amp; Unlock
              </button>
            </div>
          )}

          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem", opacity: isRdbmsUnlocked ? 1 : 0.3, pointerEvents: isRdbmsUnlocked ? "auto" : "none", height: "100%" }}>
            <h3 style={{ fontSize: "1.05rem", fontWeight: 700, display: "flex", alignItems: "center", gap: "0.5rem" }}>
              RDBMS Database Ingestion
            </h3>
            
            <form onSubmit={handleRdbmsSubmit} style={{ display: "flex", flexDirection: "column", gap: "0.6rem", marginTop: "0.25rem" }}>
              <div style={{ display: "flex", gap: "0.5rem" }}>
                <div style={{ flex: 1 }}>
                  <label className="custom-label">Target Table</label>
                  <input
                    type="text"
                    placeholder="e.g. pg_sales"
                    value={rdbmsTableName}
                    onChange={(e) => setRdbmsTableName(e.target.value.replace(/[^a-zA-Z0-9_]/g, "_"))}
                    className="custom-input"
                    required
                  />
                </div>
                <div style={{ flex: 1 }}>
                  <label className="custom-label">Database Type</label>
                  <select
                    value={dbType}
                    onChange={(e) => {
                      setDbType(e.target.value);
                      if (e.target.value === "postgresql") setDbPort(5432);
                      else if (e.target.value === "mysql") setDbPort(3306);
                      else if (e.target.value === "sqlserver") setDbPort(1433);
                      else if (e.target.value === "oracle") setDbPort(1521);
                    }}
                    className="custom-input"
                  >
                    <option value="postgresql">PostgreSQL</option>
                    <option value="mysql">MySQL</option>
                    <option value="sqlserver">SQL Server</option>
                    <option value="oracle">Oracle</option>
                  </select>
                </div>
              </div>

              <div style={{ display: "flex", gap: "0.5rem" }}>
                <div style={{ flex: 2 }}>
                  <label className="custom-label">Host</label>
                  <input
                    type="text"
                    placeholder="localhost"
                    value={dbHost}
                    onChange={(e) => setDbHost(e.target.value)}
                    className="custom-input"
                    required
                  />
                </div>
                <div style={{ flex: 1 }}>
                  <label className="custom-label">Port</label>
                  <input
                    type="number"
                    value={dbPort}
                    onChange={(e) => setDbPort(e.target.value)}
                    className="custom-input"
                    required
                  />
                </div>
              </div>

              <div style={{ display: "flex", gap: "0.5rem" }}>
                <div style={{ flex: 1 }}>
                  <label className="custom-label">User</label>
                  <input
                    type="text"
                    placeholder="postgres"
                    value={dbUser}
                    onChange={(e) => setDbUser(e.target.value)}
                    className="custom-input"
                    required
                  />
                </div>
                <div style={{ flex: 1 }}>
                  <label className="custom-label">Password</label>
                  <input
                    type="password"
                    placeholder="Password"
                    value={dbPass}
                    onChange={(e) => setDbPass(e.target.value)}
                    className="custom-input"
                  />
                </div>
              </div>

              <div>
                <label className="custom-label">Database Name</label>
                <input
                  type="text"
                  placeholder="sdoqap_oltp"
                  value={dbName}
                  onChange={(e) => setDbName(e.target.value)}
                  className="custom-input"
                  required
                />
              </div>

              <div>
                <label className="custom-label">SQL Query or Table Name</label>
                <textarea
                  placeholder="SELECT * FROM public.sales_records"
                  value={dbQuery}
                  onChange={(e) => setDbQuery(e.target.value)}
                  className="custom-input"
                  style={{ height: "48px", resize: "none" }}
                  required
                />
              </div>

              {rdbmsStatus && (
                <div
                  className={`alert-box ${rdbmsStatus.loading ? "info" : rdbmsStatus.success ? "success" : "critical"}`}
                  style={{ fontSize: "0.8rem", padding: "0.4rem", borderRadius: "6px" }}
                >
                  {rdbmsStatus.message}
                </div>
              )}

              <button
                type="submit"
                className="custom-button"
                disabled={rdbmsStatus?.loading || !dbHost || !dbName || !dbQuery || !rdbmsTableName}
                style={{ marginTop: "0.4rem" }}
              >
                {rdbmsStatus?.loading ? "Ingesting..." : "Ingest Database"}
              </button>
            </form>
          </div>
        </div>

        {/* RDBMS Learn More Modal */}
        {showRdbmsLearnMore && (
          <div style={{
            position: "fixed",
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: "rgba(15, 23, 42, 0.4)",
            backdropFilter: "blur(4px)",
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            zIndex: 1000
          }}>
            <div style={{
              background: "#FFFFFF",
              border: "1px solid #E2E8F0",
              borderRadius: "16px",
              width: "500px",
              padding: "2rem",
              boxShadow: "0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04)"
            }}>
              <h3 style={{ fontSize: "1.2rem", fontWeight: 700, marginBottom: "1rem", color: "var(--text-main)" }}>
                RDBMS Ingestion Prerequisites
              </h3>
              <div style={{ fontSize: "0.85rem", color: "var(--text-muted)", display: "flex", flexDirection: "column", gap: "0.75rem", lineHeight: "1.6", textAlign: "left" }}>
                <p>
                  <strong>1. Network & Firewall Access:</strong> Production databases are protected by security firewalls. 
                  To connect, the SDOQAP server must run inside the same intranet (Private Subnet) or connect via 
                  a secure <strong>VPN (Virtual Private Network)</strong> tunnel.
                </p>
                <p>
                  <strong>2. Host & Port Exposure:</strong> Ensure the database host is resolvable and the target port 
                  (e.g., 5432 for PostgreSQL, 3306 for MySQL) is open and accessible from this machine.
                </p>
                <p>
                  <strong>3. Authentication:</strong> Use native database credentials or configure centralized LDAP/SSO mapping 
                  in the database server configuration prior to connecting.
                </p>
              </div>

              <div style={{ marginTop: "1.5rem", borderTop: "1px solid #E2E8F0", paddingTop: "1rem", textAlign: "left" }}>
                <label style={{ display: "flex", alignItems: "center", gap: "0.5rem", cursor: "pointer" }}>
                  <input
                    type="checkbox"
                    checked={hasAcknowledgedRdbms}
                    onChange={(e) => setHasAcknowledgedRdbms(e.target.checked)}
                    style={{ width: "16px", height: "16px", accentColor: "#6C47FF" }}
                  />
                  <span style={{ fontSize: "0.85rem", color: "var(--text-main)", fontWeight: 500 }}>
                    I understand and have verified these network prerequisites.
                  </span>
                </label>
              </div>

              <div style={{ display: "flex", gap: "0.5rem", marginTop: "1.5rem" }}>
                <button
                  onClick={() => {
                    if (hasAcknowledgedRdbms) {
                      setIsRdbmsUnlocked(true);
                    }
                    setShowRdbmsLearnMore(false);
                  }}
                  disabled={!hasAcknowledgedRdbms}
                  className="custom-button"
                  style={{ 
                    margin: 0, 
                    flex: 1, 
                    background: hasAcknowledgedRdbms ? "#6C47FF" : "rgba(108, 71, 255, 0.08)", 
                    color: hasAcknowledgedRdbms ? "#FFFFFF" : "#6C47FF", 
                    borderColor: hasAcknowledgedRdbms ? "#6C47FF" : "rgba(108, 71, 255, 0.35)" 
                  }}
                >
                  Unlock Card
                </button>
                <button
                  onClick={() => setShowRdbmsLearnMore(false)}
                  className="custom-button"
                  style={{ margin: 0, flex: 1, background: "transparent", color: "var(--text-muted)", borderColor: "#CBD5E1" }}
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        )}

      </div>

      {/* Terminal log panel */}
      <div className="animate-in" style={{ marginTop: "1rem" }}>
        {/* macOS style Window Header bar */}
        <div className="terminal-header">
          <span className="terminal-dot" style={{ background: "#f43f5e" }}></span>
          <span className="terminal-dot" style={{ background: "#fbbf24" }}></span>
          <span className="terminal-dot" style={{ background: "#10b981" }}></span>
          <span style={{ color: "var(--text-muted)", fontSize: "0.75rem", marginLeft: "1rem", fontFamily: "var(--font-mono)" }}>
            Real-time Streaming Monitor {isStreaming && `— Time remaining: ${streamInfo.remaining}s`}
          </span>
        </div>

        {/* Progress Bar inside terminal border */}
        {isStreaming && (
          <div style={{ width: "100%", height: "2px", background: "rgba(255,255,255,0.05)", position: "relative" }}>
            <div style={{ width: `${getPercentage()}%`, height: "100%", background: "#6C47FF", transition: "width 0.5s ease" }} />
          </div>
        )}

        <div className="terminal-body">
          {streamInfo.logs && streamInfo.logs.length > 0 ? (
            streamInfo.logs.map((log, i) => {
              let color = "#e2e8f0";
              if (log.includes("[python]")) color = "#10b981"; // green
              if (log.includes("[spark]")) color = "#a855f7"; // purple
              if (log.includes("[ERROR]") || log.includes("failed")) color = "#f43f5e"; // red
              if (log.includes("[SYSTEM]")) color = "#6C47FF"; // purple/indigo
              
              return (
                <div key={i} style={{ color, marginBottom: "0.25rem", whiteSpace: "pre-wrap" }}>
                  {log}
                </div>
              );
            })
          ) : (
            <div style={{ color: "var(--text-muted)", textAlign: "center", marginTop: "110px", fontSize: "0.85rem" }}>
              <span>No live stream active. Start a Reddit real-time stream above to monitor execution logs.</span>
            </div>
          )}
          <div ref={terminalEndRef} />
        </div>
      </div>
    </div>
  );
}