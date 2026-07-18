import React, { useState, useEffect, useRef } from "react";
import { postApi } from "../hooks/useApi";
import "./Ingestion.css";

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
    <div className="gs-ingestion">
      {/* 1. Page Header */}
      <div className="gs-page-header">
        <div>
          <h1 className="gs-page-title">Ingestion <span>Control Stage</span></h1>
          <p className="gs-page-desc">Ingest datasets via CSV upload, REST APIs, SQL databases, or live feeds</p>
        </div>
      </div>

      {/* 2. Ingest Grid */}
      <div className="gs-ingest-grid">
        
        {/* Local CSV Dataset */}
        <div className="gs-icard">
          <h3>Local CSV Dataset Inflow</h3>
          <form onSubmit={handleCsvSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '8px', flexGrow: 1 }}>
            <div
              className={`gs-dropzone ${csvDragging ? 'dragging' : ''} ${csvFile ? 'ready' : ''}`}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
              onClick={() => document.getElementById("csvFileSelect").click()}
            >
              <input
                type="file"
                id="csvFileSelect"
                accept=".csv"
                onChange={handleFileChange}
                style={{ display: "none" }}
              />
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                <polyline points="17 8 12 3 7 8" />
                <line x1="12" y1="3" x2="12" y2="15" />
              </svg>
              {csvFile ? (
                <div>
                  <span>{csvFile.name}</span>
                  <p>{(csvFile.size / 1024).toFixed(1)} KB — ready to upload</p>
                </div>
              ) : (
                <div>
                  <span>Click or drag CSV here</span>
                  <p>Supports .csv, .tsv (max 100MB)</p>
                </div>
              )}
            </div>

            <div className="gs-input-grp">
              <label>Target Delta Table Name</label>
              <input
                type="text"
                placeholder="e.g. users_dataset"
                value={csvTableName}
                onChange={(e) => setCsvTableName(e.target.value.replace(/[^a-zA-Z0-9_]/g, "_"))}
                required
              />
            </div>

            {csvStatus && (
              <div className={`gs-toast ${csvStatus.loading ? 'loading' : csvStatus.success ? 'ok' : 'err'}`}>
                {csvStatus.message}
              </div>
            )}

            <button
              type="submit"
              className="gs-btn-submit"
              disabled={csvStatus?.loading || !csvFile || !csvTableName}
            >
              {csvStatus?.loading ? "Ingesting..." : "Validate & Ingest CSV"}
            </button>
          </form>
        </div>

        {/* JSON API Fetcher */}
        <div className="gs-icard">
          <h3>JSON API Endpoint Fetcher</h3>
          <form onSubmit={handleApiSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '8px', flexGrow: 1 }}>
            <div className="gs-input-grp">
              <label>Table Name</label>
              <input
                type="text"
                placeholder="e.g. api_products"
                value={apiTableName}
                onChange={(e) => setApiTableName(e.target.value.replace(/[^a-zA-Z0-9_]/g, "_"))}
                required
              />
            </div>

            <div className="gs-input-grp">
              <label>REST Endpoint URL</label>
              <input
                type="url"
                placeholder="https://api.store.com/v1/products"
                value={apiUrl}
                onChange={(e) => setApiUrl(e.target.value)}
                required
              />
            </div>

            <div className="gs-input-grp">
              <label>Authorization Header (Optional)</label>
              <input
                type="password"
                placeholder="Bearer token..."
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
              />
            </div>

            <div className="gs-input-row">
              <div className="gs-input-grp">
                <label>HTTP Method</label>
                <select value={apiMethod} onChange={(e) => setApiMethod(e.target.value)}>
                  <option value="GET">GET</option>
                  <option value="POST">POST</option>
                </select>
              </div>
              <div className="gs-input-grp">
                <div className="gs-slider-header">
                  <label>Interval</label>
                  <span>{apiInterval}s</span>
                </div>
                <input
                  type="range"
                  min="5"
                  max="60"
                  value={apiInterval}
                  onChange={(e) => setApiInterval(e.target.value)}
                  className="gs-slider"
                />
              </div>
            </div>

            {apiStatus && (
              <div className={`gs-toast ${apiStatus.loading ? 'loading' : apiStatus.success ? 'ok' : 'err'}`}>
                {apiStatus.message}
              </div>
            )}

            <button
              type="submit"
              className="gs-btn-submit"
              disabled={apiStatus?.loading || !apiUrl || !apiTableName}
            >
              {apiStatus?.loading ? "Connecting..." : "Trigger API Sync"}
            </button>
          </form>
        </div>

        {/* Reddit Streaming Feed */}
        <div className="gs-icard">
          <h3>Reddit Live Stream Feed</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', flexGrow: 1 }}>
            <div className="gs-input-grp">
              <label>Topics to Index</label>
              <input
                type="text"
                placeholder="#Technology, #AI"
                value={redditTopic}
                onChange={(e) => setRedditTopic(e.target.value)}
                disabled={isStreaming}
              />
            </div>

            <div className="gs-input-grp">
              <label>Target Subreddits</label>
              <input
                type="text"
                placeholder="python, bigdata"
                value={redditSubreddits}
                onChange={(e) => setRedditSubreddits(e.target.value)}
                disabled={isStreaming}
              />
            </div>

            <div className="gs-input-grp">
              <div className="gs-slider-header">
                <label>Ingest Duration</label>
                <span>{redditDuration}s</span>
              </div>
              <input
                type="range"
                min="10"
                max="120"
                step="5"
                value={redditDuration}
                onChange={(e) => setRedditDuration(e.target.value)}
                className="gs-slider"
                disabled={isStreaming}
              />
            </div>

            {redditStatus && (
              <div className={`gs-toast ${redditStatus.success ? 'ok' : 'err'}`}>
                {redditStatus.message}
              </div>
            )}

            <div className="gs-switch-row">
              <span className="gs-switch-label">Live Stream Feed Inflow</span>
              <label className="gs-switch">
                <input
                  type="checkbox"
                  checked={isStreaming}
                  onChange={handleRedditToggle}
                />
                <span className="gs-switch-slider"></span>
              </label>
            </div>
          </div>
        </div>

        <div className="gs-icard">
          {!isRdbmsUnlocked && (
            <div className="gs-lock">
              <span className="gs-lock-icon" style={{ display: 'flex', alignItems: 'center', marginBottom: '8px' }}>
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ color: 'var(--text-muted)' }}><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
              </span>
              <h4>RDBMS Ingestion Locked</h4>
              <p>Direct relational database ingestion requires corporate VPN configuration validation.</p>
              <button className="gs-lock-btn" onClick={() => setShowRdbmsLearnMore(true)}>Review Requirements</button>
            </div>
          )}

          <h3>Relational DB Direct Ingest</h3>
          <form onSubmit={handleRdbmsSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '6px', flexGrow: 1, opacity: isRdbmsUnlocked ? 1 : 0.35 }}>
            <div className="gs-input-row">
              <div className="gs-input-grp">
                <label>Target Table</label>
                <input
                  type="text"
                  placeholder="e.g. postgres_sales"
                  value={rdbmsTableName}
                  onChange={(e) => setRdbmsTableName(e.target.value.replace(/[^a-zA-Z0-9_]/g, "_"))}
                  required
                  disabled={!isRdbmsUnlocked}
                />
              </div>
              <div className="gs-input-grp">
                <label>DB Provider</label>
                <select value={dbType} onChange={(e) => {
                  setDbType(e.target.value);
                  if (e.target.value === "postgresql") setDbPort(5432);
                  else if (e.target.value === "mysql") setDbPort(3306);
                  else if (e.target.value === "sqlserver") setDbPort(1433);
                }} disabled={!isRdbmsUnlocked}>
                  <option value="postgresql">PostgreSQL</option>
                  <option value="mysql">MySQL</option>
                  <option value="sqlserver">SQL Server</option>
                </select>
              </div>
            </div>

            <div className="gs-input-row">
              <div className="gs-input-grp" style={{ flexGrow: 2 }}>
                <label>Host</label>
                <input type="text" placeholder="db.intranet.net" value={dbHost} onChange={(e) => setDbHost(e.target.value)} required disabled={!isRdbmsUnlocked} />
              </div>
              <div className="gs-input-grp" style={{ flexGrow: 1 }}>
                <label>Port</label>
                <input type="number" value={dbPort} onChange={(e) => setDbPort(e.target.value)} required disabled={!isRdbmsUnlocked} />
              </div>
            </div>

            <div className="gs-input-row">
              <div className="gs-input-grp">
                <label>User</label>
                <input type="text" placeholder="readonly_sdoqap" value={dbUser} onChange={(e) => setDbUser(e.target.value)} required disabled={!isRdbmsUnlocked} />
              </div>
              <div className="gs-input-grp">
                <label>Password</label>
                <input type="password" placeholder="••••••••" value={dbPass} onChange={(e) => setDbPass(e.target.value)} disabled={!isRdbmsUnlocked} />
              </div>
            </div>

            <div className="gs-input-grp">
              <label>Database Schema/Name</label>
              <input type="text" placeholder="sdoqap_prod" value={dbName} onChange={(e) => setDbName(e.target.value)} required disabled={!isRdbmsUnlocked} />
            </div>

            <div className="gs-input-grp">
              <label>SQL Query</label>
              <textarea placeholder="SELECT * FROM public.sales" value={dbQuery} onChange={(e) => setDbQuery(e.target.value)} style={{ height: '38px', resize: 'none' }} required disabled={!isRdbmsUnlocked} />
            </div>

            {rdbmsStatus && (
              <div className={`gs-toast ${rdbmsStatus.loading ? 'loading' : rdbmsStatus.success ? 'ok' : 'err'}`}>
                {rdbmsStatus.message}
              </div>
            )}

            <button
              type="submit"
              className="gs-btn-submit"
              disabled={rdbmsStatus?.loading || !isRdbmsUnlocked || !dbHost || !dbName || !dbQuery || !rdbmsTableName}
            >
              {rdbmsStatus?.loading ? "Ingesting..." : "Validate & Connect RDBMS"}
            </button>
          </form>
        </div>
      </div>

      {/* RDBMS Learn More Modal */}
      {showRdbmsLearnMore && (
        <div className="gs-modal-bg">
          <div className="gs-modal">
            <h3>RDBMS Firewall Prerequisites</h3>
            <div className="gs-modal-body">
              <p>
                <strong>1. Corporate IP Route:</strong> Make sure SDOQAP IP route is added in DB server firewall pg_hba.conf.
              </p>
              <p>
                <strong>2. Intranet VPN Tunnel:</strong> Use corporate network tunnel if target DB server is private.
              </p>
              <p>
                <strong>3. Port Exposure Check:</strong> Verify ports 5432 / 3306 are exposed on target hosts.
              </p>
            </div>

            <div style={{ marginTop: "14px", display: 'flex', alignItems: 'center', gap: '8px' }}>
              <input
                type="checkbox"
                checked={hasAcknowledgedRdbms}
                onChange={(e) => setHasAcknowledgedRdbms(e.target.checked)}
                id="ack-rdbms"
                style={{ width: "16px", height: "16px", accentColor: "var(--accent-purple)" }}
              />
              <label htmlFor="ack-rdbms" style={{ fontSize: "11px", color: "var(--text-main)", fontWeight: 700, cursor: 'pointer' }}>
                I have verified these networking rules.
              </label>
            </div>

            <div className="gs-modal-footer">
              <button
                onClick={() => {
                  if (hasAcknowledgedRdbms) {
                    setIsRdbmsUnlocked(true);
                  }
                  setShowRdbmsLearnMore(false);
                }}
                disabled={!hasAcknowledgedRdbms}
                className="gs-modal-btn primary"
              >
                Unlock Ingestion Card
              </button>
              <button
                onClick={() => setShowRdbmsLearnMore(false)}
                className="gs-modal-btn"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 3. Terminal log panel */}
      <div className="gs-terminal">
        <div className="gs-terminal-bar">
          <div className="gs-terminal-dots">
            <i></i><i></i><i></i>
          </div>
          <span>Real-time Stream Logs {isStreaming && `— Duration: ${streamInfo.elapsed}s / ${streamInfo.duration}s`}</span>
        </div>

        {isStreaming && (
          <div className="gs-tbar-progress">
            <div className="gs-tbar-fill" style={{ width: `${getPercentage()}%` }} />
          </div>
        )}

        <div className="gs-terminal-body">
          {streamInfo.logs && streamInfo.logs.length > 0 ? (
            streamInfo.logs.map((log, i) => {
              let color = "#cbd5e1";
              if (log.includes("[python]")) color = "#10b981";
              if (log.includes("[spark]")) color = "#a855f7";
              if (log.includes("[ERROR]")) color = "#f43f5e";
              if (log.includes("[SYSTEM]")) color = "#6C47FF";

              return (
                <div key={i} className="gs-terminal-line" style={{ color }}>
                  {log}
                </div>
              );
            })
          ) : (
            <div className="gs-empty" style={{ paddingTop: '80px' }}>
              No stream active. Trigger ingestion to inspect logs.
            </div>
          )}
          <div ref={terminalEndRef} />
        </div>
      </div>
    </div>
  );
}