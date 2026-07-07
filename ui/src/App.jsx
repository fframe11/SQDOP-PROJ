import React from "react";
import { BrowserRouter, Routes, Route } from "react-router-dom";
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

export default function App() {
  return (
    <BrowserRouter>
      <div className="app-bg" />
      <NavBar />
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
    </BrowserRouter>
  );
}

