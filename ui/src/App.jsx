import React from "react";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import NavBar from "./components/NavBar";
import Home from "./pages/Home";
import Dashboard from "./pages/Dashboard";
import Analytics from "./pages/Analytics";
import Pipeline from "./pages/Pipeline";
import Schema from "./pages/Schema";
import "./App.css";

export default function App() {
  return (
    <BrowserRouter>
      <div className="app-bg" />
      <NavBar />
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/analytics" element={<Analytics />} />
        <Route path="/pipeline" element={<Pipeline />} />
        <Route path="/schema" element={<Schema />} />
      </Routes>
    </BrowserRouter>
  );
}
