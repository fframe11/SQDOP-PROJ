import { useState, useEffect, useCallback } from "react";

const API_BASE = "/api/v1";

export function useApi(endpoint, options = {}) {
  const { refreshInterval = 0, enabled = true } = options;
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchData = useCallback(async () => {
    if (!enabled) return;
    try {
      const res = await fetch(`${API_BASE}${endpoint}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [endpoint, enabled]);

  useEffect(() => {
    fetchData();
    if (refreshInterval > 0) {
      const id = setInterval(fetchData, refreshInterval);
      return () => clearInterval(id);
    }
  }, [fetchData, refreshInterval]);

  return { data, loading, error, refetch: fetchData };
}

export async function postApi(endpoint, body = null) {
  const options = { method: "POST" };
  if (body) {
    options.headers = {
      "Content-Type": "application/json"
    };
    options.body = JSON.stringify(body);
  }
  const res = await fetch(`${API_BASE}${endpoint}`, options);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}
