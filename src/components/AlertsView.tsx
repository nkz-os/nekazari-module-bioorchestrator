import React, { useState, useEffect, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useParcelSelector } from "../hooks/useParcelSelector";
import type { AlertItem } from "../services/api";

type SeverityFilter = "all" | "critical" | "warning" | "info";

const SEVERITY_COLORS: Record<string, { bg: string; text: string; dot: string }> = {
  critical: { bg: "#f8d7da", text: "#721c24", dot: "#dc3545" },
  warning: { bg: "#fff3cd", text: "#856404", dot: "#ffc107" },
  info: { bg: "#d1ecf1", text: "#0c5460", dot: "#17a2b8" },
};

export default function AlertsView() {
  const { t } = useTranslation();
  const { parcels, selected: selectedParcel, setSelected: setSelectedParcel } = useParcelSelector();
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [severityFilter, setSeverityFilter] = useState<SeverityFilter>("all");

  const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";

  useEffect(() => {
    if (!selectedParcel) { setAlerts([]); return; }
    let cancelled = false;
    setLoading(true); setError("");
    fetch(`${API_BASE}/api/graph/agriculture/alerts?parcel_id=${encodeURIComponent(selectedParcel)}`, { credentials: "include" })
      .then(r => r.json())
      .then((d: { alerts: AlertItem[] }) => { if (!cancelled) setAlerts(d.alerts || []); })
      .catch((e: Error) => { if (!cancelled) setError(e.message); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [selectedParcel]);

  const filtered = useMemo(() => {
    if (severityFilter === "all") return alerts;
    return alerts.filter(a => a.severity === severityFilter);
  }, [alerts, severityFilter]);

  const counts = useMemo(() => ({
    all: alerts.length,
    critical: alerts.filter(a => a.severity === "critical").length,
    warning: alerts.filter(a => a.severity === "warning").length,
    info: alerts.filter(a => a.severity === "info").length,
  }), [alerts]);

  const formatTime = (ts: string) => {
    try {
      const d = new Date(ts);
      const now = Date.now();
      const diff = now - d.getTime();
      const minutes = Math.floor(diff / 60000);
      if (minutes < 60) return `${minutes}m ago`;
      const hours = Math.floor(minutes / 60);
      if (hours < 24) return `${hours}h ago`;
      const days = Math.floor(hours / 24);
      return `${days}d ago`;
    } catch { return ts; }
  };

  const FILTER_TABS: { key: SeverityFilter; label: string }[] = [
    { key: "all", label: t("alerts.filterAll") },
    { key: "critical", label: t("alerts.severity.critical") },
    { key: "warning", label: t("alerts.severity.warning") },
    { key: "info", label: t("alerts.severity.info") },
  ];

  return (
    <div>
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-4">🔔 {t("alerts.title")}</h2>

      <div style={{ marginBottom: 16, maxWidth: 400 }}>
        <select value={selectedParcel} onChange={e => setSelectedParcel(e.target.value)} style={{ width: "100%", padding: 8 }}>
          <option value="">{t("alerts.selectParcel")}</option>
          {parcels.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
        </select>
      </div>

      {!selectedParcel && (
        <div style={{ padding: 40, textAlign: "center", color: "#999" }}>
          {t("alerts.selectPrompt")}
        </div>
      )}

      {selectedParcel && loading && (
        <div style={{ padding: 20, color: "#999" }}>⏳ {t("common.loading")}</div>
      )}

      {selectedParcel && error && (
        <div style={{ color: "red", marginBottom: 12 }}>{error}</div>
      )}

      {selectedParcel && !loading && !error && (
        <>
          {alerts.length > 0 && (
            <div style={{ display: "flex", gap: 4, marginBottom: 16, flexWrap: "wrap" }}>
              {FILTER_TABS.map(tab => (
                <button
                  key={tab.key}
                  onClick={() => setSeverityFilter(tab.key)}
                  style={{
                    padding: "6px 14px",
                    border: "1px solid #ddd",
                    borderRadius: 20,
                    background: severityFilter === tab.key ? "#333" : "#fff",
                    color: severityFilter === tab.key ? "#fff" : "#333",
                    cursor: "pointer",
                    fontSize: 12,
                  }}
                >
                  {tab.label} {tab.key !== "all" && `(${counts[tab.key]})`}
                </button>
              ))}
            </div>
          )}

          {filtered.length === 0 ? (
            <div style={{ padding: 40, textAlign: "center", color: "#999" }}>
              {alerts.length === 0 ? t("alerts.empty") : t("alerts.noFilterMatch")}
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {filtered.map((alert, i) => {
                const sev = SEVERITY_COLORS[alert.severity] || SEVERITY_COLORS.info;
                return (
                  <div key={i} style={{ padding: 12, background: sev.bg, borderRadius: 8, border: `1px solid ${sev.dot}20` }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={{ width: 10, height: 10, borderRadius: "50%", background: sev.dot, display: "inline-block" }} />
                        <span style={{ fontWeight: 600, fontSize: 12, color: sev.text, textTransform: "uppercase" }}>
                          {t(`alerts.severity.${alert.severity}`)}
                        </span>
                      </div>
                      <span style={{ fontSize: 11, color: "#999" }}>{formatTime(alert.timestamp)}</span>
                    </div>
                    <div style={{ fontSize: 14, fontWeight: 500, marginBottom: 4 }}>{alert.type}</div>
                    {alert.recommended_action && (
                      <div style={{ fontSize: 12, color: "#666" }}>
                        💡 {t("alerts.recommendedAction")}: {alert.recommended_action}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </>
      )}
    </div>
  );
}
