import React, { useState, useEffect, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useParcelContext } from "../context/ParcelContext";
import { Card, Badge, Button, Stack } from "@nekazari/ui-kit";
import ContextEmptyState from "./shared/ContextEmptyState";
import type { AlertItem } from "../services/api";

type SeverityFilter = "all" | "critical" | "warning" | "info";

const SEVERITY_INTENTS: Record<string, "negative" | "warning" | "info"> = {
  critical: "negative",
  warning: "warning",
  info: "info",
};

export default function AlertsView() {
  const { t } = useTranslation();
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
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

  if (parcelLoading) return <div className="text-nkz-text-muted p-4">⏳ {t("common.loading")}</div>;
  if (parcelError) return <ContextEmptyState message={parcelError} variant="warning" actionLabel={t("panel.retry")} onAction={() => window.location.reload()} />;
  if (!selectedParcel) return <ContextEmptyState message={t("alerts.selectPrompt")} variant="info" />;

  return (
    <Stack gap="section">
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary">🔔 {t("alerts.title")}</h2>

      {loading && (
        <div className="text-nkz-text-muted py-5">⏳ {t("common.loading")}</div>
      )}

      {error && (
        <div className="text-nkz-danger mb-3">{error}</div>
      )}

      {!loading && !error && (
        <Stack gap="stack">
          {alerts.length > 0 && (
            <div className="flex gap-1 flex-wrap">
              {FILTER_TABS.map(tab => (
                <button
                  key={tab.key}
                  onClick={() => setSeverityFilter(tab.key)}
                  className={`px-3.5 py-1.5 border rounded-full text-xs cursor-pointer transition-colors ${
                    severityFilter === tab.key
                      ? "bg-nkz-text-primary text-nkz-surface border-nkz-text-primary"
                      : "bg-nkz-surface text-nkz-text-primary border-nkz-border hover:bg-nkz-surface-sunken"
                  }`}
                >
                  {tab.label} {tab.key !== "all" && `(${counts[tab.key]})`}
                </button>
              ))}
            </div>
          )}

          {filtered.length === 0 ? (
            <ContextEmptyState
              message={alerts.length === 0 ? t("alerts.empty") : t("alerts.noFilterMatch")}
              variant="info"
            />
          ) : (
            <div className="flex flex-col gap-2">
              {filtered.map((alert, i) => {
                const intent = SEVERITY_INTENTS[alert.severity] || "info";
                return (
                  <Card key={i} padding="md" className={`border-l-4 border-l-${intent === "negative" ? "nkz-danger" : intent === "warning" ? "nkz-warning" : "nkz-info"}`}>
                    <div className="flex justify-between items-center mb-1.5">
                      <div className="flex items-center gap-2">
                        <span className={`w-2.5 h-2.5 rounded-full inline-block ${
                          intent === "negative" ? "bg-nkz-danger" : intent === "warning" ? "bg-nkz-warning" : "bg-nkz-info"
                        }`} />
                        <Badge intent={intent}>
                          {t(`alerts.severity.${alert.severity}`)}
                        </Badge>
                      </div>
                      <span className="text-nkz-xs text-nkz-text-muted">{formatTime(alert.timestamp)}</span>
                    </div>
                    <div className="text-sm font-medium mb-1">
                      {alert.type}
                      {alert.eco_impact && (
                        <Badge intent={alert.eco_impact.risk_level === "high" ? "negative" : "warning"} className="ml-2">
                          🐝 Eco-Warning
                        </Badge>
                      )}
                    </div>
                    {alert.recommended_action && (
                      <div className="text-nkz-xs text-nkz-text-muted">
                        💡 {t("alerts.recommendedAction")}: {alert.recommended_action}
                      </div>
                    )}
                    {alert.eco_impact && (
                      <div className="mt-2 p-2 bg-nkz-surface-sunken rounded-nkz-md text-xs">
                        <div>🐝 {t("alerts.pollinators")}: {alert.eco_impact.pollinator_species.join(", ") || t("alerts.pollinatorsUnknown")}</div>
                        <div>⏰ {t("alerts.recommendedWindow")}: {alert.eco_impact.recommended_window}</div>
                        {alert.eco_impact.safer_alternatives.length > 0 && (
                          <div>🌿 {t("alerts.saferAlternatives")}: {alert.eco_impact.safer_alternatives.join(", ")}</div>
                        )}
                      </div>
                    )}
                  </Card>
                );
              })}
            </div>
          )}
        </Stack>
      )}
    </Stack>
  );
}
