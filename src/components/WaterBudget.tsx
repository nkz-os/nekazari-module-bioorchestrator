import React, { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParcelSelector } from "../hooks/useParcelSelector";
import { getCropContext, CropContextResponse } from "../services/api";

interface BudgetData {
  parcel_id: string; week_start: string; week_end: string;
  soil_awc_mm: number; current_moisture_estimate_mm: number; mad_mm: number;
  kc: number; kc_stage: string; eto_weekly_mm: number; etc_weekly_mm: number;
  forecast_rainfall_mm: number; deficit_mm: number;
  irrigation_required_mm: number; irrigation_required_m3_ha: number;
  confidence: string; confidence_notes: string; recommendation: string;
}

export default function WaterBudget() {
  const { t } = useTranslation();
  const { parcels, selected: selectedParcel, setSelected: setSelectedParcel } = useParcelSelector();
  const [ctx, setCtx] = useState<CropContextResponse | null>(null);
  const [budget, setBudget] = useState<BudgetData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  React.useEffect(() => {
    if (!selectedParcel) return;
    setLoading(true); setError("");
    (async () => {
      try {
        const context = await getCropContext(selectedParcel);
        setCtx(context);
        const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";
        const res = await fetch(`${API_BASE}/api/graph/agriculture/water-budget?parcel_id=${selectedParcel}`, { credentials: "include" });
        if (!res.ok) { const err = await res.json(); throw new Error(err.detail || "Failed"); }
        setBudget(await res.json());
      } catch (e: any) { setError(e.message); }
      finally { setLoading(false); }
    })();
  }, [selectedParcel]);

  const gauge = (label: string, value: number, max: number, unit: string, note: string) => (
    <div style={{ marginBottom: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13 }}>
        <span>{label}</span><span>{value} {unit}</span>
      </div>
      <div style={{ background: "#eee", borderRadius: 4, height: 8, overflow: "hidden" }}>
        <div style={{ background: "#4caf50", height: 8, width: `${Math.min(100, (value / max) * 100)}%` }} />
      </div>
      <div style={{ fontSize: 11, color: "#999" }}>{note}</div>
    </div>
  );

  return (
    <div>
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-4">💧 {t("waterBudget.title")}</h2>
      <div style={{ marginBottom: 16, maxWidth: 400 }}>
        <select value={selectedParcel} onChange={e => setSelectedParcel(e.target.value)} style={{ width: "100%", padding: 8 }}>
          <option value="">{t("waterBudget.selectParcel")}</option>
          {parcels.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
        </select>
      </div>
      {loading && <div style={{ padding: 20, textAlign: "center" }}>⏳ {t("common.loading")}</div>}
      {error && <div style={{ padding: 20, background: "#f8d7da", borderRadius: 8, maxWidth: 500 }}>{error}</div>}
      {budget && (
        <div style={{ maxWidth: 600 }}>
          <div style={{ marginBottom: 16, fontSize: 14, color: "#666" }}>{budget.week_start} → {budget.week_end}</div>
          {gauge(t("waterBudget.availableWater"), budget.current_moisture_estimate_mm, budget.soil_awc_mm, "mm", `AWC: ${budget.soil_awc_mm}mm`)}
          {gauge(t("waterBudget.etc"), budget.etc_weekly_mm, 50, "mm", `Kc: ${budget.kc} (${budget.kc_stage})`)}
          {gauge(t("waterBudget.rainfall"), budget.forecast_rainfall_mm, 50, "mm", t("waterBudget.forecast"))}
          <div style={{ padding: 16, background: budget.deficit_mm > 20 ? "#f8d7da" : budget.deficit_mm > 0 ? "#fff3cd" : "#d4edda", borderRadius: 8, marginBottom: 16 }}>
            <strong>{t("waterBudget.deficit")}: {budget.deficit_mm} mm</strong>
            {budget.irrigation_required_mm > 0 && (
              <div style={{ marginTop: 8 }}>{t("waterBudget.irrigationRequired")}: <strong>{budget.irrigation_required_mm} mm = {budget.irrigation_required_m3_ha} m³/ha</strong></div>
            )}
            <div style={{ marginTop: 8, fontSize: 14 }}>{budget.recommendation}</div>
          </div>
          <div style={{ fontSize: 11, color: "#999" }}>{t("waterBudget.confidence")}: {budget.confidence} — {budget.confidence_notes}</div>
        </div>
      )}
    </div>
  );
}
