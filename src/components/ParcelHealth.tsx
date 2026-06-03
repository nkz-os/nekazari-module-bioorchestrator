import React, { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParcelSelector } from "../hooks/useParcelSelector";
import { getCropContext, getYieldPotential, CropContextResponse, YieldPotentialResponse, fetchAssessmentHistory, fetchAlerts, HistoryPoint, AlertItem } from "../services/api";
import ParcelHealthChart from "./ParcelHealthChart";

interface AssessmentData {
  cwsiValue?: number; mdsValue?: number; mdsSeverity?: string;
  waterBalanceDeficit?: number; thermalCondition?: string; thermalSeverity?: string;
  vigorIndex?: number; vigorCondition?: string; compositeStressIndex?: number;
  dominantStressor?: string; overallSeverity?: string; recommendedAction?: string;
  yieldUtilizationPct?: number; dataFidelity?: string; assessedAt?: string;
}

export default function ParcelHealth() {
  const { t } = useTranslation();
  const { parcels, selected: selectedParcel, setSelected: setSelectedParcel } = useParcelSelector();
  const [ctx, setCtx] = useState<CropContextResponse | null>(null);
  const [yp, setYp] = useState<YieldPotentialResponse | null>(null);
  const [assessment, setAssessment] = useState<AssessmentData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [alerts, setAlerts] = useState<AlertItem[]>([]);

  useEffect(() => {
    if (!selectedParcel) return;
    setLoading(true); setError(""); setCtx(null); setYp(null); setAssessment(null);
    (async () => {
      try {
        const context = await getCropContext(selectedParcel);
        if (!context.crop?.eppo || context.crop.eppo === "unknown") {
          setError("noCropAssigned"); setLoading(false); return;
        }
        setCtx(context);
        // Fetch assessment + yield + history + alerts in parallel
        const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";
        const [assessResp, ypPromise, histPromise, alertPromise] = await Promise.allSettled([
          fetch(`${API_BASE}/api/crop-health/assessments/latest?parcelId=${selectedParcel}`, { credentials: "include" }).then(r => r.json()),
          context.variety?.name
            ? getYieldPotential(context.variety.name, context.crop.eppo, undefined, undefined, selectedParcel)
            : Promise.resolve(null),
          fetchAssessmentHistory(selectedParcel, 14),
          fetchAlerts(selectedParcel),
        ]);
        if (assessResp.status === "fulfilled" && assessResp.value?.assessments?.length) {
          setAssessment(assessResp.value.assessments[0]);
        }
        if (ypPromise.status === "fulfilled" && ypPromise.value && !("detail" in ypPromise.value)) {
          setYp(ypPromise.value);
        }
        if (histPromise.status === "fulfilled") setHistory(histPromise.value);
        if (alertPromise.status === "fulfilled") setAlerts(alertPromise.value);
      } catch (e: any) { setError(e.message || "unknown"); }
      finally { setLoading(false); }
    })();
  }, [selectedParcel]);

  const card = (icon: string, label: string, value: string | number, sub?: string, color?: string) => (
    <div style={{ padding: "8px 12px", background: color || "#f5f5f5", borderRadius: 6, minWidth: 100, flex: "1 1 auto" }}>
      <div style={{ fontSize: 11, color: "#888" }}>{icon} {label}</div>
      <div style={{ fontSize: 16, fontWeight: 600 }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "#666" }}>{sub}</div>}
    </div>
  );

  const severityColor = (s?: string) => s === "CRITICAL" ? "#f8d7da" : s === "HIGH" ? "#fff3cd" : s === "MEDIUM" ? "#fff3cd" : "#f0f4ff";

  return (
    <div>
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-4">🩺 {t("parcelHealth.title")}</h2>
      <div style={{ marginBottom: 16, maxWidth: 400 }}>
        <select value={selectedParcel} onChange={e => setSelectedParcel(e.target.value)} style={{ width: "100%", padding: 8 }}>
          <option value="">{t("parcelHealth.selectParcel")}</option>
          {parcels.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
        </select>
      </div>
      {loading && <div style={{ padding: 20, textAlign: "center" }}>⏳ {t("common.loading")}</div>}
      {error === "noCropAssigned" && (
        <div style={{ padding: 20, background: "#fff3cd", borderRadius: 8, maxWidth: 500 }}>
          <p>{t("parcelHealth.noCrop")}</p>
          <p style={{ fontSize: 13, color: "#666" }}>{t("parcelHealth.goToVarietyFinder")}</p>
        </div>
      )}
      {error && error !== "noCropAssigned" && <div style={{ padding: 20, background: "#f8d7da", borderRadius: 8, maxWidth: 500 }}>{t("parcelHealth.error")}: {error}</div>}
      {ctx?.crop && (
        <>
          {/* Alert banner */}
          {alerts.length > 0 && (
            <div style={{ padding: 10, background: "#f8d7da", borderRadius: 8, marginBottom: 12, maxWidth: 600, fontSize: 13 }}>
              ⚠️ <strong>{t("parcelHealth.activeAlerts")}:</strong>{" "}
              {alerts.map((a, i) => (
                <span key={i} style={{ marginRight: 12 }}>
                  {a.severity} — {a.recommended_action} ({a.timestamp?.slice(0, 10)})
                </span>
              ))}
            </div>
          )}
          {/* Crop info */}
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
            <div style={{ padding: "8px 12px", background: "#f0f4ff", borderRadius: 6, fontSize: 13 }}><strong>{t("parcelHealth.crop")}:</strong> {ctx.crop.name} ({ctx.crop.eppo})</div>
            {ctx.variety && <div style={{ padding: "8px 12px", background: "#f0f4ff", borderRadius: 6, fontSize: 13 }}><strong>{t("parcelHealth.variety")}:</strong> {ctx.variety.name}</div>}
            <div style={{ padding: "8px 12px", background: ctx.phenology_source?.startsWith("bioorchestrator") ? "#d4edda" : "#f0f4ff", borderRadius: 6, fontSize: 13 }}>
              <strong>{t("parcelHealth.source")}:</strong> {ctx.phenology_source?.startsWith("bioorchestrator") ? t("parcelHealth.calibrated") : t("parcelHealth.default")}
            </div>
          </div>

          {/* Sensor data cards */}
          {assessment ? (
            <>
              <div style={{ fontSize: 12, color: "#999", marginBottom: 8 }}>📡 {t("parcelHealth.sensorData")} ({assessment.assessedAt?.slice(0, 16) || "?"})</div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
                {card("💧", "CWSI", assessment.cwsiValue?.toFixed(2) ?? "—", assessment.cwsiValue !== undefined ? (assessment.cwsiValue < 0.3 ? "normal" : assessment.cwsiValue < 0.5 ? "mild" : "stress") : undefined)}
                {card("📏", "MDS", assessment.mdsValue ? `${assessment.mdsValue}µm` : "—", assessment.mdsSeverity || "", assessment.mdsSeverity === "HIGH" || assessment.mdsSeverity === "CRITICAL" ? "#f8d7da" : undefined)}
                {card("⚖️", "Water Bal", assessment.waterBalanceDeficit !== undefined ? `${assessment.waterBalanceDeficit}mm` : "—", assessment.waterBalanceDeficit !== undefined && assessment.waterBalanceDeficit < -5 ? "deficit" : "ok")}
                {card("🔥", "Thermal", assessment.thermalCondition || "—", assessment.thermalSeverity || "")}
              </div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
                {card("🌿", "Vigor", assessment.vigorIndex?.toFixed(2) ?? "—", assessment.vigorCondition || "", assessment.vigorCondition === "stress" ? "#f8d7da" : "#d4edda")}
                {card("📊", "Composite", assessment.compositeStressIndex ?? "—", `${assessment.dominantStressor || ""}`, severityColor(assessment.overallSeverity))}
                {card("⚠️", "Severity", assessment.overallSeverity || "—", assessment.recommendedAction || "", severityColor(assessment.overallSeverity))}
                {assessment.yieldUtilizationPct !== undefined && card("📈", "Yield Util", `${assessment.yieldUtilizationPct}%`, "")}
              </div>
              <div style={{ fontSize: 11, color: "#999", marginBottom: 12 }}>Fidelity: {assessment.dataFidelity || "unknown"}</div>
            </>
          ) : (
            <div style={{ padding: 16, background: "#f9f9f9", borderRadius: 8, marginBottom: 12, maxWidth: 500, fontSize: 13, color: "#666" }}>
              📡 {t("parcelHealth.noSensorData")}
            </div>
          )}

          {/* Phenology */}
          {ctx.phenology && (
            <div style={{ marginBottom: 16, padding: 12, background: "#f5f5f5", borderRadius: 8, maxWidth: 600 }}>
              <strong>{t("parcelHealth.phenology")}:</strong> {String(ctx.phenology.stage || '')} — Kc: {String(ctx.phenology.kc || '')} — Ky: {String(ctx.phenology.ky || '')}
            </div>
          )}

          {/* Yield gap */}
          {yp?.yield_gap_pct !== undefined && (
            <div style={{ marginBottom: 16, padding: 12, background: (yp.yield_gap_pct ?? 0) > 10 ? "#fff3cd" : "#d4edda", borderRadius: 8, maxWidth: 400 }}>
              <strong>{t("parcelHealth.yieldGap")}:</strong> {yp.yield_gap_pct}% ({yp.yield_gap_kg_ha} kg/ha)
              <br /><span style={{ fontSize: 12 }}>{t("parcelHealth.expected")}: {yp.expected_yield_kg_ha} kg/ha</span>
            </div>
          )}

          {/* Soil suitability + sensors */}
          {ctx.soil?.suitability && (
            <div style={{ marginBottom: 16, padding: 12, background: ctx.soil.suitability.overall === "suitable" ? "#d4edda" : "#fff3cd", borderRadius: 8, maxWidth: 400 }}>
              <strong>{t("parcelHealth.soilSuitability")}:</strong> pH {ctx.soil.suitability.ph_match ? "✅" : "❌"} — Texture {ctx.soil.suitability.texture_match ? "✅" : "❌"}
              {(ctx.soil.actual as any)?.awc_mm && <span> — AWC: {(ctx.soil.actual as any).awc_mm}mm</span>}
            </div>
          )}
          {ctx.soil_sensors && (ctx.soil_sensors as any).available && (
            <div style={{ marginBottom: 16, padding: 12, background: "#f0f4ff", borderRadius: 8, maxWidth: 600 }}>
              <strong>🌡️ {t("parcelHealth.soilSensors")}:</strong> pH {(ctx.soil_sensors as any).ph} — Moist {(ctx.soil_sensors as any).moisture_pct}%
            </div>
          )}

          {/* Historical chart */}
          <ParcelHealthChart data={history} />

          <div style={{ fontSize: 11, color: "#999", marginTop: 16 }}>
            {t("parcelHealth.phenologySource")}: {ctx.phenology_source} — {t("parcelHealth.matchLevel")}: {ctx.match_level}
          </div>
        </>
      )}
    </div>
  );
}
