import React, { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { getCropContext, getYieldPotential, CropContextResponse, YieldPotentialResponse } from "../services/api";

interface Parcel { id: string; name: string; }

export default function ParcelHealth() {
  const { t } = useTranslation();
  const [parcels, setParcels] = useState<Parcel[]>([]);
  const [selectedParcel, setSelectedParcel] = useState("");
  const [ctx, setCtx] = useState<CropContextResponse | null>(null);
  const [yp, setYp] = useState<YieldPotentialResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    const sdk = (window as any).__NKZ_SDK__;
    if (sdk?.getParcels) sdk.getParcels().then((p: Parcel[]) => setParcels(p));
  }, []);

  useEffect(() => {
    if (!selectedParcel) return;
    setLoading(true); setError(""); setCtx(null); setYp(null);
    (async () => {
      try {
        const context = await getCropContext(selectedParcel);
        if (!context.crop?.eppo || context.crop.eppo === "unknown") {
          setError("noCropAssigned"); setLoading(false); return;
        }
        setCtx(context);
        if (context.variety?.name) {
          try {
            const y = await getYieldPotential(context.variety.name, context.crop.eppo, undefined, undefined, selectedParcel);
            if (typeof y === "object" && !("detail" in y)) setYp(y);
          } catch {}
        }
      } catch (e: any) { setError(e.message || "unknown"); }
      finally { setLoading(false); }
    })();
  }, [selectedParcel]);

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
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
            <div style={{ padding: "8px 12px", background: "#f0f4ff", borderRadius: 6, fontSize: 13 }}><strong>{t("parcelHealth.crop")}:</strong> {ctx.crop.name} ({ctx.crop.eppo})</div>
            {ctx.variety && <div style={{ padding: "8px 12px", background: "#f0f4ff", borderRadius: 6, fontSize: 13 }}><strong>{t("parcelHealth.variety")}:</strong> {ctx.variety.name}</div>}
            <div style={{ padding: "8px 12px", background: "#f0f4ff", borderRadius: 6, fontSize: 13 }}><strong>{t("parcelHealth.management")}:</strong> {ctx.management || "—"}</div>
            <div style={{ padding: "8px 12px", background: ctx.phenology_source?.startsWith("bioorchestrator") ? "#d4edda" : "#f0f4ff", borderRadius: 6, fontSize: 13 }}><strong>{t("parcelHealth.source")}:</strong> {ctx.phenology_source?.startsWith("bioorchestrator") ? t("parcelHealth.calibrated") : t("parcelHealth.default")}</div>
          </div>
          {ctx.phenology && (
            <div style={{ marginBottom: 16, padding: 12, background: "#f5f5f5", borderRadius: 8, maxWidth: 600 }}>
              <strong>{t("parcelHealth.phenology")}:</strong> {String(ctx.phenology.stage || '')} — Kc: {String(ctx.phenology.kc || '')} — Ky: {String(ctx.phenology.ky || '')}
              {ctx.season?.gdd_accumulated && ` (GDD: ${ctx.season.gdd_accumulated})`}
            </div>
          )}
          {yp?.yield_gap_pct !== undefined && (
            <div style={{ marginBottom: 16, padding: 12, background: (yp.yield_gap_pct ?? 0) > 10 ? "#fff3cd" : "#d4edda", borderRadius: 8, maxWidth: 400 }}>
              <strong>{t("parcelHealth.yieldGap")}:</strong> {yp.yield_gap_pct}% ({yp.yield_gap_kg_ha} kg/ha)
              <br /><span style={{ fontSize: 12 }}>{t("parcelHealth.expected")}: {yp.expected_yield_kg_ha} kg/ha</span>
            </div>
          )}
          {ctx.soil?.suitability && (
            <div style={{ marginBottom: 16, padding: 12, background: ctx.soil.suitability.overall === "suitable" ? "#d4edda" : "#fff3cd", borderRadius: 8, maxWidth: 400 }}>
              <strong>{t("parcelHealth.soilSuitability")}:</strong> pH {ctx.soil.suitability.ph_match ? "✅" : "❌"} — Texture {ctx.soil.suitability.texture_match ? "✅" : "❌"}
              {(ctx.soil.actual as any)?.awc_mm && <span> — AWC: {(ctx.soil.actual as any).awc_mm}mm</span>}
            </div>
          )}
          {ctx.soil_sensors && (ctx.soil_sensors as any).available && (
            <div style={{ marginBottom: 16, padding: 12, background: "#f0f4ff", borderRadius: 8, maxWidth: 600 }}>
              <strong>🌡️ {t("parcelHealth.soilSensors")}:</strong> pH {(ctx.soil_sensors as any).ph} — EC {(ctx.soil_sensors as any).ec_ds_m} dS/m — Moist {(ctx.soil_sensors as any).moisture_pct}% — T {(ctx.soil_sensors as any).temperature_c}°C
              <br /><span style={{ fontSize: 11, color: "#888" }}>{(ctx.soil_sensors as any).last_reading}</span>
            </div>
          )}
          <div style={{ fontSize: 11, color: "#999", marginTop: 16 }}>
            {t("parcelHealth.phenologySource")}: {ctx.phenology_source} — {t("parcelHealth.matchLevel")}: {ctx.match_level}
            {(ctx.provenance as any)?.short && ` — ${(ctx.provenance as any).short}`}
          </div>
        </>
      )}
    </div>
  );
}
