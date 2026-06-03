import React, { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParcelSelector } from "../hooks/useParcelSelector";
import type { RegenerativeSequenceResult, CropListCrop } from "../services/api";

const CLIMATE_ZONES = ["Csa", "Csb", "BSk", "Cfb", "Dfb", "Dfc", "BSh", "Cfa", "Aw"];
const MANAGEMENT_OPTIONS = ["any", "conventional", "organic"] as const;

export default function RegenerativeSequence() {
  const { t } = useTranslation();
  const { parcels, selected: selectedParcel, setSelected: setSelectedParcel } = useParcelSelector();
  const [climate, setClimate] = useState("Csa");
  const [targetProtein, setTargetProtein] = useState("VICFX");
  const [management, setManagement] = useState<string>("any");
  const [availableCrops, setAvailableCrops] = useState<CropListCrop[]>([]);
  const [result, setResult] = useState<RegenerativeSequenceResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";

  useEffect(() => {
    fetch(`${API_BASE}/api/graph/agriculture/crops`, { credentials: "include" })
      .then(r => r.json())
      .then(d => setAvailableCrops(d.crops || []))
      .catch(() => {});
  }, []);

  const handlePlan = async () => {
    setLoading(true); setError(""); setResult(null);
    const params = new URLSearchParams({ climate_class: climate, target_protein: targetProtein, management });
    if (selectedParcel) params.append("parcel_id", selectedParcel);
    try {
      const res = await fetch(
        `${API_BASE}/api/graph/agriculture/regenerative-sequence?${params}`,
        { credentials: "include" }
      );
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || err.error || "Error");
      }
      const data: RegenerativeSequenceResult = await res.json();
      if (data.error) throw new Error(data.error);
      setResult(data);
    } catch (e: any) { setError(e.message); }
    finally { setLoading(false); }
  };

  const riskColor = (risk: string) =>
    risk === "low" ? "#d4edda" : risk === "medium" ? "#fff3cd" : risk === "high" ? "#f8d7da" : "#f5f5f5";

  const riskTextColor = (risk: string) =>
    risk === "low" ? "#155724" : risk === "medium" ? "#856404" : risk === "high" ? "#721c24" : "#666";

  return (
    <div>
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-4">🧬 {t("regenerative.title")}</h2>

      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16, maxWidth: 700 }}>
        <div style={{ minWidth: 140 }}>
          <label style={{ fontSize: 12 }}>{t("regenerative.climate")}</label><br />
          <select value={climate} onChange={e => setClimate(e.target.value)} style={{ width: "100%", padding: 8 }}>
            {CLIMATE_ZONES.map(z => <option key={z} value={z}>{z}</option>)}
          </select>
        </div>
        <div style={{ minWidth: 180 }}>
          <label style={{ fontSize: 12 }}>{t("regenerative.protein")}</label><br />
          <select value={targetProtein} onChange={e => setTargetProtein(e.target.value)} style={{ width: "100%", padding: 8 }}>
            {availableCrops.map(c => <option key={c.eppo_code} value={c.eppo_code}>{c.eppo_code} ({c.scientific_name?.slice(0, 20)})</option>)}
          </select>
        </div>
        <div style={{ minWidth: 140 }}>
          <label style={{ fontSize: 12 }}>{t("regenerative.management")}</label><br />
          <select value={management} onChange={e => setManagement(e.target.value)} style={{ width: "100%", padding: 8 }}>
            {MANAGEMENT_OPTIONS.map(m => <option key={m} value={m}>{m}</option>)}
          </select>
        </div>
      </div>

      <div style={{ marginBottom: 16, maxWidth: 400 }}>
        <select value={selectedParcel} onChange={e => setSelectedParcel(e.target.value)} style={{ width: "100%", padding: 8 }}>
          <option value="">{t("rotation.selectParcel")} ({t("comparator.optional")})</option>
          {parcels.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
        </select>
      </div>

      <button onClick={handlePlan} disabled={loading}
        style={{ padding: "8px 20px", background: "#4caf50", color: "white", border: "none", borderRadius: 4, cursor: "pointer", marginBottom: 16 }}>
        {loading ? "⏳" : t("regenerative.plan")}
      </button>
      {error && <div style={{ color: "red", marginBottom: 12 }}>{error}</div>}

      {result && (
        <>
          {result.organic_data_warning && (
            <div style={{ padding: 10, background: "#fff3cd", borderRadius: 6, marginBottom: 12, fontSize: 13, maxWidth: 700 }}>
              ⚠️ {result.organic_data_warning}
            </div>
          )}

          <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 16 }}>
            <div style={{ flex: "1 1 280px", padding: 16, background: "#f0f9ff", borderRadius: 8, border: "1px solid #bae6fd" }}>
              <div style={{ fontSize: 12, color: "#0369a1", textTransform: "uppercase", marginBottom: 4 }}>{t("regenerative.coverCrop")}</div>
              <div style={{ fontSize: 18, fontWeight: 700 }}>{result.cover_crop_common}</div>
              <div style={{ fontSize: 12, color: "#666" }}>{result.cover_crop_scientific} ({result.cover_crop_type})</div>
              <div style={{ marginTop: 12, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, fontSize: 13 }}>
                <div>Biomass: <strong>{result.cover_biomass_t_ha} t/ha</strong></div>
                <div>C/N: <strong>{result.c_n_ratio}</strong></div>
                <div>Termination: <strong>{result.termination_method}</strong></div>
                <div>GDD to term: <strong>{result.termination_gdd}</strong></div>
              </div>
            </div>

            <div style={{ flex: "1 1 280px", padding: 16, background: "#f0fdf4", borderRadius: 8, border: "1px solid #bbf7d0" }}>
              <div style={{ fontSize: 12, color: "#15803d", textTransform: "uppercase", marginBottom: 4 }}>{t("regenerative.proteinCrop")}</div>
              <div style={{ fontSize: 18, fontWeight: 700 }}>{result.protein_crop_common}</div>
              <div style={{ fontSize: 12, color: "#666" }}>{result.protein_crop_scientific}</div>
              {result.protein_variety && <div style={{ fontSize: 13, marginTop: 4 }}>Variety: <strong>{result.protein_variety}</strong></div>}
              <div style={{ marginTop: 12, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, fontSize: 13 }}>
                {result.expected_protein_yield_kg_ha != null && (
                  <div>Yield: <strong>{result.expected_protein_yield_kg_ha.toLocaleString()} kg/ha</strong></div>
                )}
                <div>Protein: <strong>{result.protein_kg_ha} kg/ha</strong></div>
                <div>Mgmt: <strong>{result.management_mode}</strong></div>
              </div>
            </div>
          </div>

          <div style={{ padding: 12, background: "#f5f5f5", borderRadius: 8, marginBottom: 16, maxWidth: 700 }}>
            <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>📅 {t("regenerative.dates")}</div>
            <div style={{ display: "flex", gap: 16, flexWrap: "wrap", fontSize: 13 }}>
              <div>Cover sowing: <strong>{result.cover_crop_sowing_date}</strong></div>
              <div>→ Termination: <strong>{result.termination_date_estimate}</strong></div>
              <div>→ Protein sowing: <strong>{result.protein_crop_sowing_date}</strong></div>
              <div>→ Harvest: <strong>{result.protein_crop_harvest_date}</strong></div>
            </div>
          </div>

          <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 16 }}>
            <div style={{ flex: "1 1 280px", padding: 16, background: "#fefce8", borderRadius: 8, border: "1px solid #fde68a" }}>
              <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>🧪 {t("regenerative.nitrogen")}</div>
              <div style={{ fontSize: 13 }}>
                <div>Cover N total: <strong>{result.n_cover_total_kg_ha} kg/ha</strong></div>
                <div>Cover N available (50%): <strong>{result.n_cover_available_kg_ha} kg/ha</strong></div>
                <div>Protein N fixed: <strong>{result.n_protein_fixed_kg_ha} kg/ha</strong></div>
              </div>
            </div>

            <div style={{ flex: "1 1 280px", padding: 16, background: riskColor(result.water_balance_risk), borderRadius: 8, border: "1px solid #ddd" }}>
              <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8, color: riskTextColor(result.water_balance_risk) }}>
                💧 {t("regenerative.waterBalance")}: {result.water_balance_risk.toUpperCase()}
              </div>
              <div style={{ fontSize: 12 }}>
                <div>ETc: {result.water_balance_detail.crop_etc_mm} mm</div>
                <div>Effective rain: {result.water_balance_detail.effective_rainfall_mm} mm</div>
                <div>Soil AWC: {result.water_balance_detail.soil_awc_mm} mm</div>
                <div style={{ fontWeight: 600, marginTop: 4 }}>
                  Deficit: {result.water_balance_detail.deficit_mm} mm
                </div>
              </div>
            </div>
          </div>

          {/* Carbon Projection */}
          {result.carbon_projection && (
            <div style={{ marginBottom: 16 }}>
              <div style={{ flex: "1 1 100%", padding: 16, background: "#f0fdf4", borderRadius: 8, border: "1px solid #bbf7d0" }}>
                <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>🌍 {t("regenerative.carbon.title")}</div>
                <div style={{ display: "flex", gap: 16, flexWrap: "wrap", fontSize: 13 }}>
                  <div style={{ flex: 1, minWidth: 200 }}>
                    {result.carbon_projection.current_soc_pct != null && (
                      <div style={{ marginBottom: 8 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                          <span>{t("regenerative.carbon.currentSoc")}: <strong>{result.carbon_projection.current_soc_pct}%</strong></span>
                          <span>{t("regenerative.carbon.targetSoc")}: <strong>{result.carbon_projection.target_soc_pct}%</strong></span>
                        </div>
                        <div style={{ height: 10, background: "#e9ecef", borderRadius: 5, overflow: "hidden" }}>
                          <div style={{
                            height: "100%", background: "linear-gradient(90deg, #28a745, #20c997)", borderRadius: 5,
                            width: `${Math.min(100, ((result.carbon_projection.current_soc_pct ?? 0) / result.carbon_projection.target_soc_pct) * 100)}%`,
                            transition: "width 0.5s"
                          }} />
                        </div>
                        {result.carbon_projection.projected_soc_pct != null && (
                          <div style={{ marginTop: 4, fontSize: 11, color: "#28a745" }}>
                            → {t("regenerative.carbon.projectedSoc")}: {result.carbon_projection.projected_soc_pct}%
                            ({result.carbon_projection.soc_delta_pct > 0 ? "+" : ""}{result.carbon_projection.soc_delta_pct}%)
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                  <div style={{ flex: 1, minWidth: 180 }}>
                    <div>🌱 CO₂e: <strong>{result.carbon_projection.co2e_sequestered_ton_ha} t/ha</strong></div>
                    <div>🧪 {t("regenerative.carbon.fertilizerSaved")}: <strong>{result.carbon_projection.fertilizer_n_saved_kg_ha} kg/ha</strong></div>
                    <div>💰 {t("regenerative.carbon.savings")}: <strong>{result.carbon_projection.fertilizer_savings_eur_ha} €/ha</strong></div>
                    {result.carbon_projection.years_to_target != null && (
                      <div style={{ marginTop: 4 }}>⏳ {t("regenerative.carbon.yearsToTarget")}: <strong>{result.carbon_projection.years_to_target}</strong></div>
                    )}
                    <div style={{ fontSize: 10, color: "#999", marginTop: 4 }}>{result.carbon_projection.soil_texture}</div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {result.alternatives.length > 0 && (
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>🔄 {t("regenerative.alternatives")}</div>
              <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
                {result.alternatives.map((alt, i) => (
                  <div key={i} style={{ flex: "1 1 200px", padding: 12, background: "#f9f9f9", borderRadius: 8, border: "1px solid #eee", fontSize: 13, minWidth: 180 }}>
                    <div style={{ fontWeight: 600 }}>{alt.cover_crop_common}</div>
                    <div style={{ fontSize: 11, color: "#666" }}>{alt.type} — C/N: {alt.c_n_ratio}</div>
                    <div style={{ marginTop: 4 }}>Biomass: {alt.biomass_t_ha} t/ha</div>
                    <div>N avail: {alt.n_available_kg_ha} kg/ha</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div style={{ padding: 10, background: "#f5f5f5", borderRadius: 8, fontSize: 11, color: "#888", maxWidth: 700 }}>
            {t("regenerative.provenance")}: {result.provenance.cover_crop_source} | {result.provenance.n_fixation_source} | {result.provenance.yield_source} | {result.provenance.climate_source}
          </div>
        </>
      )}
    </div>
  );
}
