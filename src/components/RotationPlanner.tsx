import React, { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParcelSelector } from "../hooks/useParcelSelector";

interface YearEntry {
  year: number; crop: string; variety: string;
  expected_yield_kg_ha: number; carbon_fixed_tco2e: number;
  net_margin_eur_ha: number; n_balance_kg_ha: number;
  n_fixation_kg_ha: number; n_requirement_kg_ha: number;
  soil_n_pool_after_kg_ha: number; rotation_warning?: string;
  pest_risk?: { shared_pests?: string[]; shared_count?: number; risk_level?: string };
}
interface PacRule { id: string; pass: boolean | null; detail: string; }
interface PacCompliance { score: number; max_score: number; rules: PacRule[]; disclaimer: string; }
interface PlanResult { plan: YearEntry[]; cumulative: { total_yield_kg_ha: number; total_carbon_fixed_tco2e: number; total_net_margin_eur_ha: number; final_soil_n_pool_kg_ha: number }; pac_compliance?: PacCompliance; }

export default function RotationPlanner() {
  const { t } = useTranslation();
  const { parcels, selected: selectedParcel, setSelected: setSelectedParcel } = useParcelSelector();
  const [years, setYears] = useState(3);
  const [seedPrice, setSeedPrice] = useState(1);
  const [harvestPrice, setHarvestPrice] = useState(1);
  const [operationCost, setOperationCost] = useState(1);
  const [result, setResult] = useState<PlanResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handlePlan = async () => {
    if (!selectedParcel) return;
    setLoading(true); setError("");
    const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";
    try {
      const res = await fetch(
        `${API_BASE}/api/graph/agriculture/rotation-plan?parcel_id=${selectedParcel}&years=${years}&seed_price=${seedPrice}&harvest_price=${harvestPrice}&operation_cost=${operationCost}`,
        { credentials: "include" }
      );
      if (!res.ok) throw new Error((await res.json()).detail || "Error");
      setResult(await res.json());
    } catch (e: any) { setError(e.message); }
    finally { setLoading(false); }
  };

  return (
    <div>
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-4">🔄 {t("rotationPlanner.title")}</h2>

      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16, maxWidth: 600 }}>
        <div style={{ flex: 1, minWidth: 200 }}>
          <select value={selectedParcel} onChange={e => setSelectedParcel(e.target.value)} style={{ width: "100%", padding: 8 }}>
            <option value="">{t("rotationPlanner.selectParcel")}</option>
            {parcels.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
          </select>
        </div>
        <div>
          <label style={{ fontSize: 12 }}>{t("rotationPlanner.years")}</label><br />
          <input type="range" min={2} max={6} value={years} onChange={e => setYears(Number(e.target.value))} style={{ width: 150 }} />
          <span style={{ marginLeft: 8 }}>{years}</span>
        </div>
      </div>

      <div style={{ marginBottom: 16, padding: 12, background: "#f9f9f9", borderRadius: 8, maxWidth: 500 }}>
        <strong>{t("rotationPlanner.economicInputs")}</strong> <span style={{ fontSize: 11, color: "#999" }}>({t("comparator.optional")})</span>
        <div style={{ display: "flex", gap: 12, marginTop: 8, flexWrap: "wrap" }}>
          <div><label style={{ fontSize: 12 }}>{t("comparator.seedPrice")}</label><br /><input type="number" value={seedPrice} onChange={e => setSeedPrice(Number(e.target.value))} style={{ width: 100, padding: 4 }} /></div>
          <div><label style={{ fontSize: 12 }}>{t("comparator.harvestPrice")}</label><br /><input type="number" value={harvestPrice} onChange={e => setHarvestPrice(Number(e.target.value))} style={{ width: 100, padding: 4 }} /></div>
          <div><label style={{ fontSize: 12 }}>{t("comparator.operationCost")}</label><br /><input type="number" value={operationCost} onChange={e => setOperationCost(Number(e.target.value))} style={{ width: 100, padding: 4 }} /></div>
        </div>
      </div>

      <button onClick={handlePlan} disabled={loading || !selectedParcel}
              style={{ padding: "8px 20px", background: "#4caf50", color: "white", border: "none", borderRadius: 4, cursor: "pointer", marginBottom: 16 }}>
        {loading ? "⏳" : t("rotationPlanner.calculate")}
      </button>
      {error && <div style={{ color: "red", marginBottom: 12 }}>{error}</div>}

      {result && (
        <>
          {/* Timeline */}
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
            {result.plan.map((entry, i) => (
              <div key={i} style={{ flex: "1 1 180px", padding: 12, background: entry.rotation_warning ? "#fff3cd" : "#f0f4ff", borderRadius: 8, border: "1px solid #ddd", minWidth: 160 }}>
                <div style={{ fontSize: 12, color: "#666" }}>{t("rotationPlanner.year")} {entry.year}</div>
                <div style={{ fontWeight: 600, fontSize: 15 }}>{entry.crop}</div>
                <div style={{ fontSize: 12 }}>{entry.variety || "—"}</div>
                <div style={{ marginTop: 8, fontSize: 12 }}>
                  {entry.expected_yield_kg_ha.toLocaleString()} kg/ha<br />
                  🌱 {entry.carbon_fixed_tco2e} tCO₂e<br />
                  🧪 N: {entry.n_balance_kg_ha > 0 ? "+" : ""}{entry.n_balance_kg_ha} kg/ha
                  {entry.n_fixation_kg_ha > 0 && <span> (fixes {entry.n_fixation_kg_ha})</span>}
                </div>
                {entry.rotation_warning && <div style={{ marginTop: 4, fontSize: 11, color: "#856404" }}>⚠️ {entry.rotation_warning}</div>}
                {entry.pest_risk && entry.pest_risk.risk_level && entry.pest_risk.risk_level !== "none" && entry.pest_risk.risk_level !== "unknown" && (
                  <div style={{ marginTop: 4, fontSize: 11, color: entry.pest_risk.risk_level === "high" ? "#721c24" : "#856404" }}>
                    🐛 {entry.pest_risk.shared_pests?.slice(0, 3).join(", ")}
                    {(entry.pest_risk.shared_count || 0) > 3 && ` +${(entry.pest_risk.shared_count || 0) - 3} more`}
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Cumulative */}
          <div style={{ padding: 16, background: "#d4edda", borderRadius: 8, maxWidth: 500 }}>
            <strong>{t("rotationPlanner.cumulative")} ({years} {t("rotationPlanner.yearsLabel")})</strong>
            <div style={{ display: "flex", gap: 24, marginTop: 8, flexWrap: "wrap" }}>
              <div>🌾 {result.cumulative.total_yield_kg_ha.toLocaleString()} kg/ha</div>
              <div>🌍 {result.cumulative.total_carbon_fixed_tco2e} tCO₂e</div>
              <div>💰 {result.cumulative.total_net_margin_eur_ha.toLocaleString()} €/ha</div>
              <div>🧪 N pool: {result.cumulative.final_soil_n_pool_kg_ha} kg/ha</div>
            </div>
          </div>

          {/* PAC Compliance */}
          {result.pac_compliance && (
            <div style={{ marginTop: 16, maxWidth: 600 }}>
              <strong style={{ fontSize: 15 }}>🇪🇺 {t("rotationPlanner.pac.title")}</strong>
              <div style={{ display: "flex", alignItems: "center", gap: 16, marginTop: 8, marginBottom: 12 }}>
                <div style={{
                  width: 72, height: 72, borderRadius: "50%",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  background: result.pac_compliance.score >= 80 ? "#d4edda" : result.pac_compliance.score >= 50 ? "#fff3cd" : "#f8d7da",
                  border: `4px solid ${result.pac_compliance.score >= 80 ? "#28a745" : result.pac_compliance.score >= 50 ? "#ffc107" : "#dc3545"}`,
                  fontSize: 20, fontWeight: 700
                }}>
                  {result.pac_compliance.score}%
                </div>
                <div style={{ fontSize: 12, color: "#888" }}>
                  {t("rotationPlanner.pac.score")}: {result.pac_compliance.score}/{result.pac_compliance.max_score}
                </div>
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {result.pac_compliance.rules.map((rule) => (
                  <div key={rule.id} style={{
                    padding: "8px 12px", borderRadius: 6, fontSize: 13,
                    background: rule.pass === true ? "#d4edda" : rule.pass === false ? "#f8d7da" : "#f5f5f5",
                    border: `1px solid ${rule.pass === true ? "#c3e6cb" : rule.pass === false ? "#f5c6cb" : "#ddd"}`,
                  }}>
                    <span style={{ marginRight: 8 }}>
                      {rule.pass === true ? "✅" : rule.pass === false ? "❌" : "⊘"}
                    </span>
                    <strong>{t(`rotationPlanner.pac.rule.${rule.id}`)}</strong>
                    <span style={{ marginLeft: 8, color: "#666" }}>{rule.detail}</span>
                  </div>
                ))}
              </div>
              <div style={{ fontSize: 11, color: "#aaa", marginTop: 8, fontStyle: "italic" }}>
                {result.pac_compliance.disclaimer}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
