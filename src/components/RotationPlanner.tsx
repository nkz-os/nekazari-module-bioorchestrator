import React, { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParcelSelector } from "../hooks/useParcelSelector";

interface YearEntry {
  year: number; crop: string; variety: string;
  expected_yield_kg_ha: number; carbon_fixed_tco2e: number;
  net_margin_eur_ha: number; n_balance_kg_ha: number;
  n_fixation_kg_ha: number; n_requirement_kg_ha: number;
  soil_n_pool_after_kg_ha: number; rotation_warning?: string;
}
interface PlanResult { plan: YearEntry[]; cumulative: { total_yield_kg_ha: number; total_carbon_fixed_tco2e: number; total_net_margin_eur_ha: number; final_soil_n_pool_kg_ha: number }; }

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
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-4">🔄 {t("rotation.title")}</h2>

      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16, maxWidth: 600 }}>
        <div style={{ flex: 1, minWidth: 200 }}>
          <select value={selectedParcel} onChange={e => setSelectedParcel(e.target.value)} style={{ width: "100%", padding: 8 }}>
            <option value="">{t("rotation.selectParcel")}</option>
            {parcels.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
          </select>
        </div>
        <div>
          <label style={{ fontSize: 12 }}>{t("rotation.years")}</label><br />
          <input type="range" min={2} max={6} value={years} onChange={e => setYears(Number(e.target.value))} style={{ width: 150 }} />
          <span style={{ marginLeft: 8 }}>{years}</span>
        </div>
      </div>

      <div style={{ marginBottom: 16, padding: 12, background: "#f9f9f9", borderRadius: 8, maxWidth: 500 }}>
        <strong>{t("rotation.economicInputs")}</strong> <span style={{ fontSize: 11, color: "#999" }}>({t("comparator.optional")})</span>
        <div style={{ display: "flex", gap: 12, marginTop: 8, flexWrap: "wrap" }}>
          <div><label style={{ fontSize: 12 }}>{t("comparator.seedPrice")}</label><br /><input type="number" value={seedPrice} onChange={e => setSeedPrice(Number(e.target.value))} style={{ width: 100, padding: 4 }} /></div>
          <div><label style={{ fontSize: 12 }}>{t("comparator.harvestPrice")}</label><br /><input type="number" value={harvestPrice} onChange={e => setHarvestPrice(Number(e.target.value))} style={{ width: 100, padding: 4 }} /></div>
          <div><label style={{ fontSize: 12 }}>{t("comparator.operationCost")}</label><br /><input type="number" value={operationCost} onChange={e => setOperationCost(Number(e.target.value))} style={{ width: 100, padding: 4 }} /></div>
        </div>
      </div>

      <button onClick={handlePlan} disabled={loading || !selectedParcel}
              style={{ padding: "8px 20px", background: "#4caf50", color: "white", border: "none", borderRadius: 4, cursor: "pointer", marginBottom: 16 }}>
        {loading ? "⏳" : t("rotation.calculate")}
      </button>
      {error && <div style={{ color: "red", marginBottom: 12 }}>{error}</div>}

      {result && (
        <>
          {/* Timeline */}
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
            {result.plan.map((entry, i) => (
              <div key={i} style={{ flex: "1 1 180px", padding: 12, background: entry.rotation_warning ? "#fff3cd" : "#f0f4ff", borderRadius: 8, border: "1px solid #ddd", minWidth: 160 }}>
                <div style={{ fontSize: 12, color: "#666" }}>{t("rotation.year")} {entry.year}</div>
                <div style={{ fontWeight: 600, fontSize: 15 }}>{entry.crop}</div>
                <div style={{ fontSize: 12 }}>{entry.variety || "—"}</div>
                <div style={{ marginTop: 8, fontSize: 12 }}>
                  {entry.expected_yield_kg_ha.toLocaleString()} kg/ha<br />
                  🌱 {entry.carbon_fixed_tco2e} tCO₂e<br />
                  🧪 N: {entry.n_balance_kg_ha > 0 ? "+" : ""}{entry.n_balance_kg_ha} kg/ha
                  {entry.n_fixation_kg_ha > 0 && <span> (fixes {entry.n_fixation_kg_ha})</span>}
                </div>
                {entry.rotation_warning && <div style={{ marginTop: 4, fontSize: 11, color: "#856404" }}>⚠️ {entry.rotation_warning}</div>}
              </div>
            ))}
          </div>

          {/* Cumulative */}
          <div style={{ padding: 16, background: "#d4edda", borderRadius: 8, maxWidth: 500 }}>
            <strong>{t("rotation.cumulative")} ({years} {t("rotation.yearsLabel")})</strong>
            <div style={{ display: "flex", gap: 24, marginTop: 8, flexWrap: "wrap" }}>
              <div>🌾 {result.cumulative.total_yield_kg_ha.toLocaleString()} kg/ha</div>
              <div>🌍 {result.cumulative.total_carbon_fixed_tco2e} tCO₂e</div>
              <div>💰 {result.cumulative.total_net_margin_eur_ha.toLocaleString()} €/ha</div>
              <div>🧪 N pool: {result.cumulative.final_soil_n_pool_kg_ha} kg/ha</div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
