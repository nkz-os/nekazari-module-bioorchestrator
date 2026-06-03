import React, { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParcelSelector } from "../hooks/useParcelSelector";

interface CropItem { eppo_code: string; scientific_name: string; }
interface ComparisonRow {
  crop: string; best_variety: string;
  agronomics: { expected_yield_kg_ha: number; confidence_interval?: [number,number]; trials_analyzed: number; growing_season_days: number; operations_count: number; growing_season_source?: string };
  environmental: { carbon_fixed_tco2e_ha: number; n_fixation_kg_ha: number; n_fixation_source?: string };
  economic: { net_margin_eur_ha: number; total_cost_eur_ha: number; gross_revenue_eur_ha: number };
  soil_suitability: { overall: string; warnings: string[] };
  composite_score?: number;
  forage_value?: { crude_protein_pct: number | null; organic_matter_digestibility_pct: number | null };
  market_maturity?: { registered_varieties: number };
}
interface Result { comparisons: ComparisonRow[]; ranking: { by_margin: string[]; by_carbon: string[]; by_score: string[] }; target_environment: any; economic_inputs: any; }

export default function CropComparator() {
  const { t } = useTranslation();
  const { parcels, selected: selectedParcel, setSelected: setSelectedParcel } = useParcelSelector();
  const [availableCrops, setAvailableCrops] = useState<CropItem[]>([]);
  const [selectedCrops, setSelectedCrops] = useState<Set<string>>(new Set());
  const [seedPrice, setSeedPrice] = useState(1);
  const [harvestPrice, setHarvestPrice] = useState(1);
  const [operationCost, setOperationCost] = useState(1);
  const [result, setResult] = useState<Result | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";
    fetch(`${API_BASE}/api/graph/agriculture/crops`, { credentials: "include" })
      .then(r => r.json())
      .then(d => setAvailableCrops(d.crops || []))
      .catch(() => {});
  }, []);

  const toggleCrop = (eppo: string) => {
    const next = new Set(selectedCrops);
    next.has(eppo) ? next.delete(eppo) : next.add(eppo);
    setSelectedCrops(next);
  };

  const handleCompare = async () => {
    if (!selectedParcel || selectedCrops.size === 0) return;
    setLoading(true); setError("");
    const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";
    const crops = Array.from(selectedCrops).join(",");
    try {
      const res = await fetch(
        `${API_BASE}/api/graph/agriculture/compare-crops?parcel_id=${selectedParcel}&crops=${crops}&seed_price=${seedPrice}&harvest_price=${harvestPrice}&operation_cost=${operationCost}`,
        { credentials: "include" }
      );
      if (!res.ok) throw new Error((await res.json()).detail || "Error");
      setResult(await res.json());
    } catch (e: any) { setError(e.message); }
    finally { setLoading(false); }
  };

  const pricesAreDefault = seedPrice === 1 && harvestPrice === 1 && operationCost === 1;

  return (
    <div>
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-4">📊 {t("comparator.title")}</h2>

      <div style={{ marginBottom: 16, maxWidth: 400 }}>
        <select value={selectedParcel} onChange={e => setSelectedParcel(e.target.value)} style={{ width: "100%", padding: 8 }}>
          <option value="">{t("comparator.selectParcel")}</option>
          {parcels.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
        </select>
      </div>

      {/* Crop selector */}
      <div style={{ marginBottom: 16 }}>
        <strong>{t("comparator.selectCrops")}:</strong>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 8, maxWidth: 600 }}>
          {availableCrops.slice(0, 12).map(c => (
            <label key={c.eppo_code} style={{ padding: "4px 10px", background: selectedCrops.has(c.eppo_code) ? "#d4edda" : "#f5f5f5", borderRadius: 6, fontSize: 13, cursor: "pointer", border: "1px solid #ddd" }}>
              <input type="checkbox" checked={selectedCrops.has(c.eppo_code)} onChange={() => toggleCrop(c.eppo_code)} style={{ marginRight: 4 }} />
              {c.eppo_code} ({c.scientific_name?.slice(0, 20) || ""})
            </label>
          ))}
        </div>
      </div>

      {/* Economic inputs */}
      <div style={{ marginBottom: 16, padding: 12, background: "#f9f9f9", borderRadius: 8, maxWidth: 500 }}>
        <strong>{t("comparator.economicInputs")}</strong> <span style={{ fontSize: 11, color: "#999" }}>({t("comparator.optional")})</span>
        <div style={{ display: "flex", gap: 12, marginTop: 8, flexWrap: "wrap" }}>
          <div><label style={{ fontSize: 12 }}>{t("comparator.seedPrice")} (€/ha)</label><br /><input type="number" value={seedPrice} onChange={e => setSeedPrice(Number(e.target.value))} style={{ width: 100, padding: 4 }} /></div>
          <div><label style={{ fontSize: 12 }}>{t("comparator.harvestPrice")} (€/t)</label><br /><input type="number" value={harvestPrice} onChange={e => setHarvestPrice(Number(e.target.value))} style={{ width: 100, padding: 4 }} /></div>
          <div><label style={{ fontSize: 12 }}>{t("comparator.operationCost")} (€/op)</label><br /><input type="number" value={operationCost} onChange={e => setOperationCost(Number(e.target.value))} style={{ width: 100, padding: 4 }} /></div>
        </div>
      </div>

      <button onClick={handleCompare} disabled={loading || !selectedParcel || selectedCrops.size === 0}
              style={{ padding: "8px 20px", background: "#4caf50", color: "white", border: "none", borderRadius: 4, cursor: "pointer", marginBottom: 16 }}>
        {loading ? "⏳" : t("comparator.compare")}
      </button>
      {error && <div style={{ color: "red", marginBottom: 12 }}>{error}</div>}

      {result && (
        <div style={{ overflowX: "auto" }}>
          {pricesAreDefault && (
            <div style={{ padding: 8, background: "#fff3cd", borderRadius: 6, marginBottom: 12, fontSize: 13, maxWidth: 500 }}>
              ⚠️ {t("comparator.defaultPricesNote")}
            </div>
          )}
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ background: "#f5f5f5", textAlign: "left" }}>
                <th style={{ padding: 8 }}>{t("comparator.crop")}</th>
                <th style={{ padding: 8 }}>{t("comparator.variety")}</th>
                <th style={{ padding: 8 }}>Yield (kg/ha)</th>
                <th style={{ padding: 8 }}>C (tCO₂e/ha)</th>
                <th style={{ padding: 8 }}>N fix</th>
                <th style={{ padding: 8 }}>{pricesAreDefault ? "Score*" : "Margin (€/ha)"}</th>
                <th style={{ padding: 8 }}>{t("comparator.score")}</th>
                <th style={{ padding: 8 }}>{t("comparator.soil")}</th>
                <th style={{ padding: 8 }}>🌾 Forage</th>
                <th style={{ padding: 8 }}>🏷️ EU</th>
              </tr>
            </thead>
            <tbody>
              {result.comparisons.map((c, i) => (
                <tr key={c.crop} style={{ borderBottom: "1px solid #eee", background: i === 0 ? "#f0faf0" : "transparent" }}>
                  <td style={{ padding: 8, fontWeight: 600 }}>{c.crop}</td>
                  <td style={{ padding: 8 }}>{c.best_variety || "—"}</td>
                  <td style={{ padding: 8 }}>{c.agronomics.expected_yield_kg_ha.toLocaleString()}</td>
                  <td style={{ padding: 8 }}>{c.environmental.carbon_fixed_tco2e_ha}</td>
                  <td style={{ padding: 8, fontSize: 12 }}>
                    {c.environmental.n_fixation_kg_ha > 0 ? `+${c.environmental.n_fixation_kg_ha}` : "—"}
                    {c.environmental.n_fixation_source && (
                      <sup style={{ fontSize: 9, color: "#999", marginLeft: 2 }} title={c.environmental.n_fixation_source}>
                        {c.environmental.n_fixation_source.includes("AgriKnowledge") ? "🧪" :
                         c.environmental.n_fixation_source.includes("EPPO") ? "🔬" :
                         c.environmental.n_fixation_source.includes("measured") ? "📏" : "📋"}
                      </sup>
                    )}
                  </td>
                  <td style={{ padding: 8 }}>{pricesAreDefault ? c.agronomics.expected_yield_kg_ha.toLocaleString() : `${c.economic.net_margin_eur_ha.toLocaleString()} €`}</td>
                  <td style={{ padding: 8 }}>{c.composite_score ? "⭐".repeat(Math.min(5, Math.round(c.composite_score / 20))) : ""}</td>
                  <td style={{ padding: 8 }}>{c.soil_suitability.overall === "suitable" ? "✅" : "⚠️"}</td>
                  <td style={{ padding: 8, fontSize: 12 }}>
                    {c.forage_value?.crude_protein_pct != null ? `${c.forage_value.crude_protein_pct}% CP` : "—"}
                  </td>
                  <td style={{ padding: 8, fontSize: 12 }}>
                    {(c.market_maturity?.registered_varieties || 0) > 0 ? c.market_maturity!.registered_varieties.toLocaleString() : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {pricesAreDefault && <div style={{ fontSize: 11, color: "#999", marginTop: 4 }}>* {t("comparator.scoreNote")}</div>}
        </div>
      )}
    </div>
  );
}
