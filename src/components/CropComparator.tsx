import React, { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParcelContext } from '../context/ParcelContext';
import { Card, Button, Stack, EmptyState, Skeleton } from '@nekazari/ui-kit';
import { AlertTriangle, Activity } from 'lucide-react';

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
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
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

  if (parcelLoading) return <Skeleton variant="rect" height="200px" />;
  if (parcelError) return <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={parcelError} />;
  if (!selectedParcel) return <EmptyState icon={<Activity className="w-8 h-8" />} title={t('app.selectParcelPrompt')} />;

  return (
    <Stack gap="section">
      <div>
        <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-1">📊 {t("comparator.title")}</h2>
        <p className="text-nkz-text-muted text-sm">{t("comparator.subtitle")}</p>
      </div>

      {/* Crop selector */}
      <div>
        <strong className="text-nkz-sm block mb-nkz-inline">{t("comparator.selectCrops")}:</strong>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          {availableCrops.slice(0, 12).map(c => (
            <label
              key={c.eppo_code}
              className={`inline-flex items-center px-2 py-1 rounded-nkz-md border text-nkz-xs cursor-pointer ${
                selectedCrops.has(c.eppo_code)
                  ? 'bg-nkz-positive-soft border-nkz-positive'
                  : 'bg-nkz-surface border-nkz-border'
              }`}
            >
              <input type="checkbox" checked={selectedCrops.has(c.eppo_code)} onChange={() => toggleCrop(c.eppo_code)} className="mr-1" />
              {c.eppo_code} ({c.scientific_name?.slice(0, 20) || ""})
            </label>
          ))}
        </div>
      </div>

      {/* Economic inputs */}
      <Card padding="md">
        <strong className="text-nkz-sm">{t("comparator.economicInputs")}</strong>{" "}
        <span className="text-nkz-xs text-nkz-text-muted">({t("comparator.optional")})</span>
        <div style={{ display: "flex", gap: 12, marginTop: 8, flexWrap: "wrap" }}>
          <div>
            <label className="text-nkz-xs">{t("comparator.seedPrice")} (€/ha)</label><br />
            <input type="number" value={seedPrice} onChange={e => setSeedPrice(Number(e.target.value))} className="w-24 px-2 py-1 border border-nkz-border rounded-nkz-sm text-nkz-sm" />
          </div>
          <div>
            <label className="text-nkz-xs">{t("comparator.harvestPrice")} (€/t)</label><br />
            <input type="number" value={harvestPrice} onChange={e => setHarvestPrice(Number(e.target.value))} className="w-24 px-2 py-1 border border-nkz-border rounded-nkz-sm text-nkz-sm" />
          </div>
          <div>
            <label className="text-nkz-xs">{t("comparator.operationCost")} (€/op)</label><br />
            <input type="number" value={operationCost} onChange={e => setOperationCost(Number(e.target.value))} className="w-24 px-2 py-1 border border-nkz-border rounded-nkz-sm text-nkz-sm" />
          </div>
        </div>
      </Card>

      <Button
        onClick={handleCompare}
        disabled={loading || !selectedParcel || selectedCrops.size === 0}
        variant="primary"
        loading={loading}
      >
        {t("comparator.compare")}
      </Button>

      {error && <div className="text-nkz-negative text-nkz-sm">{error}</div>}

      {result && (
        <div style={{ overflowX: "auto" }}>
          {pricesAreDefault && (
            <div className="px-3 py-2 bg-nkz-warning-soft rounded-nkz-md text-nkz-xs border border-nkz-warning max-w-xl mb-nkz-stack">
              ⚠️ {t("comparator.defaultPricesNote")}
            </div>
          )}
          <table className="w-full border-collapse text-nkz-xs">
            <thead>
              <tr className="bg-nkz-surface text-left">
                <th className="p-2">{t("comparator.crop")}</th>
                <th className="p-2">{t("comparator.variety")}</th>
                <th className="p-2">Yield (kg/ha)</th>
                <th className="p-2">C (tCO₂e/ha)</th>
                <th className="p-2">N fix</th>
                <th className="p-2">{pricesAreDefault ? "Score*" : "Margin (€/ha)"}</th>
                <th className="p-2">{t("comparator.score")}</th>
                <th className="p-2">{t("comparator.soil")}</th>
                <th className="p-2">🌾 Forage</th>
                <th className="p-2">🏷️ EU</th>
              </tr>
            </thead>
            <tbody>
              {result.comparisons.map((c, i) => (
                <tr key={c.crop} className={`border-b border-nkz-border ${i === 0 ? 'bg-nkz-positive-soft' : ''}`}>
                  <td className="p-2 font-semibold">{c.crop}</td>
                  <td className="p-2">{c.best_variety || "—"}</td>
                  <td className="p-2">{c.agronomics.expected_yield_kg_ha.toLocaleString()}</td>
                  <td className="p-2">{c.environmental.carbon_fixed_tco2e_ha}</td>
                  <td className="p-2 text-nkz-xs">
                    {c.environmental.n_fixation_kg_ha > 0 ? `+${c.environmental.n_fixation_kg_ha}` : "—"}
                    {c.environmental.n_fixation_source && (
                      <sup className="text-[9px] text-nkz-text-muted ml-0.5" title={c.environmental.n_fixation_source}>
                        {c.environmental.n_fixation_source.includes("AgriKnowledge") ? "🧪" :
                         c.environmental.n_fixation_source.includes("EPPO") ? "🔬" :
                         c.environmental.n_fixation_source.includes("measured") ? "📏" : "📋"}
                      </sup>
                    )}
                  </td>
                  <td className="p-2">{pricesAreDefault ? c.agronomics.expected_yield_kg_ha.toLocaleString() : `${c.economic.net_margin_eur_ha.toLocaleString()} €`}</td>
                  <td className="p-2">{c.composite_score ? "⭐".repeat(Math.min(5, Math.round(c.composite_score / 20))) : ""}</td>
                  <td className="p-2">{c.soil_suitability.overall === "suitable" ? "✅" : "⚠️"}</td>
                  <td className="p-2 text-nkz-xs">
                    {c.forage_value?.crude_protein_pct != null ? `${c.forage_value.crude_protein_pct}% CP` : "—"}
                  </td>
                  <td className="p-2 text-nkz-xs">
                    {(c.market_maturity?.registered_varieties || 0) > 0 ? c.market_maturity!.registered_varieties.toLocaleString() : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {pricesAreDefault && <div className="text-nkz-xs text-nkz-text-muted mt-1">* {t("comparator.scoreNote")}</div>}
        </div>
      )}
    </Stack>
  );
}
