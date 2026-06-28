import React, { useState, useEffect } from "react";
import { useTranslation } from '@nekazari/sdk';
import { useParcelContext } from "../context/ParcelContext";
import { Card, Button, Select, Stack } from "@nekazari/ui-kit";
import ContextEmptyState from "./shared/ContextEmptyState";
import type { RegenerativeSequenceResult, CropListCrop } from "../services/api";

const CLIMATE_ZONES = ["Csa", "Csb", "BSk", "Cfb", "Dfb", "Dfc", "BSh", "Cfa", "Aw"];
const MANAGEMENT_OPTIONS = ["any", "conventional", "organic"] as const;

export default function RegenerativeSequence() {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
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

  if (parcelLoading) return <div className="text-nkz-text-muted p-4">⏳ {t("common.loading")}</div>;
  if (parcelError) return <ContextEmptyState message={parcelError} variant="warning" actionLabel={t("panel.retry")} onAction={() => window.location.reload()} />;
  if (!selectedParcel) return <ContextEmptyState message={t("alerts.selectPrompt")} variant="info" />;

  return (
    <Stack gap="section">
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary">🧬 {t("regenerative.title")}</h2>

      <div className="flex gap-3 flex-wrap mb-2 max-w-3xl items-end">
        <div className="min-w-[140px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t("regenerative.climate")}</label>
          <Select
            value={climate}
            onValueChange={setClimate}
            options={CLIMATE_ZONES.map(z => ({ value: z, label: z }))}
          />
        </div>
        <div className="min-w-[180px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t("regenerative.protein")}</label>
          <Select
            value={targetProtein}
            onValueChange={setTargetProtein}
            options={availableCrops.map(c => ({ value: c.eppo_code, label: `${c.eppo_code} (${c.scientific_name?.slice(0, 20)})` }))}
          />
        </div>
        <div className="min-w-[140px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t("regenerative.management")}</label>
          <Select
            value={management}
            onValueChange={setManagement}
            options={MANAGEMENT_OPTIONS.map(m => ({ value: m, label: m }))}
          />
        </div>
        <Button variant="primary" onClick={handlePlan} disabled={loading}>
          {loading ? "⏳" : t("regenerative.plan")}
        </Button>
      </div>
      {error && <div className="text-nkz-danger text-sm mb-3">{error}</div>}

      {result && (
        <Stack gap="stack">
          {result.organic_data_warning && (
            <div className="bg-nkz-warning-soft border border-nkz-warning rounded-nkz-md p-3 text-sm max-w-3xl">
              ⚠️ {result.organic_data_warning}
            </div>
          )}

          <div className="flex gap-4 flex-wrap">
            <Card padding="md" className="flex-1 min-w-[280px]">
              <div className="text-nkz-xs text-nkz-info uppercase mb-1 font-semibold">{t("regenerative.coverCrop")}</div>
              <div className="text-nkz-lg font-bold">{result.cover_crop_common}</div>
              <div className="text-nkz-xs text-nkz-text-muted">{result.cover_crop_scientific} ({result.cover_crop_type})</div>
              <div className="mt-3 grid grid-cols-2 gap-2 text-sm">
                <div>Biomass: <strong>{result.cover_biomass_t_ha} t/ha</strong></div>
                <div>C/N: <strong>{result.c_n_ratio}</strong></div>
                <div>Termination: <strong>{result.termination_method}</strong></div>
                <div>GDD to term: <strong>{result.termination_gdd}</strong></div>
              </div>
            </Card>

            <Card padding="md" className="flex-1 min-w-[280px]">
              <div className="text-nkz-xs text-nkz-success uppercase mb-1 font-semibold">{t("regenerative.proteinCrop")}</div>
              <div className="text-nkz-lg font-bold">{result.protein_crop_common}</div>
              <div className="text-nkz-xs text-nkz-text-muted">{result.protein_crop_scientific}</div>
              {result.protein_variety && <div className="text-sm mt-1">Variety: <strong>{result.protein_variety}</strong></div>}
              <div className="mt-3 grid grid-cols-2 gap-2 text-sm">
                {result.expected_protein_yield_kg_ha != null && (
                  <div>Yield: <strong>{result.expected_protein_yield_kg_ha.toLocaleString()} kg/ha</strong></div>
                )}
                <div>Protein: <strong>{result.protein_kg_ha} kg/ha</strong></div>
                <div>Mgmt: <strong>{result.management_mode}</strong></div>
              </div>
            </Card>
          </div>

          <Card padding="md" className="max-w-3xl">
            <div className="text-sm font-semibold mb-2">📅 {t("regenerative.dates")}</div>
            <div className="flex gap-4 flex-wrap text-sm">
              <div>Cover sowing: <strong>{result.cover_crop_sowing_date}</strong></div>
              <div>→ Termination: <strong>{result.termination_date_estimate}</strong></div>
              <div>→ Protein sowing: <strong>{result.protein_crop_sowing_date}</strong></div>
              <div>→ Harvest: <strong>{result.protein_crop_harvest_date}</strong></div>
            </div>
          </Card>

          <div className="flex gap-4 flex-wrap">
            <Card padding="md" className="flex-1 min-w-[280px]">
              <div className="text-sm font-semibold mb-2">🧪 {t("regenerative.nitrogen")}</div>
              <div className="text-sm">
                <div>Cover N total: <strong>{result.n_cover_total_kg_ha} kg/ha</strong></div>
                <div>Cover N available (50%): <strong>{result.n_cover_available_kg_ha} kg/ha</strong></div>
                <div>Protein N fixed: <strong>{result.n_protein_fixed_kg_ha} kg/ha</strong></div>
              </div>
            </Card>

            <Card padding="md" className={`flex-1 min-w-[280px] ${result.water_balance_risk === "low" ? "bg-nkz-success-soft border-nkz-success" : result.water_balance_risk === "medium" ? "bg-nkz-warning-soft border-nkz-warning" : "bg-nkz-danger-soft border-nkz-danger"}`}>
              <div className={`text-sm font-semibold mb-2 ${result.water_balance_risk === "low" ? "text-nkz-success" : result.water_balance_risk === "medium" ? "text-nkz-warning" : "text-nkz-danger"}`}>
                💧 {t("regenerative.waterBalance")}: {result.water_balance_risk.toUpperCase()}
              </div>
              <div className="text-xs">
                <div>ETc: {result.water_balance_detail.crop_etc_mm} mm</div>
                <div>Effective rain: {result.water_balance_detail.effective_rainfall_mm} mm</div>
                <div>Soil AWC: {result.water_balance_detail.soil_awc_mm} mm</div>
                <div className="font-semibold mt-1">
                  Deficit: {result.water_balance_detail.deficit_mm} mm
                </div>
              </div>
            </Card>
          </div>

          {/* Carbon Projection */}
          {result.carbon_projection && (
            <Card padding="md">
              <div className="text-sm font-semibold mb-2">🌍 {t("regenerative.carbon.title")}</div>
              <div className="flex gap-4 flex-wrap text-sm">
                <div className="flex-1 min-w-[200px]">
                  {result.carbon_projection.current_soc_pct != null && (
                    <div className="mb-2">
                      <div className="flex justify-between mb-1">
                        <span>{t("regenerative.carbon.currentSoc")}: <strong>{result.carbon_projection.current_soc_pct}%</strong></span>
                        <span>{t("regenerative.carbon.targetSoc")}: <strong>{result.carbon_projection.target_soc_pct}%</strong></span>
                      </div>
                      <div className="h-2.5 bg-nkz-border rounded-full overflow-hidden">
                        <div className="h-full rounded-full transition-all duration-500" style={{
                          background: "linear-gradient(90deg, #28a745, #20c997)",
                          width: `${Math.min(100, ((result.carbon_projection.current_soc_pct ?? 0) / result.carbon_projection.target_soc_pct) * 100)}%`,
                        }} />
                      </div>
                      {result.carbon_projection.projected_soc_pct != null && (
                        <div className="mt-1 text-nkz-xs text-nkz-success">
                          → {t("regenerative.carbon.projectedSoc")}: {result.carbon_projection.projected_soc_pct}%
                          ({result.carbon_projection.soc_delta_pct > 0 ? "+" : ""}{result.carbon_projection.soc_delta_pct}%)
                        </div>
                      )}
                    </div>
                  )}
                </div>
                <div className="flex-1 min-w-[180px]">
                  <div>🌱 CO₂e: <strong>{result.carbon_projection.co2e_sequestered_ton_ha} t/ha</strong></div>
                  <div>🧪 {t("regenerative.carbon.fertilizerSaved")}: <strong>{result.carbon_projection.fertilizer_n_saved_kg_ha} kg/ha</strong></div>
                  <div>💰 {t("regenerative.carbon.savings")}: <strong>{result.carbon_projection.fertilizer_savings_eur_ha} €/ha</strong></div>
                  {result.carbon_projection.years_to_target != null && (
                    <div className="mt-1">⏳ {t("regenerative.carbon.yearsToTarget")}: <strong>{result.carbon_projection.years_to_target}</strong></div>
                  )}
                  <div className="text-nkz-xs text-nkz-text-muted mt-1">{result.carbon_projection.soil_texture}</div>
                </div>
              </div>
            </Card>
          )}

          {result.alternatives.length > 0 && (
            <Stack gap="stack">
              <div className="text-sm font-semibold">🔄 {t("regenerative.alternatives")}</div>
              <div className="flex gap-3 flex-wrap">
                {result.alternatives.map((alt, i) => (
                  <Card key={i} padding="md" className="flex-1 min-w-[180px]">
                    <div className="font-semibold">{alt.cover_crop_common}</div>
                    <div className="text-nkz-xs text-nkz-text-muted">{alt.type} — C/N: {alt.c_n_ratio}</div>
                    <div className="mt-1">Biomass: {alt.biomass_t_ha} t/ha</div>
                    <div>N avail: {alt.n_available_kg_ha} kg/ha</div>
                  </Card>
                ))}
              </div>
            </Stack>
          )}

          <div className="text-nkz-xs text-nkz-text-muted bg-nkz-surface-sunken rounded-nkz-md p-3 max-w-3xl">
            {t("regenerative.provenance")}: {result.provenance.cover_crop_source} | {result.provenance.n_fixation_source} | {result.provenance.yield_source} | {result.provenance.climate_source}
          </div>
        </Stack>
      )}
    </Stack>
  );
}
