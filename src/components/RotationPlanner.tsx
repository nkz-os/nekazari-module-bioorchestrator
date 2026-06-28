import React, { useState } from "react";
import { useTranslation } from '@nekazari/sdk';
import { useParcelContext } from '../context/ParcelContext';
import { Card, Button, Stack, EmptyState, Skeleton, Slider, ProgressBar } from '@nekazari/ui-kit';
import { AlertTriangle, Activity } from 'lucide-react';

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

interface Props {
  startingCrop?: string;
  management?: string;
}

export default function RotationPlanner({ startingCrop: initialCrop, management: initialMgmt }: Props = {}) {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
  const [years, setYears] = useState(4);
  const [seedPrice, setSeedPrice] = useState(1);
  const [harvestPrice, setHarvestPrice] = useState(1);
  const [operationCost, setOperationCost] = useState(1);
  const [startingCrop, setStartingCrop] = useState(initialCrop || '');
  const [management, setManagement] = useState(initialMgmt || 'any');
  const [result, setResult] = useState<PlanResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handlePlan = async () => {
    if (!selectedParcel) return;
    setLoading(true); setError("");
    const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";
    try {
      const params = new URLSearchParams({
        parcel_id: selectedParcel,
        years: String(years),
        seed_price: String(seedPrice),
        harvest_price: String(harvestPrice),
        operation_cost: String(operationCost),
        management,
      });
      if (startingCrop) params.set('starting_crop', startingCrop);
      const res = await fetch(
        `${API_BASE}/api/graph/agriculture/rotation-plan?${params.toString()}`,
        { credentials: "include" }
      );
      if (!res.ok) throw new Error((await res.json()).detail || "Error");
      setResult(await res.json());
    } catch (e: any) { setError(e.message); }
    finally { setLoading(false); }
  };

  if (parcelLoading) return <Skeleton variant="rect" height="200px" />;
  if (parcelError) return <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={parcelError} />;
  if (!selectedParcel) return <EmptyState icon={<Activity className="w-8 h-8" />} title={t('app.selectParcelPrompt')} />;

  return (
    <Stack gap="section">
      <div>
        <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-1">🔄 {t("rotationPlanner.title")}</h2>
        <p className="text-nkz-text-muted text-sm">{t("rotationPlanner.subtitle")}</p>
      </div>

      <div>
        <Slider
          value={years}
          onChange={setYears}
          min={2}
          max={6}
          label={t("rotationPlanner.years")}
          unit={String(years)}
        />
      </div>

      <Card padding="md">
        <strong className="text-nkz-sm">{t("rotationPlanner.economicInputs")}</strong>{" "}
        <span className="text-nkz-xs text-nkz-text-muted">({t("comparator.optional")})</span>
        <div style={{ display: "flex", gap: 12, marginTop: 8, flexWrap: "wrap" }}>
          <div>
            <label className="text-nkz-xs">{t("comparator.seedPrice")}</label><br />
            <input type="number" value={seedPrice} onChange={e => setSeedPrice(Number(e.target.value))} className="w-24 px-2 py-1 border border-nkz-border rounded-nkz-sm text-nkz-sm" />
          </div>
          <div>
            <label className="text-nkz-xs">{t("comparator.harvestPrice")}</label><br />
            <input type="number" value={harvestPrice} onChange={e => setHarvestPrice(Number(e.target.value))} className="w-24 px-2 py-1 border border-nkz-border rounded-nkz-sm text-nkz-sm" />
          </div>
          <div>
            <label className="text-nkz-xs">{t("comparator.operationCost")}</label><br />
            <input type="number" value={operationCost} onChange={e => setOperationCost(Number(e.target.value))} className="w-24 px-2 py-1 border border-nkz-border rounded-nkz-sm text-nkz-sm" />
          </div>
        </div>
      </Card>

      <Button
        onClick={handlePlan}
        disabled={loading || !selectedParcel}
        variant="primary"
        loading={loading}
      >
        {t("rotationPlanner.calculate")}
      </Button>

      {error && <div className="text-nkz-negative text-nkz-sm">{error}</div>}

      {result && (
        <>
          {/* Timeline */}
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            {result.plan.map((entry, i) => (
              <div
                key={i}
                className={`flex-1 p-nkz-stack rounded-nkz-md border min-w-[160px] ${
                  entry.rotation_warning
                    ? 'bg-nkz-warning-soft border-nkz-warning'
                    : 'bg-nkz-info-soft border-nkz-border'
                }`}
                style={{ flex: "1 1 180px" }}
              >
                <div className="text-nkz-xs text-nkz-text-muted">{t("rotationPlanner.year")} {entry.year}</div>
                <div className="font-semibold text-nkz-base">{entry.crop}</div>
                <div className="text-nkz-xs">{entry.variety || "—"}</div>
                <div className="mt-2 text-nkz-xs">
                  {entry.expected_yield_kg_ha.toLocaleString()} kg/ha<br />
                  🌱 {entry.carbon_fixed_tco2e} tCO₂e<br />
                  🧪 N: {entry.n_balance_kg_ha > 0 ? "+" : ""}{entry.n_balance_kg_ha} kg/ha
                  {entry.n_fixation_kg_ha > 0 && <span> (fixes {entry.n_fixation_kg_ha})</span>}
                </div>
                {entry.rotation_warning && (
                  <div className="mt-1 text-nkz-xs text-nkz-warning">⚠️ {entry.rotation_warning}</div>
                )}
                {entry.pest_risk && entry.pest_risk.risk_level && entry.pest_risk.risk_level !== "none" && entry.pest_risk.risk_level !== "unknown" && (
                  <div className={`mt-1 text-nkz-xs ${entry.pest_risk.risk_level === "high" ? "text-nkz-negative" : "text-nkz-warning"}`}>
                    🐛 {entry.pest_risk.shared_pests?.slice(0, 3).join(", ")}
                    {(entry.pest_risk.shared_count || 0) > 3 && ` +${(entry.pest_risk.shared_count || 0) - 3} more`}
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Cumulative */}
          <Card padding="md" className="bg-nkz-positive-soft">
            <strong className="text-nkz-sm">{t("rotationPlanner.cumulative")} ({years} {t("rotationPlanner.yearsLabel")})</strong>
            <div style={{ display: "flex", gap: 24, marginTop: 8, flexWrap: "wrap" }} className="text-nkz-sm">
              <div>🌾 {result.cumulative.total_yield_kg_ha.toLocaleString()} kg/ha</div>
              <div>🌍 {result.cumulative.total_carbon_fixed_tco2e} tCO₂e</div>
              <div>💰 {result.cumulative.total_net_margin_eur_ha.toLocaleString()} €/ha</div>
              <div>🧪 N pool: {result.cumulative.final_soil_n_pool_kg_ha} kg/ha</div>
            </div>
          </Card>

          {/* PAC Compliance */}
          {result.pac_compliance && (
            <div>
              <strong className="text-nkz-base">🇪🇺 {t("rotationPlanner.pac.title")}</strong>
              <div className="flex items-center gap-4 mt-2 mb-nkz-stack">
                <div
                  className={`w-[72px] h-[72px] rounded-full flex items-center justify-center text-nkz-xl font-bold border-4 ${
                    result.pac_compliance.score >= 80
                      ? 'bg-nkz-positive-soft border-nkz-positive text-nkz-positive'
                      : result.pac_compliance.score >= 50
                      ? 'bg-nkz-warning-soft border-nkz-warning text-nkz-warning'
                      : 'bg-nkz-negative-soft border-nkz-negative text-nkz-negative'
                  }`}
                >
                  {result.pac_compliance.score}%
                </div>
                <div className="flex-1">
                  <ProgressBar
                    value={result.pac_compliance.score}
                    intent={result.pac_compliance.score >= 80 ? 'positive' : result.pac_compliance.score >= 50 ? 'warning' : 'negative'}
                    showLabel
                  />
                  <div className="text-nkz-xs text-nkz-text-muted mt-1">
                    {t("rotationPlanner.pac.score")}: {result.pac_compliance.score}/{result.pac_compliance.max_score}
                  </div>
                </div>
              </div>
              <Stack gap="inline">
                {result.pac_compliance.rules.map((rule) => (
                  <div
                    key={rule.id}
                    className={`px-nkz-stack py-2 rounded-nkz-md text-nkz-xs border ${
                      rule.pass === true
                        ? 'bg-nkz-positive-soft border-nkz-positive'
                        : rule.pass === false
                        ? 'bg-nkz-negative-soft border-nkz-negative'
                        : 'bg-nkz-surface border-nkz-border'
                    }`}
                  >
                    <span className="mr-2">
                      {rule.pass === true ? "✅" : rule.pass === false ? "❌" : "⊘"}
                    </span>
                    <strong>{t(`rotationPlanner.pac.rule.${rule.id}`)}</strong>
                    <span className="ml-2 text-nkz-text-muted">{rule.detail}</span>
                  </div>
                ))}
              </Stack>
              <div className="text-nkz-xs text-nkz-text-muted mt-2 italic">
                {result.pac_compliance.disclaimer}
              </div>
            </div>
          )}
        </>
      )}
    </Stack>
  );
}
