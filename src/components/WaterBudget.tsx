import React, { useState } from "react";
import { useTranslation } from '@nekazari/sdk';
import { useParcelContext } from '../context/ParcelContext';
import { Card, Stack, EmptyState, Skeleton, Spinner, ProgressBar } from '@nekazari/ui-kit';
import { AlertTriangle, Activity } from 'lucide-react';
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
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
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

  function gaugeIntent(value: number, max: number): 'positive' | 'warning' | 'negative' | 'default' {
    const pct = max > 0 ? (value / max) * 100 : 0;
    if (pct > 60) return 'positive';
    if (pct > 30) return 'warning';
    return 'negative';
  }

  if (parcelLoading) return <Skeleton variant="rect" height="200px" />;
  if (parcelError) return <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={parcelError} />;
  if (!selectedParcel) return <EmptyState icon={<Activity className="w-8 h-8" />} title={t('app.selectParcelPrompt')} />;

  return (
    <Stack gap="section">
      <div>
        <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-1">💧 {t("waterBudget.title")}</h2>
        <p className="text-nkz-text-muted text-sm">{t("waterBudget.subtitle")}</p>
      </div>

      {loading && (
        <div className="flex items-center gap-2 text-nkz-text-muted text-nkz-sm p-nkz-stack">
          <Spinner size="md" /> {t("common.loading")}
        </div>
      )}
      {error && (
        <div className="px-nkz-stack py-nkz-inline bg-nkz-negative-soft border border-nkz-negative rounded-nkz-md text-nkz-sm text-nkz-negative max-w-xl">
          {error}
        </div>
      )}

      {budget && (
        <Stack gap="stack">
          <div className="text-nkz-sm text-nkz-text-muted">{budget.week_start} → {budget.week_end}</div>

          {/* Available water gauge */}
          <div>
            <div className="flex justify-between text-nkz-xs mb-1">
              <span>{t("waterBudget.availableWater")}</span>
              <span>{budget.current_moisture_estimate_mm} mm</span>
            </div>
            <ProgressBar
              value={budget.soil_awc_mm > 0 ? Math.min(100, (budget.current_moisture_estimate_mm / budget.soil_awc_mm) * 100) : 0}
              intent={gaugeIntent(budget.current_moisture_estimate_mm, budget.soil_awc_mm)}
              size="sm"
            />
            <div className="text-nkz-xs text-nkz-text-muted mt-0.5">AWC: {budget.soil_awc_mm}mm</div>
          </div>

          {/* ETC gauge */}
          <div>
            <div className="flex justify-between text-nkz-xs mb-1">
              <span>{t("waterBudget.etc")}</span>
              <span>{budget.etc_weekly_mm} mm</span>
            </div>
            <ProgressBar
              value={Math.min(100, (budget.etc_weekly_mm / 50) * 100)}
              intent={budget.etc_weekly_mm > 35 ? 'negative' : budget.etc_weekly_mm > 20 ? 'warning' : 'positive'}
              size="sm"
            />
            <div className="text-nkz-xs text-nkz-text-muted mt-0.5">Kc: {budget.kc} ({budget.kc_stage})</div>
          </div>

          {/* Rainfall gauge */}
          <div>
            <div className="flex justify-between text-nkz-xs mb-1">
              <span>{t("waterBudget.rainfall")}</span>
              <span>{budget.forecast_rainfall_mm} mm</span>
            </div>
            <ProgressBar
              value={Math.min(100, (budget.forecast_rainfall_mm / 50) * 100)}
              intent={budget.forecast_rainfall_mm > 30 ? 'positive' : budget.forecast_rainfall_mm > 10 ? 'warning' : 'negative'}
              size="sm"
            />
            <div className="text-nkz-xs text-nkz-text-muted mt-0.5">{t("waterBudget.forecast")}</div>
          </div>

          {/* Deficit / recommendation */}
          <Card
            padding="md"
            className={
              budget.deficit_mm > 20
                ? 'bg-nkz-negative-soft border-nkz-negative'
                : budget.deficit_mm > 0
                ? 'bg-nkz-warning-soft border-nkz-warning'
                : 'bg-nkz-positive-soft border-nkz-positive'
            }
          >
            <strong>{t("waterBudget.deficit")}: {budget.deficit_mm} mm</strong>
            {budget.irrigation_required_mm > 0 && (
              <div className="mt-2">
                {t("waterBudget.irrigationRequired")}: <strong>{budget.irrigation_required_mm} mm = {budget.irrigation_required_m3_ha} m³/ha</strong>
              </div>
            )}
            <div className="mt-2 text-nkz-sm">{budget.recommendation}</div>
          </Card>

          <div className="text-nkz-xs text-nkz-text-muted">
            {t("waterBudget.confidence")}: {budget.confidence} — {budget.confidence_notes}
          </div>
        </Stack>
      )}
    </Stack>
  );
}
