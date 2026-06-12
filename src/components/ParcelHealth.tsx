import React, { useState, useEffect } from "react";
import { useTranslation } from "@nekazari/sdk";
import { useParcelContext } from "../context/ParcelContext";
import {
  Panel, MetricCard, MetricGrid, ProgressBar,
  Stack, Badge, Card, EmptyState, Skeleton, DetailGrid, DetailItem,
} from "@nekazari/ui-kit";
import {
  Heart, Droplets, Thermometer, Activity, AlertTriangle, Sprout, Monitor,
} from "lucide-react";
import {
  getCropContext, getYieldPotential,
  CropContextResponse, YieldPotentialResponse,
  fetchAssessmentHistory, fetchAlerts,
  HistoryPoint, AlertItem,
} from "../services/api";
import ParcelHealthChart from "./ParcelHealthChart";

interface AssessmentData {
  cwsiValue?: number;
  mdsValue?: number;
  mdsSeverity?: string;
  waterBalanceDeficit?: number;
  thermalCondition?: string;
  thermalSeverity?: string;
  vigorIndex?: number;
  vigorCondition?: string;
  compositeStressIndex?: number;
  dominantStressor?: string;
  overallSeverity?: string;
  recommendedAction?: string;
  yieldUtilizationPct?: number;
  dataFidelity?: string;
  assessedAt?: string;
}

export default function ParcelHealth() {
  const { t } = useTranslation("bioorchestrator");
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();

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

  // Guard: still loading parcels
  if (parcelLoading) return <Skeleton variant="rect" height="300px" />;

  // Guard: parcel loading error
  if (parcelError) return <EmptyState icon={<AlertTriangle className="w-6 h-6" />} title={parcelError} />;

  // Guard: no parcel selected
  if (!selectedParcel) return <EmptyState icon={<Heart className="w-6 h-6" />} title={t("parcelHealth.selectPrompt")} />;

  const severityIntent = (s?: string): "negative" | "warning" | "positive" | "info" | "default" =>
    s === "CRITICAL" ? "negative" : s === "HIGH" ? "warning" : s === "MEDIUM" ? "info" : "positive";

  return (
    <Stack gap="section">
      <div>
        <div className="flex items-center gap-2 mb-1">
          <Heart className="w-5 h-5 text-nkz-accent-base" />
          <h2 className="text-nkz-xl font-bold text-nkz-text-primary">{t("parcelHealth.title")}</h2>
        </div>
        <p className="text-nkz-base text-nkz-text-muted">{t("app.cards.parcelStatus.subtitle")}</p>
      </div>

      {loading && <Skeleton variant="rect" height="200px" />}

      {error === "noCropAssigned" && (
        <EmptyState
          icon={<Sprout className="w-6 h-6" />}
          title={t("parcelHealth.noCrop")}
          description={t("parcelHealth.goToVarietyFinder")}
        />
      )}

      {error && error !== "noCropAssigned" && (
        <EmptyState
          icon={<AlertTriangle className="w-6 h-6" />}
          title={t("parcelHealth.error")}
          description={error}
        />
      )}

      {ctx?.crop && (
        <>
          {/* Alert banner */}
          {alerts.length > 0 && (
            <Card padding="md" className="border-nkz-danger bg-nkz-danger-soft">
              <div className="flex items-center gap-2 mb-2">
                <AlertTriangle className="w-4 h-4 text-nkz-danger" />
                <strong className="text-nkz-sm">{t("parcelHealth.activeAlerts")}:</strong>
              </div>
              <div className="flex flex-wrap gap-1">
                {alerts.map((a, i) => (
                  <Badge key={i} intent="negative">{a.severity} — {a.recommended_action}</Badge>
                ))}
              </div>
            </Card>
          )}

          {/* Crop info */}
          <Card padding="md">
            <DetailGrid columns={3}>
              <DetailItem label={t("parcelHealth.crop")} value={`${ctx.crop.name} (${ctx.crop.eppo})`} />
              {ctx.variety && <DetailItem label={t("parcelHealth.variety")} value={ctx.variety.name} />}
              <DetailItem
                label={t("parcelHealth.source")}
                value={ctx.phenology_source?.startsWith("bioorchestrator") ? t("parcelHealth.calibrated") : t("parcelHealth.default")}
              />
            </DetailGrid>
          </Card>

          {/* Sensor data */}
          {assessment ? (
            <>
              <div className="flex items-center gap-2 text-nkz-sm text-nkz-text-muted">
                <Monitor className="w-4 h-4" />
                {t("parcelHealth.sensorData")} ({assessment.assessedAt?.slice(0, 16) || "?"})
              </div>

              <MetricGrid columns={3}>
                <MetricCard
                  label="CWSI"
                  value={assessment.cwsiValue?.toFixed(2) ?? "—"}
                  trend={assessment.cwsiValue !== undefined ? {
                    direction: assessment.cwsiValue < 0.3 ? "up" : assessment.cwsiValue < 0.5 ? "neutral" : "down",
                    value: assessment.cwsiValue < 0.3 ? "normal" : assessment.cwsiValue < 0.5 ? "mild" : "stress",
                  } : undefined}
                />
                <MetricCard
                  label="MDS"
                  value={assessment.mdsValue ? `${assessment.mdsValue}µm` : "—"}
                />
                <MetricCard
                  label="Water Bal"
                  value={assessment.waterBalanceDeficit !== undefined ? `${assessment.waterBalanceDeficit}mm` : "—"}
                  unit={assessment.waterBalanceDeficit !== undefined && assessment.waterBalanceDeficit < -5 ? "deficit" : "ok"}
                />
                <MetricCard
                  label="Thermal"
                  value={assessment.thermalCondition || "—"}
                  unit={assessment.thermalSeverity || ""}
                />
                <MetricCard
                  label="Vigor"
                  value={assessment.vigorIndex?.toFixed(2) ?? "—"}
                  unit={assessment.vigorCondition || ""}
                />
                <MetricCard
                  label="Yield Util"
                  value={assessment.yieldUtilizationPct !== undefined ? `${assessment.yieldUtilizationPct}%` : "—"}
                />
              </MetricGrid>

              <Panel>
                <Panel.Header>
                  <Panel.Title icon={<Activity className="w-4 h-4 text-nkz-accent-base" />}>
                    Composite Stress
                  </Panel.Title>
                </Panel.Header>
                <Panel.Body>
                  <ProgressBar
                    value={Math.round((assessment.compositeStressIndex ?? 0) * 100)}
                    intent={assessment.overallSeverity === "CRITICAL" ? "negative" : assessment.overallSeverity === "HIGH" ? "warning" : "positive"}
                    showLabel
                  />
                  <DetailGrid columns={2} className="mt-4">
                    <DetailItem label="Dominant Stressor" value={assessment.dominantStressor || "None"} />
                    <DetailItem label="Recommended Action" value={assessment.recommendedAction || "Continue monitoring"} />
                    <DetailItem label="Severity" value={assessment.overallSeverity || "—"} />
                    <DetailItem label="Fidelity" value={assessment.dataFidelity || "unknown"} />
                  </DetailGrid>
                </Panel.Body>
              </Panel>
            </>
          ) : (
            <EmptyState
              icon={<Activity className="w-6 h-6" />}
              title={t("parcelHealth.noSensorData")}
            />
          )}

          {/* Phenology */}
          {ctx.phenology && (
            <Card padding="md">
              <DetailGrid columns={3}>
                <DetailItem label={t("parcelHealth.phenology")} value={String(ctx.phenology.stage || "")} />
                <DetailItem label="Kc" value={String(ctx.phenology.kc || "")} />
                <DetailItem label="Ky" value={String(ctx.phenology.ky || "")} />
              </DetailGrid>
            </Card>
          )}

          {/* Yield gap */}
          {yp?.yield_gap_pct !== undefined && (
            <Card padding="md" className={(yp.yield_gap_pct ?? 0) > 10 ? "border-nkz-warning bg-nkz-warning-soft" : "border-nkz-positive bg-nkz-positive-soft"}>
              <DetailGrid columns={2}>
                <DetailItem label={t("parcelHealth.yieldGap")} value={`${yp.yield_gap_pct}% (${yp.yield_gap_kg_ha} kg/ha)`} />
                <DetailItem label={t("parcelHealth.expected")} value={`${yp.expected_yield_kg_ha} kg/ha`} />
              </DetailGrid>
            </Card>
          )}

          {/* Soil suitability */}
          {ctx.soil?.suitability && (
            <Card padding="md" className={ctx.soil.suitability.overall === "suitable" ? "border-nkz-positive bg-nkz-positive-soft" : "border-nkz-warning bg-nkz-warning-soft"}>
              <DetailGrid columns={3}>
                <DetailItem
                  label={t("parcelHealth.soilSuitability")}
                  value={`pH ${ctx.soil.suitability.ph_match ? "✅" : "❌"} — Texture ${ctx.soil.suitability.texture_match ? "✅" : "❌"}`}
                />
                {(ctx.soil.actual as any)?.awc_mm && (
                  <DetailItem label="AWC" value={`${(ctx.soil.actual as any).awc_mm}mm`} />
                )}
                {ctx.soil.suitability.overall != null && (
                  <DetailItem label="Overall" value={String(ctx.soil.suitability.overall)} />
                )}
              </DetailGrid>
            </Card>
          )}

          {/* Soil sensors */}
          {ctx.soil_sensors && (ctx.soil_sensors as any).available && (
            <Card padding="md">
              <div className="flex items-center gap-2 mb-2">
                <Thermometer className="w-4 h-4 text-nkz-accent-base" />
                <strong className="text-nkz-sm">{t("parcelHealth.soilSensors")}:</strong>
              </div>
              <DetailGrid columns={2}>
                <DetailItem label="pH" value={(ctx.soil_sensors as any).ph} />
                <DetailItem label="Moisture" value={`${(ctx.soil_sensors as any).moisture_pct}%`} />
              </DetailGrid>
            </Card>
          )}

          {/* Historical chart */}
          <ParcelHealthChart data={history} />

          <div className="text-nkz-xs text-nkz-text-muted mt-4">
            {t("parcelHealth.phenologySource")}: {ctx.phenology_source} — {t("parcelHealth.matchLevel")}: {ctx.match_level}
          </div>
        </>
      )}
    </Stack>
  );
}
