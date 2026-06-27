import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Card, EmptyState, Skeleton, Stack, Badge, DetailGrid, DetailItem } from '@nekazari/ui-kit';
import { Microscope, AlertTriangle, Sprout, Calendar, CloudRain, TrendingUp } from 'lucide-react';
import { useParcelContext } from '../context/ParcelContext';
import { usePlanningScenario } from '../context/PlanningScenarioContext';
import { useBioApi } from '../services/api';

interface DailyPoint {
  day: number;
  dvs?: number;
  lai: number;
  tagp: number;
  twso?: number;
  wso?: number;
}

interface WofostResult {
  model: string;
  method: string;
  simulated_yield_kg_ha?: number;
  total_biomass_kg_ha?: number;
  max_lai?: number;
  days_simulated: number;
  daily_output: DailyPoint[];
  parcel_id: string;
  crop_slug: string;
  sowing_date: string;
  soil_inputs: { sand_pct: number; clay_pct: number };
  soil_hydraulic: {
    awc_mm_per_metre: number;
    k_sat_mm_d: number;
    theta_fc: number;
    theta_wp: number;
  };
  weather_days_fetched: number;
}

export default function WofostSimulation() {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
  const { enabled: scenarioEnabled } = usePlanningScenario();
  const api = useBioApi();

  const [cropSlug, setCropSlug] = useState('');
  const [sowingDate, setSowingDate] = useState('');
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<WofostResult | null>(null);
  const [error, setError] = useState('');

  // Guards
  if (parcelLoading) return <Skeleton variant="rect" height="300px" />;
  if (parcelError) {
    return <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={parcelError} />;
  }
  if (!selectedParcel) {
    return (
      <EmptyState
        icon={<Microscope className="w-8 h-8" />}
        title={t('wofostSimulation.selectParcel')}
      />
    );
  }
  if (scenarioEnabled) {
    return (
      <EmptyState
        icon={<AlertTriangle className="w-8 h-8" />}
        title={t('scenarioMode.campaignBlocked')}
        description={t('scenarioMode.campaignBlockedDetail')}
      />
    );
  }

  const handleRun = async () => {
    setRunning(true);
    setError('');
    setResult(null);
    try {
      const data = await api.runWofostSimulation(
        selectedParcel,
        cropSlug || undefined,
        sowingDate || undefined,
      );
      setResult(data);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunning(false);
    }
  };

  // Simple SVG chart for daily output
  const renderChart = (daily: DailyPoint[]) => {
    if (!daily || daily.length === 0) return null;
    const width = 600;
    const height = 240;
    const pad = { top: 20, right: 20, bottom: 30, left: 50 };
    const plotW = width - pad.left - pad.right;
    const plotH = height - pad.top - pad.bottom;

    const xMax = daily[daily.length - 1].day;
    const laiMax = Math.max(...daily.map(d => d.lai || 0), 1);
    const bioMax = Math.max(...daily.map(d => d.tagp || 0), 1);

    const x = (d: DailyPoint) => pad.left + ((d.day / xMax) * plotW);
    const yLai = (d: DailyPoint) => pad.top + plotH - ((d.lai / laiMax) * plotH);
    const yBio = (d: DailyPoint) => pad.top + plotH - ((d.tagp / bioMax) * plotH);

    const laiPoints = daily.map(d => `${x(d)},${yLai(d)}`).join(' ');
    const bioPoints = daily.map(d => `${x(d)},${yBio(d)}`).join(' ');

    // Y-axis ticks
    const yTicks = [0, 0.25, 0.5, 0.75, 1].map(f => pad.top + plotH - (f * plotH));

    return (
      <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-auto" aria-label="LAI and Biomass chart">
        {/* Grid */}
        {yTicks.map((yt, i) => (
          <line key={i} x1={pad.left} y1={yt} x2={width - pad.right} y2={yt}
                stroke="var(--nkz-border, #e5e7eb)" strokeWidth="1" />
        ))}
        {/* Y labels */}
        <text x={pad.left - 8} y={pad.top + plotH / 2} textAnchor="middle"
              transform={`rotate(-90, ${pad.left - 8}, ${pad.top + plotH / 2})`}
              fontSize="10" fill="currentColor" className="text-nkz-text-muted">
          LAI / Biomass (kg/ha)
        </text>
        {/* X label */}
        <text x={pad.left + plotW / 2} y={height - 4} textAnchor="middle"
              fontSize="10" fill="currentColor" className="text-nkz-text-muted">
          {t('wofostSimulation.daysSinceSowing')}
        </text>
        {/* LAI line */}
        <polyline points={laiPoints} fill="none" stroke="#10B981" strokeWidth="2" />
        {/* Biomass line */}
        <polyline points={bioPoints} fill="none" stroke="#6366F1" strokeWidth="2" strokeDasharray="5,3" />
        {/* Legend */}
        <rect x={pad.left + 8} y={pad.top + 4} width="10" height="10" fill="#10B981" rx="2" />
        <text x={pad.left + 22} y={pad.top + 12} fontSize="10" fill="currentColor">LAI</text>
        <rect x={pad.left + 60} y={pad.top + 4} width="10" height="10" fill="#6366F1" rx="2" />
        <text x={pad.left + 74} y={pad.top + 12} fontSize="10" fill="currentColor">
          {t('wofostSimulation.biomass')} (TAGP)
        </text>
      </svg>
    );
  };

  return (
    <Stack gap="section">
      {/* Header */}
      <div>
        <div className="flex items-center gap-2 mb-1">
          <Microscope className="w-5 h-5 text-nkz-accent-base" />
          <h2 className="text-nkz-xl font-bold text-nkz-text-primary">{t('wofostSimulation.title')}</h2>
        </div>
        <p className="text-nkz-base text-nkz-text-muted">{t('wofostSimulation.subtitle')}</p>
      </div>

      {/* Input section */}
      <Card padding="md">
        <div className="flex flex-wrap gap-3 items-end">
          <div className="flex-1 min-w-[180px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
              {t('wofostSimulation.cropSlug')}
            </label>
            <input
              type="text"
              value={cropSlug}
              onChange={e => setCropSlug(e.target.value)}
              placeholder="wheat (auto-detected)"
              className="w-full px-3 py-2 rounded-nkz-md border border-nkz-border bg-transparent
                         text-nkz-sm text-nkz-text-primary placeholder:text-nkz-text-muted
                         focus:outline-none focus:ring-2 focus:ring-nkz-accent-base"
            />
          </div>
          <div className="flex-1 min-w-[180px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
              {t('wofostSimulation.sowingDate')}
            </label>
            <input
              type="date"
              value={sowingDate}
              onChange={e => setSowingDate(e.target.value)}
              className="w-full px-3 py-2 rounded-nkz-md border border-nkz-border bg-transparent
                         text-nkz-sm text-nkz-text-primary
                         focus:outline-none focus:ring-2 focus:ring-nkz-accent-base"
            />
          </div>
          <Button onClick={handleRun} disabled={running} loading={running}>
            <Microscope className="w-4 h-4 mr-1" />
            {running ? t('wofostSimulation.running') : t('wofostSimulation.run')}
          </Button>
        </div>
      </Card>

      {/* Error */}
      {error && (
        <Card padding="md" className="border-nkz-danger bg-nkz-danger-soft">
          <div className="flex items-center gap-2 text-nkz-danger">
            <AlertTriangle className="w-5 h-5" />
            <span>{error}</span>
          </div>
        </Card>
      )}

      {/* Results */}
      {result && (
        <>
          {/* Model badge */}
          <div className="flex flex-wrap items-center gap-2">
            <Badge intent="info">
              <Microscope className="w-3 h-3 mr-1 inline" />
              {result.model || 'WOFOST'}
            </Badge>
            <Badge intent="default">{result.method || 'PCSE'}</Badge>
            <span className="text-nkz-xs text-nkz-text-muted">
              {t('wofostSimulation.crop')}: {result.crop_slug} — {result.sowing_date}
            </span>
          </div>

          {/* Key metrics */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Card padding="md" className="text-center border-nkz-positive bg-nkz-positive-soft">
              <p className="text-nkz-xs text-nkz-text-secondary">{t('wofostSimulation.simulatedYield')}</p>
              <p className="text-nkz-xl font-bold text-nkz-positive">
                {result.simulated_yield_kg_ha?.toLocaleString() ?? '—'} kg/ha
              </p>
            </Card>
            <Card padding="md" className="text-center">
              <p className="text-nkz-xs text-nkz-text-secondary">{t('wofostSimulation.totalBiomass')}</p>
              <p className="text-nkz-xl font-bold text-nkz-text-primary">
                {result.total_biomass_kg_ha?.toLocaleString() ?? '—'} kg/ha
              </p>
            </Card>
            <Card padding="md" className="text-center">
              <p className="text-nkz-xs text-nkz-text-secondary">{t('wofostSimulation.maxLai')}</p>
              <p className="text-nkz-xl font-bold text-nkz-text-primary">
                {result.max_lai?.toFixed(2) ?? '—'}
              </p>
            </Card>
            <Card padding="md" className="text-center">
              <p className="text-nkz-xs text-nkz-text-secondary">{t('wofostSimulation.daysSimulated')}</p>
              <p className="text-nkz-xl font-bold text-nkz-text-primary">
                {result.days_simulated}
              </p>
            </Card>
          </div>

          {/* Soil inputs */}
          <Card padding="md">
            <DetailGrid columns={2}>
              <DetailItem label={t('wofostSimulation.sand')} value={`${result.soil_inputs?.sand_pct ?? '—'}%`} />
              <DetailItem label={t('wofostSimulation.clay')} value={`${result.soil_inputs?.clay_pct ?? '—'}%`} />
              <DetailItem label={t('wofostSimulation.awc')} value={`${result.soil_hydraulic?.awc_mm_per_metre ?? '—'} mm/m`} />
              <DetailItem label={t('wofostSimulation.kSat')} value={`${result.soil_hydraulic?.k_sat_mm_d ?? '—'} mm/d`} />
              <DetailItem label={t('wofostSimulation.thetaFc')} value={result.soil_hydraulic?.theta_fc?.toFixed(3) ?? '—'} />
              <DetailItem label={t('wofostSimulation.thetaWp')} value={result.soil_hydraulic?.theta_wp?.toFixed(3) ?? '—'} />
            </DetailGrid>
          </Card>

          {/* Weather summary */}
          <Card padding="md">
            <div className="flex items-center gap-2">
              <CloudRain className="w-4 h-4 text-nkz-accent-base" />
              <span className="text-nkz-sm text-nkz-text-secondary">
                {t('wofostSimulation.weatherDays')}: {result.weather_days_fetched}
              </span>
            </div>
          </Card>

          {/* Daily output chart */}
          {result.daily_output && result.daily_output.length > 0 && (
            <Card padding="lg">
              <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-3 flex items-center gap-1.5">
                <TrendingUp className="w-4 h-4 text-nkz-accent-base" />
                {t('wofostSimulation.dailyOutput')}
              </h3>
              {renderChart(result.daily_output)}
            </Card>
          )}
        </>
      )}

      {/* Initial state */}
      {!result && !running && !error && (
        <Card padding="lg">
          <p className="text-nkz-sm text-nkz-text-muted">{t('wofostSimulation.intro')}</p>
        </Card>
      )}
    </Stack>
  );
}
