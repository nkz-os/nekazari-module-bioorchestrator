import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Card, EmptyState, Skeleton, Stack, Badge, Select } from '@nekazari/ui-kit';
import { Microscope, AlertTriangle, TrendingUp, CloudRain, Layers } from 'lucide-react';
import { useParcelContext } from '../context/ParcelContext';
import { usePlanningScenario } from '../context/PlanningScenarioContext';
import { useBioApi, ApiError } from '../services/api';
import {
  ENGINES, chartLine, depthLabel, errorHintKey, formatYield, pct, segmentLabel, sowingSourceKey,
  weatherSourceKey, type CropSimulationResult, type Engine, type Irrigation,
} from './cropSimulationModel';

type T = (key: string, opts?: Record<string, unknown>) => string;

const CANOPY_COLOR = '#10B981';
const STRESS_COLOR = '#6366F1';

function SimChart({ daily, t }: { daily: CropSimulationResult['daily']; t: T }) {
  if (daily.length === 0) return null;
  const width = 600;
  const height = 240;
  const pad = { top: 20, right: 20, bottom: 30, left: 50 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;
  const canopy = chartLine(daily, 'canopy_cover');
  const stress = chartLine(daily, 'water_stress');
  const xMax = Math.max(1, ...[...canopy.projected, ...canopy.observed, ...stress.observed, ...stress.projected].map(p => p[0]));
  const px = (p: [number, number]) =>
    `${pad.left + (p[0] / xMax) * plotW},${pad.top + plotH - Math.min(1, Math.max(0, p[1])) * plotH}`;
  const line = (pts: [number, number][], color: string, projected: boolean) =>
    pts.length > 1 ? (
      <polyline
        points={pts.map(px).join(' ')} fill="none" stroke={color} strokeWidth="2"
        strokeDasharray={projected ? '5,3' : undefined} opacity={projected ? 0.5 : 1}
      />
    ) : null;
  const yTicks = [0, 0.25, 0.5, 0.75, 1];
  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-auto" role="img" aria-label={t('cropSimulation.chartLabel')}>
      {yTicks.map(f => (
        <g key={f}>
          <line x1={pad.left} y1={pad.top + plotH - f * plotH} x2={width - pad.right} y2={pad.top + plotH - f * plotH}
                stroke="var(--nkz-border, #e5e7eb)" strokeWidth="1" />
          <text x={pad.left - 6} y={pad.top + plotH - f * plotH + 3} textAnchor="end" fontSize="10" fill="currentColor"
                className="text-nkz-text-muted">{Math.round(f * 100)}%</text>
        </g>
      ))}
      <text x={pad.left + plotW / 2} y={height - 4} textAnchor="middle" fontSize="10" fill="currentColor"
            className="text-nkz-text-muted">{t('cropSimulation.daysSinceSowing')}</text>
      {line(canopy.observed, CANOPY_COLOR, false)}
      {line(canopy.projected, CANOPY_COLOR, true)}
      {line(stress.observed, STRESS_COLOR, false)}
      {line(stress.projected, STRESS_COLOR, true)}
      <rect x={pad.left + 8} y={pad.top + 4} width="10" height="10" fill={CANOPY_COLOR} rx="2" />
      <text x={pad.left + 22} y={pad.top + 12} fontSize="10" fill="currentColor">{t('cropSimulation.canopyCover')}</text>
      <rect x={pad.left + 120} y={pad.top + 4} width="10" height="10" fill={STRESS_COLOR} rx="2" />
      <text x={pad.left + 134} y={pad.top + 12} fontSize="10" fill="currentColor">{t('cropSimulation.waterStress')}</text>
    </svg>
  );
}

function Metric({ label, main, range, unit, prominent }: {
  label: string; main: string; range?: string | null; unit?: string; prominent?: boolean;
}) {
  return (
    <Card padding="md" className={prominent ? 'text-center border-nkz-positive bg-nkz-positive-soft' : 'text-center'}>
      <p className="text-nkz-xs text-nkz-text-secondary">{label}</p>
      <p className={`text-nkz-xl font-bold ${prominent ? 'text-nkz-positive' : 'text-nkz-text-primary'}`}>
        {main}{unit ? ` ${unit}` : ''}
      </p>
      {range && <p className="text-nkz-xs text-nkz-text-muted">{range} {unit}</p>}
    </Card>
  );
}

/** Pure presentation of a simulation answer (no data fetching). */
export function CropSimulationResultView({ result }: { result: CropSimulationResult }) {
  const { t: tr } = useTranslation('bioorchestrator');
  const t = tr as T;
  const inSeason = result.status === 'in_season';
  const y = formatYield(result.yield_t_ha);
  const p = formatYield(result.potential_yield_t_ha);
  const iw = result.initial_water;
  const srcLabel = (s: string) => { const k = weatherSourceKey(s); return k ? t(k) : s; };
  const sowKey = sowingSourceKey(result.inputs.sowing.source);

  return (
    <>
      <div className="flex flex-wrap items-center gap-2">
        <Badge intent="info">
          <Microscope className="w-3 h-3 mr-1 inline" />
          {result.engine} {result.engine_version}
        </Badge>
        <Badge intent={inSeason ? 'warning' : 'positive'}>
          {t(inSeason ? 'cropSimulation.status.in_season' : 'cropSimulation.status.complete')}
        </Badge>
        <span className="text-nkz-xs text-nkz-text-muted">
          {t('cropSimulation.crop')}: {result.crop_slug} → {result.aquacrop_crop} — {result.sowing_date}
        </span>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Metric label={t('cropSimulation.yield')} main={y.main} range={y.range} unit="t/ha" prominent />
        <Metric label={t('cropSimulation.potentialYield')} main={p.main} range={p.range} unit="t/ha" />
        <Metric label={t('cropSimulation.waterGap')}
                main={result.water_gap_pct == null ? '—' : `${result.water_gap_pct.toFixed(0)}%`} />
        <Metric label={t(inSeason ? 'cropSimulation.harvestMedian' : 'cropSimulation.harvestDate')} main={result.harvest_date} />
      </div>

      {inSeason && result.ensemble && (
        <p className="text-nkz-sm text-nkz-text-secondary">
          {t('cropSimulation.ensembleBasis', { n: result.ensemble.n_years, years: result.ensemble.years.join(', ') })}
        </p>
      )}

      <Card padding="md">
        <p className="text-nkz-sm text-nkz-text-secondary">
          {iw.method === 'spinup'
            ? t('cropSimulation.initialWater.spinup', { days: iw.spinup_days, start: iw.start ?? '' })
            : t('cropSimulation.initialWater.assumed_fc')}
        </p>
      </Card>

      {result.daily.length > 0 && (
        <Card padding="lg">
          <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-3 flex items-center gap-1.5">
            <TrendingUp className="w-4 h-4 text-nkz-accent-base" />
            {t('cropSimulation.dailyOutput')}
          </h3>
          <SimChart daily={result.daily} t={t} />
          {inSeason && <p className="text-nkz-xs text-nkz-text-muted mt-2">{t('cropSimulation.projectedNote')}</p>}
        </Card>
      )}

      <Card padding="md">
        <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-2 flex items-center gap-1.5">
          <CloudRain className="w-4 h-4 text-nkz-accent-base" />
          {t('cropSimulation.inputs.weather')}
        </h3>
        <ul className="text-nkz-sm text-nkz-text-secondary">
          {result.inputs.weather.segments.map((s, i) => (
            <li key={i}>{segmentLabel(s, srcLabel(s.source))}</li>
          ))}
        </ul>
        <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mt-4 mb-2 flex items-center gap-1.5">
          <Layers className="w-4 h-4 text-nkz-accent-base" />
          {t('cropSimulation.inputs.soil')}
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full text-nkz-sm">
            <thead>
              <tr className="text-left text-nkz-text-muted border-b border-nkz-border">
                <th className="px-2 py-1">{t('cropSimulation.soil.depth')}</th>
                <th className="px-2 py-1 text-right">{t('cropSimulation.soil.wp')}</th>
                <th className="px-2 py-1 text-right">{t('cropSimulation.soil.fc')}</th>
                <th className="px-2 py-1 text-right">{t('cropSimulation.soil.sat')}</th>
                <th className="px-2 py-1 text-right">{t('cropSimulation.soil.ksat')}</th>
              </tr>
            </thead>
            <tbody>
              {result.inputs.soil.layers.map((l, i) => (
                <tr key={i} className="border-b border-nkz-border text-nkz-text-primary">
                  <td className="px-2 py-1">{depthLabel(l)}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{pct(l.wp, 1)}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{pct(l.fc, 1)}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{pct(l.sat, 1)}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{l.ksat_mm_day.toFixed(0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="text-nkz-sm text-nkz-text-secondary mt-4">
          {t('cropSimulation.inputs.sowing')}: {result.sowing_date} ({sowKey ? t(sowKey) : result.inputs.sowing.source})
        </p>
      </Card>

      {result.warnings.length > 0 && (
        <Card padding="md" className="border-nkz-warning bg-nkz-warning-soft">
          <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-2 flex items-center gap-1.5">
            <AlertTriangle className="w-4 h-4 text-nkz-warning" />
            {t('cropSimulation.warnings')}
          </h3>
          <ul className="text-nkz-sm text-nkz-text-secondary">
            {result.warnings.map((w, i) => <li key={i}>{w}</li>)}
          </ul>
        </Card>
      )}
    </>
  );
}

interface SimError { message: string; code: string | null }

export default function CropSimulation() {
  const { t: tr } = useTranslation('bioorchestrator');
  const t = tr as T;
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
  const { enabled: scenarioEnabled } = usePlanningScenario();
  const api = useBioApi();

  const [cropSlug, setCropSlug] = useState('');
  const [sowingDate, setSowingDate] = useState('');
  const [irrigation, setIrrigation] = useState<Irrigation>('rainfed');
  const [engine] = useState<Engine>(ENGINES[0]);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<CropSimulationResult | null>(null);
  const [error, setError] = useState<SimError | null>(null);

  if (parcelLoading) return <Skeleton variant="rect" height="300px" />;
  if (parcelError) {
    return <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={parcelError} />;
  }
  if (!selectedParcel) {
    return (
      <EmptyState icon={<Microscope className="w-8 h-8" />} title={t('cropSimulation.selectParcel')} />
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
    setError(null);
    setResult(null);
    try {
      const data = await api.runCropSimulation({
        parcelId: selectedParcel,
        cropSlug: cropSlug.trim() || undefined,
        sowingDate: sowingDate || undefined,
        irrigation,
        engine,
      });
      setResult(data);
    } catch (e: unknown) {
      setError(e instanceof ApiError
        ? { message: e.message, code: e.code }
        : { message: e instanceof Error ? e.message : String(e), code: null });
    } finally {
      setRunning(false);
    }
  };

  return (
    <Stack gap="section">
      <div>
        <div className="flex items-center gap-2 mb-1">
          <Microscope className="w-5 h-5 text-nkz-accent-base" />
          <h2 className="text-nkz-xl font-bold text-nkz-text-primary">{t('cropSimulation.title')}</h2>
        </div>
        <p className="text-nkz-base text-nkz-text-muted">{t('cropSimulation.subtitle')}</p>
      </div>

      <Card padding="md">
        <div className="flex flex-wrap gap-3 items-end">
          <div className="flex-1 min-w-[180px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
              {t('cropSimulation.cropSlug')}
            </label>
            <input
              type="text"
              value={cropSlug}
              onChange={e => setCropSlug(e.target.value)}
              placeholder={t('cropSimulation.cropPlaceholder')}
              className="w-full px-3 py-2 rounded-nkz-md border border-nkz-border bg-transparent
                         text-nkz-sm text-nkz-text-primary placeholder:text-nkz-text-muted
                         focus:outline-none focus:ring-2 focus:ring-nkz-accent-base"
            />
            <p className="text-nkz-xs text-nkz-text-muted mt-1">{t('cropSimulation.cropHint')}</p>
          </div>
          <div className="flex-1 min-w-[180px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
              {t('cropSimulation.sowingDate')}
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
          <div className="flex-1 min-w-[180px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
              {t('cropSimulation.irrigation')}
            </label>
            <Select
              value={irrigation}
              onValueChange={(v: string) => setIrrigation(v === 'full' ? 'full' : 'rainfed')}
              options={[
                { value: 'rainfed', label: t('cropSimulation.irrigationRainfed') },
                { value: 'full', label: t('cropSimulation.irrigationFull') },
              ]}
            />
          </div>
          <Button onClick={handleRun} disabled={running} loading={running}>
            <Microscope className="w-4 h-4 mr-1" />
            {running ? t('cropSimulation.running') : t('cropSimulation.run')}
          </Button>
        </div>
        <div className="flex items-center gap-2 mt-3">
          <span className="text-nkz-xs text-nkz-text-muted">{t('cropSimulation.engine')}:</span>
          <Badge intent="default">{engine}</Badge>
        </div>
      </Card>

      {error && (
        <Card padding="md" className="border-nkz-danger bg-nkz-danger-soft">
          <div className="flex items-start gap-2 text-nkz-danger">
            <AlertTriangle className="w-5 h-5" />
            <div>
              <p>{error.message}</p>
              <p className="text-nkz-sm text-nkz-text-secondary">{t(errorHintKey(error.code))}</p>
            </div>
          </div>
        </Card>
      )}

      {result && <CropSimulationResultView result={result} />}

      {!result && !running && !error && (
        <Card padding="lg">
          <p className="text-nkz-sm text-nkz-text-muted">{t('cropSimulation.intro')}</p>
        </Card>
      )}
    </Stack>
  );
}
