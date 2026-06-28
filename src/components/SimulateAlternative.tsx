import React, { useState, useEffect } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Button, Stack, EmptyState, Select, Skeleton } from '@nekazari/ui-kit';
import { TrendingUp, AlertTriangle, RefreshCw } from 'lucide-react';
import { useBioApi, getCropContext } from '../services/api';
import { useParcelContext } from '../context/ParcelContext';

interface SimResult {
  baseline: string;
  scenario: string;
  yield_delta_kg_ha?: number;
  fertilizer_delta?: { element: string; baseline_kg_ha: number; scenario_kg_ha: number; delta_kg_ha: number }[];
  rotation_violations?: string[];
  soil_ok?: boolean;
  issues?: string[];
  baseline_data_gaps?: string[];
  scenario_data_gaps?: string[];
  error?: string;
}

const SimulateAlternative: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();

  const [scenarioCrop, setScenarioCrop] = useState('');
  const [baselineCrop, setBaselineCrop] = useState('');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<SimResult | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!selectedParcel) return;
    getCropContext(selectedParcel)
      .then(ctx => {
        if (ctx?.crop?.eppo && ctx.crop.eppo !== 'unknown') {
          setBaselineCrop(ctx.crop.eppo);
        }
      })
      .catch(() => {});
  }, [selectedParcel]);

  const handleSimulate = async () => {
    if (!baselineCrop || !scenarioCrop) return;
    setLoading(true);
    setError('');
    try {
      const data = await api.simulateCrop?.(baselineCrop, scenarioCrop);
      setResult(data);
    } catch (e: any) {
      setError(e.message || 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  if (parcelLoading) return <Card padding="md"><Skeleton variant="rect" height="100px" /></Card>;
  if (parcelError) return <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={parcelError} />;
  if (!selectedParcel) return <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={t('varietyFinder.selectParcel', { defaultValue: 'Select parcel' })} />;

  return (
    <Stack gap="section">
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary flex items-center gap-2">
        <RefreshCw className="w-5 h-5 text-nkz-accent-base" />
        {t('simulateAlternative.title', { defaultValue: 'Simular cultivo alternativo' })}
      </h2>
      <p className="text-nkz-sm text-nkz-text-muted">
        {t('simulateAlternative.desc', { defaultValue: 'Compara qué habría pasado si hubieras plantado otro cultivo.' })}
      </p>

      <Card padding="md">
        <div className="flex flex-wrap gap-3 items-end">
          <div className="flex-1 min-w-[180px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
              {t('simulateAlternative.baselineCrop', { defaultValue: 'Cultivo actual' })}
            </label>
            <input
              type="text"
              value={baselineCrop}
              onChange={e => setBaselineCrop(e.target.value.toUpperCase())}
              className="w-full rounded-md border border-nkz-border bg-nkz-surface px-2 py-1.5 text-nkz-sm"
              placeholder="TRZAX"
            />
          </div>
          <div className="flex-1 min-w-[180px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
              {t('simulateAlternative.scenarioCrop', { defaultValue: 'Cultivo alternativo' })}
            </label>
            <input
              type="text"
              value={scenarioCrop}
              onChange={e => setScenarioCrop(e.target.value.toUpperCase())}
              className="w-full rounded-md border border-nkz-border bg-nkz-surface px-2 py-1.5 text-nkz-sm"
              placeholder="ZEAMX"
            />
          </div>
          <Button onClick={handleSimulate} disabled={!baselineCrop || !scenarioCrop || loading} loading={loading}>
            <RefreshCw className="w-4 h-4 mr-1" />
            {t('simulateAlternative.compare', { defaultValue: 'Comparar' })}
          </Button>
        </div>
      </Card>

      {loading && <Card padding="md"><Skeleton variant="rect" height="80px" /></Card>}

      {error && (
        <Card padding="md">
          <div className="flex items-center gap-2 text-nkz-error">
            <AlertTriangle className="w-5 h-5" /><span>{error}</span>
          </div>
        </Card>
      )}

      {result && !result.error && (
        <Card padding="md">
          <h3 className="text-nkz-sm font-semibold mb-2 flex items-center gap-1">
            <TrendingUp className="w-4 h-4 text-nkz-accent-base" />
            {result.baseline} → {result.scenario}
          </h3>
          <div className="space-y-2 text-nkz-sm">
            {result.yield_delta_kg_ha != null && (
              <div className="flex items-center gap-2">
                <span className="text-nkz-text-secondary">
                  {t('simulateAlternative.yieldDelta', { defaultValue: 'Delta rendimiento' })}:
                </span>
                <Badge intent={result.yield_delta_kg_ha > 0 ? 'positive' : 'negative'}>
                  {result.yield_delta_kg_ha > 0 ? '+' : ''}{result.yield_delta_kg_ha?.toLocaleString()} kg/ha
                </Badge>
              </div>
            )}
            {result.fertilizer_delta && result.fertilizer_delta.length > 0 && (
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-nkz-text-secondary">
                  {t('simulateAlternative.fertilizerDelta', { defaultValue: 'Delta fertilizante' })}:
                </span>
                {result.fertilizer_delta.map(f => (
                  <Badge key={f.element} intent={f.delta_kg_ha > 0 ? 'warning' : 'positive'}>
                    {f.element}: {f.delta_kg_ha > 0 ? '+' : ''}{f.delta_kg_ha} kg/ha
                  </Badge>
                ))}
              </div>
            )}
            {result.soil_ok != null && (
              <div className="flex items-center gap-2">
                <Badge intent={result.soil_ok ? 'positive' : 'negative'}>
                  {result.soil_ok
                    ? t('simulateAlternative.soilCompatible', { defaultValue: 'Compatible' })
                    : t('simulateAlternative.soilNotCompatible', { defaultValue: 'No compatible' })}
                </Badge>
              </div>
            )}
            {result.rotation_violations && result.rotation_violations.length > 0 && (
              <div className="text-nkz-text-warning flex items-center gap-1">
                <AlertTriangle className="w-3 h-3" />
                {t('simulateAlternative.rotationViolation', { defaultValue: 'Restricción de rotación' })}: {result.rotation_violations.join(', ')}
              </div>
            )}
            {result.scenario_data_gaps && result.scenario_data_gaps.length > 0 && (
              <div className="text-nkz-text-muted text-nkz-xs">
                {t('simulateAlternative.dataGaps', { defaultValue: 'Datos faltantes' })}: {result.scenario_data_gaps.join(', ')}
              </div>
            )}
          </div>
        </Card>
      )}
    </Stack>
  );
};

export default SimulateAlternative;
