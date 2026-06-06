import React, { useEffect, useState } from 'react';
import { Card, Badge, Button, Skeleton, DetailGrid, DetailItem } from '@nekazari/ui-kit';
import { useTranslation } from '@nekazari/sdk';
import { Search, Sprout, MapPin, Thermometer, Globe, TrendingUp, AlertTriangle } from 'lucide-react';
import { useBioApi } from '../services/api';

type ClimateClass = 'Csa' | 'BSk' | 'Cfb' | 'BSh' | 'Dfa' | 'Dfb';

const CLIMATES: { value: ClimateClass; label: string; desc: string }[] = [
  { value: 'Csa', label: 'Csa', desc: 'Hot-summer Mediterranean' },
  { value: 'BSk', label: 'BSk', desc: 'Cold semi-arid' },
  { value: 'Cfb', label: 'Cfb', desc: 'Oceanic' },
  { value: 'BSh', label: 'BSh', desc: 'Hot semi-arid' },
  { value: 'Dfa', label: 'Dfa', desc: 'Hot-summer continental' },
  { value: 'Dfb', label: 'Dfb', desc: 'Warm-summer continental' },
];

interface CropOption { eppo_code: string; scientific_name: string; trial_count: number; }
interface VarietyResult {
  variety: string;
  mean_yield_kg_ha: number;
  min_yield_kg_ha: number;
  max_yield_kg_ha: number;
  stddev_yield_kg_ha: number;
  trial_count: number;
  trial_years: number[];
  trial_sites: string[];
}

type ViewState = 'input' | 'loading' | 'results' | 'error';

const VarietyFinder: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();

  const [crop, setCrop] = useState('');
  const [climate, setClimate] = useState<ClimateClass>('Csa');
  const [soil, setSoil] = useState('');
  const [cropOptions, setCropOptions] = useState<CropOption[]>([]);
  const [results, setResults] = useState<VarietyResult[]>([]);
  const [similarSites, setSimilarSites] = useState<string[]>([]);
  const [targetEnv, setTargetEnv] = useState<Record<string, any>>({});
  const [view, setView] = useState<ViewState>('input');
  const [error, setError] = useState('');

  // Load crop list on mount
  useEffect(() => {
    api.getAgricultureCrops?.()
      .then((d: any) => setCropOptions(d?.crops || []))
      .catch(() => {});
  }, []);

  const handleSearch = async () => {
    if (!crop) return;
    setView('loading');
    setError('');
    try {
      const params = new URLSearchParams({ crop, climate_class: climate, top_n: '15' });
      if (soil) params.set('soil_type', soil);
      const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";
      const resp = await fetch(`${API_BASE}/api/graph/agriculture/extrapolate?${params}`, { credentials: 'include' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      setResults(data.ranked_varieties || []);
      setSimilarSites(data.similar_sites || []);
      setTargetEnv(data.target_environment || {});
      setView('results');
    } catch (e: any) {
      setError(e.message || 'Unknown error');
      setView('error');
    }
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-2 mb-2">
        <Search className="w-5 h-5 text-nkz-accent-base" />
        <h2 className="text-nkz-lg font-bold text-nkz-text-primary">{t('varietyFinder.title')}</h2>
      </div>

      {/* Onboarding tooltip */}
      <div className="mb-3 rounded-nkz-md bg-nkz-info-soft border border-nkz-info p-nkz-stack text-nkz-xs text-nkz-text-secondary">
        <strong className="text-nkz-text-primary">💡 {t('varietyFinder.title')}:</strong>{' '}
        {t('onboarding.varietyFinder')}
      </div>

      {/* Input form */}
      <Card padding="md">
        <div className="flex flex-wrap gap-3 items-end">
          {/* Crop selector */}
          <div className="flex-1 min-w-[200px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">{t('varietyFinder.crop')}</label>
            <select
              className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-2 text-nkz-sm"
              value={crop}
              onChange={e => setCrop(e.target.value)}
            >
              <option value="">{t('varietyFinder.selectCrop')}</option>
              {cropOptions.map(c => {
                const name = t(`crops.${c.eppo_code}`, { defaultValue: c.scientific_name === '(unknown)' ? c.eppo_code : c.scientific_name });
                return (
                  <option key={c.eppo_code} value={c.eppo_code}>
                    {name} ({c.trial_count} {t('varietyFinder.trials').toLowerCase()})
                  </option>
                );
              })}
            </select>
          </div>

          {/* Climate selector */}
          <div className="w-[150px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">{t('varietyFinder.climate')}</label>
            <select
              className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-2 text-nkz-sm"
              value={climate}
              onChange={e => setClimate(e.target.value as ClimateClass)}
            >
              {CLIMATES.map(c => (
                <option key={c.value} value={c.value}>{c.label} — {c.desc}</option>
              ))}
            </select>
          </div>

          {/* Soil (optional) */}
          <div className="w-[150px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">{t('varietyFinder.soil')} ({t('varietyFinder.optional')})</label>
            <select
              className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-2 text-nkz-sm"
              value={soil}
              onChange={e => setSoil(e.target.value)}
            >
              <option value="">{t('varietyFinder.anySoil')}</option>
              {['Calcisol','Cambisol','Chernozem','Fluvisol','Gleysol','Leptosol','Luvisol','Phaeozem','Regosol','Vertisol'].map(s => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          </div>

          {/* Search */}
          <Button
            onClick={handleSearch}
            disabled={!crop || view === 'loading'}
            loading={view === 'loading'}
          >
            <Search className="w-4 h-4 mr-1" />
            {t('varietyFinder.search')}
          </Button>
        </div>
      </Card>

      {/* Loading */}
      {view === 'loading' && (
        <Card padding="md">
          <Skeleton variant="rect" height="60px" />
          <div className="mt-2"><Skeleton variant="rect" height="200px" /></div>
        </Card>
      )}

      {/* Error */}
      {view === 'error' && (
        <Card padding="md">
          <div className="flex items-center gap-2 text-nkz-error">
            <AlertTriangle className="w-5 h-5" />
            <span>{error}</span>
          </div>
        </Card>
      )}

      {/* Results */}
      {view === 'results' && (
        <>
          {/* Target environment summary */}
          <Card padding="md">
            <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-2 flex items-center gap-1.5">
              <Globe className="w-4 h-4 text-nkz-accent-base" />
              {t('varietyFinder.targetEnvironment')}
            </h3>
            <DetailGrid columns={2}>
              <DetailItem label={t('varietyFinder.crop')} value={crop} />
              <DetailItem label="Köppen" value={climate} />
              {soil && <DetailItem label={t('varietyFinder.soil')} value={soil} />}
              <DetailItem label={t('varietyFinder.similarSites')} value={`${similarSites.length} sites`} />
            </DetailGrid>
            {similarSites.length > 0 && (
              <div className="mt-1 flex flex-wrap gap-1">
                {similarSites.slice(0, 8).map(s => (
                  <Badge key={s} intent="info">{s}</Badge>
                ))}
                {similarSites.length > 8 && <Badge intent="default">+{similarSites.length - 8}</Badge>}
              </div>
            )}
          </Card>

          {/* Variety table */}
          <Card padding="md">
            <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-3 flex items-center gap-1.5">
              <TrendingUp className="w-4 h-4 text-nkz-accent-base" />
              {t('varietyFinder.rankedVarieties')} ({results.length})
            </h3>

            {results.length === 0 ? (
              <p className="text-nkz-sm text-nkz-text-muted">{t('varietyFinder.noResults')}</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-nkz-sm">
                  <thead>
                    <tr className="border-b border-nkz-border text-nkz-text-secondary">
                      <th className="text-left py-2 pr-3">#</th>
                      <th className="text-left py-2 pr-3">{t('varietyFinder.variety')}</th>
                      <th className="text-right py-2 pr-3">{t('varietyFinder.meanYield')}</th>
                      <th className="text-right py-2 pr-3">Min</th>
                      <th className="text-right py-2 pr-3">Max</th>
                      <th className="text-right py-2 pr-3">±</th>
                      <th className="text-center py-2 pr-3">{t('varietyFinder.trials')}</th>
                      <th className="text-left py-2">{t('varietyFinder.sites')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((v, i) => (
                      <tr key={v.variety} className="border-b border-nkz-border-subtle hover:bg-nkz-surface-sunken">
                        <td className="py-2 pr-3 text-nkz-text-muted">{i + 1}</td>
                        <td className="py-2 pr-3 font-medium text-nkz-text-primary">{v.variety}</td>
                        <td className="py-2 pr-3 text-right font-semibold text-nkz-accent-base">
                          {v.mean_yield_kg_ha?.toLocaleString()}
                        </td>
                        <td className="py-2 pr-3 text-right text-nkz-text-secondary">
                          {v.min_yield_kg_ha?.toLocaleString()}
                        </td>
                        <td className="py-2 pr-3 text-right text-nkz-text-secondary">
                          {v.max_yield_kg_ha?.toLocaleString()}
                        </td>
                        <td className="py-2 pr-3 text-right text-nkz-text-muted">
                          ±{v.stddev_yield_kg_ha?.toLocaleString()}
                        </td>
                        <td className="py-2 pr-3 text-center">
                          <Badge intent={v.trial_count >= 5 ? 'positive' : 'warning'}>
                            {v.trial_count}
                          </Badge>
                        </td>
                        <td className="py-2 text-nkz-text-secondary text-xs max-w-[200px] truncate">
                          {v.trial_sites?.slice(0, 3).join(', ')}
                          {(v.trial_sites?.length || 0) > 3 ? ` +${v.trial_sites.length - 3}` : ''}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        </>
      )}
    </div>
  );
};

export default VarietyFinder;
