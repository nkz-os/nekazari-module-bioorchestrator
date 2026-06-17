import React, { useEffect, useState } from 'react';
import { Card, Badge, Button, Skeleton, DetailGrid, DetailItem, Select, Stack, EmptyState } from '@nekazari/ui-kit';
import { useTranslation } from '@nekazari/sdk';
import { Search, Sprout, MapPin, Thermometer, Globe, TrendingUp, AlertTriangle } from 'lucide-react';
import { useBioApi } from '../services/api';
import { useParcelContext } from '../context/ParcelContext';

interface ClimateOption {
  value: string;
  label: string;
  desc: string;
}

interface SoilOption {
  value: string;
  label: string;
}

const KOPPEN_DESCRIPTIONS: Record<string, string> = {
  Af: 'Tropical rainforest', Am: 'Tropical monsoon', Aw: 'Tropical savanna',
  BWh: 'Hot desert', BWk: 'Cold desert', BSh: 'Hot semi-arid', BSk: 'Cold semi-arid',
  Csa: 'Hot-summer Mediterranean', Csb: 'Warm-summer Mediterranean', Csc: 'Cold-summer Mediterranean',
  Cwa: 'Monsoon-influenced humid subtropical', Cwb: 'Subtropical highland',
  Cfa: 'Humid subtropical', Cfb: 'Oceanic', Cfc: 'Subpolar oceanic',
  Dfa: 'Hot-summer continental', Dfb: 'Warm-summer continental', Dfc: 'Subarctic', Dfd: 'Extreme subarctic',
  ET: 'Tundra', EF: 'Ice cap',
};

const FALLBACK_CLIMATES: ClimateOption[] = [
  { value: 'Csa', label: 'Csa', desc: 'Hot-summer Mediterranean' },
  { value: 'BSk', label: 'BSk', desc: 'Cold semi-arid' },
  { value: 'Cfb', label: 'Cfb', desc: 'Oceanic' },
  { value: 'BSh', label: 'BSh', desc: 'Hot semi-arid' },
  { value: 'Dfa', label: 'Dfa', desc: 'Hot-summer continental' },
  { value: 'Dfb', label: 'Dfb', desc: 'Warm-summer continental' },
];

const FALLBACK_SOILS: SoilOption[] = [
  { value: '', label: 'Any soil' },
  { value: 'Calcisol', label: 'Calcisol' },
  { value: 'Cambisol', label: 'Cambisol' },
  { value: 'Chernozem', label: 'Chernozem' },
  { value: 'Fluvisol', label: 'Fluvisol' },
  { value: 'Gleysol', label: 'Gleysol' },
  { value: 'Leptosol', label: 'Leptosol' },
  { value: 'Luvisol', label: 'Luvisol' },
  { value: 'Phaeozem', label: 'Phaeozem' },
  { value: 'Regosol', label: 'Regosol' },
  { value: 'Vertisol', label: 'Vertisol' },
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
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();

  const [crop, setCrop] = useState('');
  const [climate, setClimate] = useState<string>('Csa');
  const [soil, setSoil] = useState('');
  const [cropOptions, setCropOptions] = useState<CropOption[]>([]);
  const [climates, setClimates] = useState<ClimateOption[]>([]);
  const [soils, setSoils] = useState<SoilOption[]>([]);
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

  // Fetch reference data on mount
  useEffect(() => {
    api.getClimateClasses?.()
      .then((d: any) => {
        if (d?.climate_classes) {
          setClimates(d.climate_classes.map((c: string) => ({
            value: c,
            label: c,
            desc: KOPPEN_DESCRIPTIONS[c] || '',
          })));
        }
      })
      .catch(() => {
        setClimates(FALLBACK_CLIMATES);
      });

    api.getSoilTypes?.()
      .then((d: any) => {
        if (d?.soil_types) {
          setSoils([
            { value: '', label: 'Any soil' },
            ...d.soil_types.map((s: string) => ({ value: s, label: s })),
          ]);
        }
      })
      .catch(() => {
        setSoils(FALLBACK_SOILS);
      });
  }, []);

  const handleSearch = async () => {
    if (!crop) return;
    setView('loading');
    setError('');
    try {
      const data = await api.extrapolateVarieties({
        crop,
        climate_class: climate,
        top_n: '15',
        ...(soil ? { soil_type: soil } : {}),
      });
      setResults(data.ranked_varieties || []);
      setSimilarSites(data.similar_sites || []);
      setTargetEnv(data.target_environment || {});
      setView('results');
    } catch (e: any) {
      setError(e.message || 'Unknown error');
      setView('error');
    }
  };

  if (parcelLoading) {
    return (
      <Card padding="md">
        <Skeleton variant="rect" height="120px" />
      </Card>
    );
  }

  if (parcelError) {
    return (
      <EmptyState
        icon={<AlertTriangle className="w-8 h-8" />}
        title={parcelError}
      />
    );
  }

  if (!selectedParcel) {
    return (
      <EmptyState
        icon={<MapPin className="w-8 h-8" />}
        title={t('varietyFinder.selectParcel')}
        description={t('varietyFinder.selectParcelDesc')}
      />
    );
  }

  return (
    <Stack gap="section">
      {/* Header */}
      <Stack gap="tight">
        <div className="flex items-center gap-2">
          <Search className="w-5 h-5 text-nkz-accent-base" />
          <h2 className="text-nkz-lg font-bold text-nkz-text-primary">{t('varietyFinder.title')}</h2>
        </div>
        <p className="text-nkz-sm text-nkz-text-secondary">{t('onboarding.varietyFinder')}</p>
      </Stack>

      {/* Input form */}
      <Card padding="md">
        <div className="flex flex-wrap gap-3 items-end">
          {/* Crop selector */}
          <div className="flex-1 min-w-[200px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">{t('varietyFinder.crop')}</label>
            <Select
              value={crop}
              onValueChange={setCrop}
              placeholder={t('varietyFinder.selectCrop')}
              options={cropOptions.map(c => {
                const name = t(`crops.${c.eppo_code}`, { defaultValue: c.scientific_name === '(unknown)' ? c.eppo_code : c.scientific_name });
                return { value: c.eppo_code, label: `${name} (${c.trial_count} ${t('varietyFinder.trials').toLowerCase()})` };
              })}
            />
          </div>

          {/* Climate selector */}
          <div className="w-[150px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">{t('varietyFinder.climate')}</label>
            <Select
              value={climate}
              onValueChange={(v) => setClimate(v)}
              options={climates.length > 0 ? climates.map(c => ({ value: c.value, label: `${c.label}${c.desc ? ` — ${c.desc}` : ''}` })) : [{ value: 'Csa', label: 'Csa' }]}
            />
          </div>

          {/* Soil (optional) */}
          <div className="w-[150px]">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">{t('varietyFinder.soil')} ({t('varietyFinder.optional')})</label>
            <Select
              value={soil}
              onValueChange={setSoil}
              placeholder={t('varietyFinder.anySoil')}
              options={soils.length > 0 ? soils.map(s => ({ value: s.value, label: s.label })) : [{ value: '', label: 'Any soil' }]}
            />
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
            <span className="flex-1">{error}</span>
            <Button variant="ghost" size="sm" onClick={handleSearch}>
              {t('panel.retry')}
            </Button>
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
    </Stack>
  );
};

export default VarietyFinder;
