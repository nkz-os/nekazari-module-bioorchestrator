import React, { useState, useEffect } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Button, Stack, EmptyState, Skeleton, DetailGrid, DetailItem, Select, MetricCard } from '@nekazari/ui-kit';
import { TrendingUp, Activity, Globe, AlertTriangle, Sprout, BarChart3 } from 'lucide-react';
import { useBioApi } from '../services/api';
import { useParcelContext } from '../context/ParcelContext';
import { usePlanningScenario } from '../context/PlanningScenarioContext';

interface VarietyResult {
  variety: string;
  mean_yield_kg_ha: number;
  min_yield_kg_ha: number;
  max_yield_kg_ha: number;
  stddev_yield_kg_ha: number;
  trial_count: number;
  trial_years: number[];
  trial_sites: string[];
  confidence_interval?: [number, number];
}

interface ExtrapolateResponse {
  ranked_varieties: VarietyResult[];
  similar_sites: string[];
  target_environment: Record<string, unknown>;
  trials_analyzed: number;
  similar_sites_count: number;
}

interface PerStage {
  stage: string;
  ky: number;
  kc: number;
  d1_dap: number;
  d2_dap: number;
  status: 'completed' | 'current' | 'future';
  etc_estimate_mm: number;
  etc_ratio: number;
  stage_stress_factor: number;
}

interface YieldProjResponse {
  parcel_id: string;
  crop: { eppo: string; variety?: string };
  days_since_planting: number;
  current_stage: string;
  potential_yield_kg_ha: number;
  projected_yield_kg_ha: number;
  cumulative_stress_factor: number;
  yield_loss_pct: number;
  per_stage: PerStage[];
  methodology: string;
}

interface CompareRow {
  crop: string;
  best_variety: string;
  expected_yield_kg_ha: number;
  net_margin_eur_ha: number;
  carbon_fixed_tco2e_ha: number;
  soil_warnings: string[];
}

interface CompareResponse {
  comparisons: CompareRow[];
  target_environment: Record<string, unknown>;
}

interface CropOption { eppo_code: string; scientific_name: string; }

type TabId = 'extrapolate' | 'compare' | 'inCampaign';

const KOPPEN_OPTIONS = [
  { value: 'Csa', label: 'Csa — Hot-summer Mediterranean' },
  { value: 'Csb', label: 'Csb — Warm-summer Mediterranean' },
  { value: 'Cfa', label: 'Cfa — Humid subtropical' },
  { value: 'Cfb', label: 'Cfb — Oceanic' },
  { value: 'BSh', label: 'BSh — Hot semi-arid' },
  { value: 'BSk', label: 'BSk — Cold semi-arid' },
  { value: 'BWh', label: 'BWh — Hot desert' },
  { value: 'Cwa', label: 'Cwa — Monsoon subtropical' },
  { value: 'Dfa', label: 'Dfa — Hot-summer continental' },
  { value: 'Dfb', label: 'Dfb — Warm-summer continental' },
];

export default function YieldProjection() {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
  const { enabled: scenarioEnabled } = usePlanningScenario();
  const api = useBioApi();

  const [activeTab, setActiveTab] = useState<TabId>('extrapolate');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  // Extrapolate state
  const [crop, setCrop] = useState('');
  const [climate, setClimate] = useState('Csa');
  const [cropOptions, setCropOptions] = useState<CropOption[]>([]);
  const [extrapolateResult, setExtrapolateResult] = useState<ExtrapolateResponse | null>(null);

  // Compare state
  const [selectedCrops, setSelectedCrops] = useState<Set<string>>(new Set());
  const [compareResult, setCompareResult] = useState<CompareResponse | null>(null);

  // In-Campaign state
  const [yieldProj, setYieldProj] = useState<YieldProjResponse | null>(null);

  useEffect(() => {
    api.getAgricultureCrops?.()
      .then((d: any) => {
        if (Array.isArray(d?.crops)) setCropOptions(d.crops);
      })
      .catch(() => {});
  }, []);

  // Guard states
  if (parcelLoading) return <Skeleton variant="rect" height="300px" />;
  if (parcelError) return <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={parcelError} />;
  if (!selectedParcel) {
    return (
      <EmptyState
        icon={<TrendingUp className="w-8 h-8" />}
        title={t('yieldProjection.selectParcel')}
      />
    );
  }
  if (scenarioEnabled && activeTab === 'inCampaign') {
    return (
      <EmptyState
        icon={<AlertTriangle className="w-8 h-8" />}
        title={t('scenarioMode.campaignBlocked')}
        description={t('scenarioMode.campaignBlockedDetail')}
      />
    );
  }

  const handleExtrapolate = async () => {
    if (!crop) return;
    setLoading(true); setError('');
    try {
      const data = await api.extrapolateVarieties({
        crop,
        climate_class: climate,
        top_n: '5',
      });
      setExtrapolateResult({
        ranked_varieties: data.ranked_varieties || [],
        similar_sites: data.similar_sites || [],
        target_environment: data.target_environment || {},
        trials_analyzed: data.trials_analyzed ?? (data.ranked_varieties?.reduce((s: number, v: any) => s + (v.trial_count || 0), 0) || 0),
        similar_sites_count: (data.similar_sites || []).length,
      });
    } catch (e: any) {
      setError(e.message || 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  const toggleCrop = (eppo: string) => {
    const next = new Set(selectedCrops);
    next.has(eppo) ? next.delete(eppo) : next.add(eppo);
    setSelectedCrops(next);
  };

  const handleCompare = async () => {
    if (!selectedParcel || selectedCrops.size === 0) return;
    setLoading(true); setError('');
    try {
      const data = await api.compareCrops(selectedParcel, Array.from(selectedCrops));
      setCompareResult({
        comparisons: (data.comparisons || []).map((c: any) => ({
          crop: c.crop,
          best_variety: c.agronomics?.best_variety || c.best_variety || '—',
          expected_yield_kg_ha: c.agronomics?.expected_yield_kg_ha || 0,
          net_margin_eur_ha: c.economic?.net_margin_eur_ha || 0,
          carbon_fixed_tco2e_ha: c.environmental?.carbon_fixed_tco2e_ha || 0,
          soil_warnings: c.soil_suitability?.warnings || [],
        })),
        target_environment: data.target_environment || {},
      });
    } catch (e: any) {
      setError(e.message || String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleInCampaign = async () => {
    setLoading(true); setError('');
    try {
      const data = await api.getYieldProjection(selectedParcel);
      setYieldProj(data);
    } catch (e: any) {
      setError(e.message || 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  const tabs: { id: TabId; icon: React.ElementType; label: string }[] = [
    { id: 'extrapolate', icon: Globe, label: t('yieldProjection.tabs.extrapolate') },
    { id: 'compare', icon: BarChart3, label: t('yieldProjection.tabs.compare') },
    { id: 'inCampaign', icon: Activity, label: t('yieldProjection.tabs.inCampaign') },
  ];

  const statusBadge = (status: string) => {
    const map: Record<string, 'positive' | 'warning' | 'info' | 'default'> = {
      completed: 'positive', current: 'warning', future: 'default',
    };
    return <Badge intent={map[status] || 'default'}>{status}</Badge>;
  };

  return (
    <Stack gap="section">
      {/* Header */}
      <div>
        <div className="flex items-center gap-2 mb-1">
          <TrendingUp className="w-5 h-5 text-nkz-accent-base" />
          <h2 className="text-nkz-xl font-bold text-nkz-text-primary">{t('yieldProjection.title')}</h2>
        </div>
        <p className="text-nkz-base text-nkz-text-muted">{t('yieldProjection.subtitle')}</p>
      </div>

      {/* Tab switcher */}
      <div className="flex gap-1 p-1 rounded-nkz-lg border border-nkz-border bg-nkz-surface-raised">
        {tabs.map(tab => (
          <button
            key={tab.id}
            type="button"
            className={`flex-1 flex items-center justify-center gap-1.5 px-4 py-2 rounded-nkz-md text-nkz-sm font-semibold transition-all ${
              activeTab === tab.id
                ? 'bg-nkz-accent-base text-nkz-text-on-accent shadow-sm'
                : 'text-nkz-text-secondary hover:bg-nkz-surface'
            }`}
            onClick={() => setActiveTab(tab.id)}
          >
            <tab.icon className="w-4 h-4" />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Error */}
      {error && (
        <Card padding="md" className="border-nkz-danger bg-nkz-danger-soft">
          <div className="flex items-center gap-2 text-nkz-danger">
            <AlertTriangle className="w-5 h-5" />
            <span>{error}</span>
          </div>
        </Card>
      )}

      {/* ── Tab: Extrapolate (Phase A) ── */}
      {activeTab === 'extrapolate' && (
        <>
          <Card padding="md">
            <div className="flex flex-wrap gap-3 items-end">
              <div className="flex-1 min-w-[200px]">
                <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
                  {t('yieldProjection.crop')}
                </label>
                <Select
                  value={crop}
                  onValueChange={setCrop}
                  placeholder={t('yieldProjection.selectCrop')}
                  options={cropOptions.map(c => ({
                    value: c.eppo_code,
                    label: `${t(`crops.${c.eppo_code}`, { defaultValue: c.scientific_name })} (${c.eppo_code})`,
                  }))}
                />
              </div>
              <div className="w-[220px]">
                <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
                  {t('yieldProjection.climateClass')}
                </label>
                <Select
                  value={climate}
                  onValueChange={setClimate}
                  options={KOPPEN_OPTIONS}
                />
              </div>
              <Button onClick={handleExtrapolate} disabled={!crop || loading} loading={loading}>
                <Globe className="w-4 h-4 mr-1" />
                {t('yieldProjection.search')}
              </Button>
            </div>
          </Card>

          {loading && <Skeleton variant="rect" height="200px" />}

          {extrapolateResult && (
            <>
              {/* Data quality badge */}
              <Card padding="md">
                <DetailGrid columns={2}>
                  <DetailItem label={t('yieldProjection.trialsAnalyzed')} value={String(extrapolateResult.trials_analyzed)} />
                  <DetailItem label={t('yieldProjection.similarSites')} value={String(extrapolateResult.similar_sites_count)} />
                </DetailGrid>
              </Card>

              {/* Variety table */}
              <Card padding="md">
                <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-3">
                  {t('yieldProjection.topVarieties')} ({extrapolateResult.ranked_varieties.length})
                </h3>
                {extrapolateResult.ranked_varieties.length === 0 ? (
                  <p className="text-nkz-sm text-nkz-text-muted">{t('yieldProjection.noResults')}</p>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-nkz-sm">
                      <thead>
                        <tr className="border-b border-nkz-border text-nkz-text-secondary">
                          <th className="text-left py-2 pr-3">#</th>
                          <th className="text-left py-2 pr-3">{t('yieldProjection.variety')}</th>
                          <th className="text-right py-2 pr-3">{t('yieldProjection.meanYield')}</th>
                          <th className="text-right py-2 pr-3">{t('yieldProjection.trials')}</th>
                          <th className="text-right py-2">{t('yieldProjection.confidence')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {extrapolateResult.ranked_varieties.map((v, i) => (
                          <tr key={v.variety} className="border-b border-nkz-border-subtle hover:bg-nkz-surface-sunken">
                            <td className="py-2 pr-3 text-nkz-text-muted">{i + 1}</td>
                            <td className="py-2 pr-3 font-medium text-nkz-text-primary">{v.variety}</td>
                            <td className="py-2 pr-3 text-right font-semibold text-nkz-accent-base">
                              {v.mean_yield_kg_ha?.toLocaleString()} kg/ha
                            </td>
                            <td className="py-2 pr-3 text-center">
                              <Badge intent={v.trial_count >= 5 ? 'positive' : 'warning'}>{v.trial_count}</Badge>
                            </td>
                            <td className="py-2 text-right text-nkz-text-secondary">
                              {v.confidence_interval ? `±${v.confidence_interval[1] - v.mean_yield_kg_ha} kg/ha` : '—'}
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

          {!extrapolateResult && !loading && (
            <EmptyState
              icon={<Globe className="w-6 h-6" />}
              title={t('yieldProjection.pickCrop')}
              description={t('yieldProjection.pickCropDesc')}
            />
          )}
        </>
      )}

      {/* ── Tab: Multi-Crop Compare (Phase B) ── */}
      {activeTab === 'compare' && (
        <>
          <Card padding="md">
            <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-2">
              {t('yieldProjection.selectCrops')}
            </label>
            <div className="flex flex-wrap gap-2 mb-3">
              {cropOptions.slice(0, 20).map(c => {
                const selected = selectedCrops.has(c.eppo_code);
                return (
                  <button
                    key={c.eppo_code}
                    type="button"
                    className={`px-3 py-1.5 rounded-nkz-md text-nkz-xs font-medium transition-all border ${
                      selected
                        ? 'bg-nkz-accent-base text-nkz-text-on-accent border-nkz-accent-base'
                        : 'bg-nkz-surface text-nkz-text-secondary border-nkz-border hover:border-nkz-accent-base'
                    }`}
                    onClick={() => toggleCrop(c.eppo_code)}
                  >
                    {t(`crops.${c.eppo_code}`, { defaultValue: c.scientific_name })}
                  </button>
                );
              })}
            </div>
            <Button onClick={handleCompare} disabled={selectedCrops.size === 0 || loading} loading={loading}>
              <BarChart3 className="w-4 h-4 mr-1" />
              {t('yieldProjection.compare')}
            </Button>
          </Card>

          {loading && <Skeleton variant="rect" height="200px" />}

          {compareResult && (
            <Card padding="md">
              <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-3">
                {t('yieldProjection.comparisonResults')}
              </h3>
              <div className="overflow-x-auto">
                <table className="w-full text-nkz-sm">
                  <thead>
                    <tr className="border-b border-nkz-border text-nkz-text-secondary">
                      <th className="text-left py-2 pr-3">{t('yieldProjection.crop')}</th>
                      <th className="text-left py-2 pr-3">{t('yieldProjection.bestVariety')}</th>
                      <th className="text-right py-2 pr-3">{t('yieldProjection.meanYield')}</th>
                      <th className="text-right py-2 pr-3">{t('yieldProjection.margin')}</th>
                      <th className="text-right py-2 pr-3">{t('yieldProjection.carbon')}</th>
                      <th className="text-left py-2">{t('yieldProjection.soil')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {compareResult.comparisons.map((c, i) => (
                      <tr key={i} className="border-b border-nkz-border-subtle hover:bg-nkz-surface-sunken">
                        <td className="py-2 pr-3 font-medium">{c.crop}</td>
                        <td className="py-2 pr-3 text-nkz-text-secondary">{c.best_variety}</td>
                        <td className="py-2 pr-3 text-right font-semibold text-nkz-accent-base">
                          {c.expected_yield_kg_ha?.toLocaleString()} kg/ha
                        </td>
                        <td className="py-2 pr-3 text-right text-nkz-positive">
                          {c.net_margin_eur_ha?.toLocaleString()} €/ha
                        </td>
                        <td className="py-2 pr-3 text-right">
                          {c.carbon_fixed_tco2e_ha?.toFixed(2)} tCO₂e/ha
                        </td>
                        <td className="py-2 text-left">
                          {c.soil_warnings.length > 0 ? (
                            <div className="flex flex-wrap gap-1">
                              {c.soil_warnings.map((w, j) => (
                                <Badge key={j} intent="warning">{w}</Badge>
                              ))}
                            </div>
                          ) : (
                            <Badge intent="positive">{t('yieldProjection.suitable')}</Badge>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}

          {!compareResult && !loading && (
            <EmptyState
              icon={<BarChart3 className="w-6 h-6" />}
              title={t('yieldProjection.pickCrops')}
            />
          )}
        </>
      )}

      {/* ── Tab: In-Campaign (Phase B yield-projection) ── */}
      {activeTab === 'inCampaign' && (
        <>
          <Card padding="md">
            <p className="text-nkz-sm text-nkz-text-muted mb-3">{t('yieldProjection.inCampaignDesc')}</p>
            <Button onClick={handleInCampaign} loading={loading}>
              <Activity className="w-4 h-4 mr-1" />
              {t('yieldProjection.loadProjection')}
            </Button>
          </Card>

          {loading && <Skeleton variant="rect" height="250px" />}

          {yieldProj && (
            <>
              {/* Current stage badge */}
              <Card padding="md">
                <div className="flex items-center gap-2 mb-2">
                  <Sprout className="w-4 h-4 text-nkz-accent-base" />
                  <span className="text-nkz-sm font-semibold">{t('yieldProjection.currentStage')}:</span>
                  <Badge intent="info">{yieldProj.current_stage || '—'}</Badge>
                  <span className="text-nkz-xs text-nkz-text-muted">
                    ({t('yieldProjection.daysSincePlanting')}: {yieldProj.days_since_planting})
                  </span>
                </div>
              </Card>

              {/* Potential vs Projected */}
              <div className="grid grid-cols-2 gap-4">
                <Card padding="lg" className="text-center border-nkz-positive bg-nkz-positive-soft">
                  <p className="text-nkz-xs text-nkz-text-secondary">{t('yieldProjection.potential')}</p>
                  <p className="text-nkz-2xl font-bold text-nkz-positive">
                    {yieldProj.potential_yield_kg_ha?.toLocaleString()} kg/ha
                  </p>
                </Card>
                <Card padding="lg" className="text-center border-nkz-accent-base bg-nkz-accent-soft">
                  <p className="text-nkz-xs text-nkz-text-secondary">{t('yieldProjection.projected')}</p>
                  <p className="text-nkz-2xl font-bold text-nkz-accent-base">
                    {yieldProj.projected_yield_kg_ha?.toLocaleString()} kg/ha
                  </p>
                  <Badge intent={yieldProj.yield_loss_pct > 10 ? 'warning' : 'positive'}>
                    {yieldProj.yield_loss_pct?.toFixed(1)}% {t('yieldProjection.loss')}
                  </Badge>
                </Card>
              </div>

              {/* Stress gauge */}
              <Card padding="md">
                <div className="flex items-center gap-2 mb-2">
                  <Activity className="w-4 h-4 text-nkz-accent-base" />
                  <span className="text-nkz-sm font-semibold">{t('yieldProjection.stressFactor')}</span>
                </div>
                <div className="w-full bg-nkz-surface-sunken rounded-full h-4">
                  <div
                    className={`h-4 rounded-full transition-all ${
                      yieldProj.cumulative_stress_factor > 0.5 ? 'bg-nkz-danger' :
                      yieldProj.cumulative_stress_factor > 0.2 ? 'bg-nkz-warning' : 'bg-nkz-positive'
                    }`}
                    style={{ width: `${Math.round(yieldProj.cumulative_stress_factor * 100)}%` }}
                  />
                </div>
                <p className="text-nkz-xs text-nkz-text-muted mt-1">
                  {(yieldProj.cumulative_stress_factor * 100).toFixed(0)}% — {yieldProj.methodology}
                </p>
              </Card>

              {/* Per-stage breakdown */}
              {yieldProj.per_stage && yieldProj.per_stage.length > 0 && (
                <Card padding="md">
                  <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-3">
                    {t('yieldProjection.perStage')}
                  </h3>
                  <div className="overflow-x-auto">
                    <table className="w-full text-nkz-sm">
                      <thead>
                        <tr className="border-b border-nkz-border text-nkz-text-secondary">
                          <th className="text-left py-2 pr-3">{t('yieldProjection.stage')}</th>
                          <th className="text-right py-2 pr-3">Ky</th>
                          <th className="text-right py-2 pr-3">Kc</th>
                          <th className="text-right py-2 pr-3">{t('yieldProjection.etc')}</th>
                          <th className="text-right py-2 pr-3">{t('yieldProjection.stress')}</th>
                          <th className="text-left py-2">{t('yieldProjection.status')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {yieldProj.per_stage.map((s, i) => (
                          <tr key={i} className={`border-b border-nkz-border-subtle ${
                            s.status === 'current' ? 'bg-nkz-accent-soft' : ''
                          }`}>
                            <td className="py-2 pr-3 font-medium">{s.stage}</td>
                            <td className="py-2 pr-3 text-right">{s.ky?.toFixed(2)}</td>
                            <td className="py-2 pr-3 text-right">{s.kc?.toFixed(2)}</td>
                            <td className="py-2 pr-3 text-right">{s.etc_estimate_mm?.toFixed(0)} mm</td>
                            <td className="py-2 pr-3 text-right">{((s.stage_stress_factor || 0) * 100).toFixed(0)}%</td>
                            <td className="py-2">{statusBadge(s.status)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </Card>
              )}
            </>
          )}

          {!yieldProj && !loading && !error && (
            <EmptyState
              icon={<Activity className="w-6 h-6" />}
              title={t('yieldProjection.noProjection')}
              description={t('yieldProjection.noProjectionDesc')}
            />
          )}
        </>
      )}
    </Stack>
  );
}
