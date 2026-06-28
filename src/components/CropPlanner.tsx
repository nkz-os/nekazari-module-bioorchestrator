import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Button, Stack, EmptyState, Skeleton, Select, ProgressBar } from '@nekazari/ui-kit';
import { Search, Sprout, MapPin, TrendingUp, CheckCircle, AlertTriangle } from 'lucide-react';
import { useBioApi } from '../services/api';
import { useParcelContext } from '../context/ParcelContext';
import { usePlanningScenario } from '../context/PlanningScenarioContext';
import EconomicInputsPanel, { EconomicInputs } from './EconomicInputsPanel';
import RecommendationTrustBadge from './RecommendationTrustBadge';
import CropExpandPanel from './CropExpandPanel';
import AssignVarietyModal from './AssignVarietyModal';

interface CropSuggestion {
  crop_eppo: string;
  crop_name: string;
  season_slot: string[];
  best_variety: string;
  crop_uri: string;
  variety_uri: string;
  agronomics: { expected_yield_kg_ha: number; confidence_interval?: [number, number]; trials_analyzed: number; confidence: string };
  yield_conventional_kg_ha: number;
  yield_organic_kg_ha?: number | null;
  economic: { harvest_price_eur_t: number; gross_revenue_eur_ha: number; net_margin_eur_ha: number; organic_net_margin_eur_ha?: number | null; parcel_net_margin_eur?: number | null };
  suitability: { overall: string; warnings: string[] };
  thermal_risk: string;
  water_demand?: { level: string; ratio: number } | null;
  gluten_status?: string;
  composite_score: number;
  recommendation_trust: any;
}

interface SuggestResult {
  parcel_id: string;
  target_environment: any;
  filters_applied: any;
  suggestions: CropSuggestion[];
  data_quality: any;
  error?: string;
}

type ViewState = 'idle' | 'loading' | 'results' | 'error';
type PlannerTab = 'ranking' | 'optimize';

const DEFAULT_ECONOMICS: EconomicInputs = {
  seedPrice: 0,
  harvestPrice: 220,
  priceUnit: 'eur_per_t',
  operationCost: 80,
};

const SEASON_TABS = [
  { value: 'all', key: 'planning.seasonAll' },
  { value: 'winter', key: 'planning.seasonWinter' },
  { value: 'summer', key: 'planning.seasonSummer' },
] as const;

const MANAGEMENT_OPTIONS = [
  { value: 'any', key: 'planning.managementAny' },
  { value: 'conventional', key: 'planning.managementConventional' },
  { value: 'organic', key: 'planning.managementOrganic' },
];

interface CropPlannerProps {
  onNavigateTool?: (toolId: string) => void;
}

const CropPlanner: React.FC<CropPlannerProps> = ({ onNavigateTool }) => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
  const { setCrop: setScenarioCrop, setEnabled: setScenarioEnabled } = usePlanningScenario();

  const [seasonSlot, setSeasonSlot] = useState('all');
  const [management, setManagement] = useState('any');
  const [irrigation, setIrrigation] = useState('');
  const [glutenFreeOnly, setGlutenFreeOnly] = useState(false);
  const [inferredIrrigation, setInferredIrrigation] = useState<string | null>(null);
  const [economics, setEconomics] = useState<EconomicInputs>(DEFAULT_ECONOMICS);
  const [activeTab, setActiveTab] = useState<PlannerTab>('ranking');
  const [view, setView] = useState<ViewState>('idle');
  const [result, setResult] = useState<SuggestResult | null>(null);
  const [error, setError] = useState('');
  const [envBadge, setEnvBadge] = useState<string>('');

  // Assign modal
  const [assignVariety, setAssignVariety] = useState<any>(null);
  const [assignedMessage, setAssignedMessage] = useState('');
  const [expandedEppo, setExpandedEppo] = useState<string | null>(null);
  const [rankingSelection, setRankingSelection] = useState<string | null>(null);

  // Load parcel environment on select
  useEffect(() => {
    if (!selectedParcel) return;
    api.parcelEnvironment?.(selectedParcel)
      .then((env: any) => {
        if (!env || env.error) return;
        const parts: string[] = [];
        if (env.climate_class) parts.push(env.climate_class);
        if (env.soil?.texture) parts.push(env.soil.texture);
        if (env.area_ha) parts.push(`${env.area_ha} ha`);
        setEnvBadge(parts.join(' · '));
        if (env.irrigation?.inferred) {
          setInferredIrrigation(env.irrigation.inferred);
          setIrrigation('');
        }
      })
      .catch(() => {});
  }, [selectedParcel]);

  const handleAnalyze = useCallback(async () => {
    if (!selectedParcel) return;
    setView('loading');
    setError('');
    try {
      const data = await api.suggestCrops?.({
        parcel_id: selectedParcel,
        season_slot: seasonSlot,
        management,
        irrigation_regime: irrigation || (inferredIrrigation || ''),
        top_n: 15,
        seed_price: economics.seedPrice,
        harvest_price: economics.harvestPrice,
        price_unit: economics.priceUnit,
        operation_cost: economics.operationCost,
      });
      if (data?.error) {
        setError(data.error);
        setView('error');
        return;
      }
      setResult(data);
      setView('results');
    } catch (e: any) {
      setError(e.message || 'Unknown error');
      setView('error');
    }
  }, [selectedParcel, seasonSlot, management, irrigation, inferredIrrigation, economics]);

  const handleAssignClick = (s: CropSuggestion) => {
    setAssignVariety({
      name: s.best_variety,
      cropUri: s.crop_uri,
      varietyUri: s.variety_uri,
      expectedYield: s.agronomics.expected_yield_kg_ha,
      confidenceInterval: s.agronomics.confidence_interval || [s.agronomics.expected_yield_kg_ha, s.agronomics.expected_yield_kg_ha],
      trialCount: s.agronomics.trials_analyzed,
    });
  };

  if (parcelLoading) return <Card padding="md"><Skeleton variant="rect" height="200px" /></Card>;

  if (parcelError) {
    return <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={parcelError} />;
  }

  if (!selectedParcel) {
    return (
      <EmptyState
        icon={<MapPin className="w-8 h-8" />}
        title={t('varietyFinder.selectParcel', { defaultValue: 'Select a parcel' })}
        description={t('varietyFinder.selectParcelDesc', { defaultValue: 'Choose a parcel to analyze' })}
      />
    );
  }

  return (
    <Stack gap="section">
      {/* Header */}
      <Stack gap="tight">
        <h2 className="text-nkz-lg font-bold text-nkz-text-primary flex items-center gap-2">
          <Sprout className="w-5 h-5 text-nkz-accent-base" />
          {t('planning.recommendCrops', { defaultValue: 'Recomendar cultivos' })}
        </h2>
        {envBadge && <Badge intent="info">{envBadge}</Badge>}
      </Stack>

      {/* Tab bar */}
      <div className="flex gap-1 p-1 rounded-nkz-lg border border-nkz-border bg-nkz-surface-raised">
        <button
          className={`flex-1 px-4 py-2 rounded-nkz-md text-nkz-sm font-semibold transition-colors ${
            activeTab === 'ranking' ? 'bg-nkz-accent-base text-nkz-text-on-accent' : 'text-nkz-text-secondary hover:text-nkz-text-primary'
          }`}
          onClick={() => setActiveTab('ranking')}
        >
          {t('planning.recommendCrops', { defaultValue: 'Recomendar' })}
        </button>
        <button
          className={`flex-1 px-4 py-2 rounded-nkz-md text-nkz-sm font-semibold transition-colors ${
            activeTab === 'optimize' ? 'bg-nkz-accent-base text-nkz-text-on-accent' : 'text-nkz-text-secondary hover:text-nkz-text-primary'
          }`}
          onClick={() => setActiveTab('optimize')}
        >
          🔄 {t('rotationOptimizer.optimize', { defaultValue: 'Optimizar' })}
        </button>
      </div>

      {activeTab === 'ranking' && (
      <>
      <Card padding="md">
        <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-2">
          {t('planning.filtersTitle', { defaultValue: 'Condiciones de cultivo' })}
        </h3>
        <div className="flex flex-wrap gap-3 items-end mb-3">
          {/* Season tabs */}
          <div className="flex gap-1">
            {SEASON_TABS.map(tab => (
              <Button
                key={tab.value}
                variant={seasonSlot === tab.value ? 'primary' : 'ghost'}
                size="sm"
                onClick={() => setSeasonSlot(tab.value)}
              >
                {t(tab.key, { defaultValue: tab.value === 'all' ? 'Todas' : tab.value === 'winter' ? 'Invierno' : 'Verano' })}
              </Button>
            ))}
          </div>

          {/* Management */}
          <div className="w-[140px]">
            <Select
              value={management}
              onValueChange={setManagement}
              options={MANAGEMENT_OPTIONS.map(o => ({
                value: o.value,
                label: t(o.key, { defaultValue: o.value }),
              }))}
            />
          </div>

          {/* Irrigation */}
          <div className="w-[160px]">
            <Select
              value={irrigation}
              onValueChange={setIrrigation}
              placeholder={inferredIrrigation
                ? `${t('planning.irrigationInferred', { defaultValue: 'Inferido' })}: ${inferredIrrigation}`
                : t('planning.irrigationOverride', { defaultValue: 'Régimen hídrico' })
              }
              options={[
                { value: '', label: inferredIrrigation ? `${t('planning.irrigationInferred', { defaultValue: 'Inferido' })} (${inferredIrrigation})` : t('planning.irrigationOverride', { defaultValue: 'Régimen' }) },
                { value: 'secano', label: t('planning.irrigationOverride', { defaultValue: 'Secano' }) },
                { value: 'regadío', label: t('planning.irrigationOverride', { defaultValue: 'Regadío' }) },
              ]}
            />
          </div>
        </div>

        {/* Gluten-free toggle */}
        <div className="flex items-center gap-2 mt-2">
          <input
            type="checkbox"
            id="glutenFree"
            checked={glutenFreeOnly}
            onChange={e => setGlutenFreeOnly(e.target.checked)}
            className="rounded border-nkz-border"
          />
          <label htmlFor="glutenFree" className="text-nkz-sm text-nkz-text-secondary cursor-pointer">
            🌾 {t('planning.glutenFreeOnly', { defaultValue: 'Sin gluten solamente' })}
          </label>
        </div>

        {/* Economics */}
        <EconomicInputsPanel value={economics} onChange={setEconomics} />
      </Card>

      {/* CTA */}
      <Button size="lg" onClick={handleAnalyze} disabled={view === 'loading'} loading={view === 'loading'}>
        <Search className="w-4 h-4 mr-1" />
        {t('planning.recommendCrops', { defaultValue: 'Recomendar cultivos' })}
      </Button>

      {/* Loading */}
      {view === 'loading' && (
        <Card padding="md">
          <Skeleton variant="rect" height="40px" />
          <div className="mt-2"><Skeleton variant="rect" height="300px" /></div>
        </Card>
      )}

      {/* Error */}
      {view === 'error' && (
        <Card padding="md">
          <div className="flex items-center gap-2 text-nkz-error">
            <AlertTriangle className="w-5 h-5" />
            <span className="flex-1">{error}</span>
            <Button variant="ghost" size="sm" onClick={handleAnalyze}>
              {t('panel.retry', { defaultValue: 'Reintentar' })}
            </Button>
          </div>
        </Card>
      )}

      {/* Results */}
      {view === 'results' && result && (
        <>
          {(() => {
            const filtered = glutenFreeOnly
              ? result.suggestions.filter(s => s.gluten_status !== 'contains_gluten')
              : result.suggestions;

            return filtered.length === 0 ? (
            <EmptyState
              icon={<Search className="w-8 h-8" />}
              title={t('planning.noSuggestions', { defaultValue: 'Sin datos de ensayo para este perfil' })}
            />
          ) : (
            <Card padding="md">
              <div className="overflow-x-auto">
                <table className="w-full text-nkz-sm">
                  <thead>
                    <tr className="border-b border-nkz-border text-nkz-text-secondary">
                      <th className="text-left py-2 pr-2">#</th>
                      <th className="text-left py-2 pr-3">{t('varietyFinder.variety', { defaultValue: 'Variedad' })}</th>
                      <th className="text-left py-2 pr-3">{t('planning.seasonAll', { defaultValue: 'Temp.' })}</th>
                      <th className="text-right py-2 pr-3">{t('yieldProjection.meanYield', { defaultValue: 'kg/ha' })}</th>
                      {management === 'organic' && <th className="text-right py-2 pr-3">{t('planning.pricePerKg', { defaultValue: 'Eco kg/ha' })}</th>}
                      <th className="text-right py-2 pr-3">{t('planning.marginPerHa', { defaultValue: 'Margen €/ha' })}</th>
                      <th className="text-center py-2 pr-2">{t('varietyFinder.confidence', { defaultValue: 'Fiab.' })}</th>
                      <th className="text-center py-2">💧</th>
                      <th className="text-center py-2"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {filtered.map((s: any, i: number) => (
                      <React.Fragment key={s.crop_eppo}>
                      <tr className="border-b border-nkz-border-subtle hover:bg-nkz-surface-sunken cursor-pointer"
                          onClick={() => {
                            setExpandedEppo(expandedEppo === s.crop_eppo ? null : s.crop_eppo);
                            setRankingSelection(s.crop_eppo);
                          }}>
                        <td className="py-2 pr-2 text-nkz-text-muted">{i + 1}</td>
                        <td className="py-2 pr-3">
                          <div className="font-medium text-nkz-text-primary">{s.best_variety || s.crop_eppo}</div>
                          <div className="text-nkz-xs text-nkz-text-secondary">{s.crop_name || s.crop_eppo}</div>
                        </td>
                        <td className="py-2 pr-3">
                          <Badge intent="info">
                            {s.season_slot?.includes('winter') ? '❄️' : ''}{s.season_slot?.includes('summer') ? '☀️' : ''}
                          </Badge>
                          {s.gluten_status === 'gluten_free' && (
                            <Badge intent="positive" className="ml-1">🌾 GF</Badge>
                          )}
                          {s.gluten_status === 'gluten_free_caveat' && (
                            <Badge intent="warning" className="ml-1">🌾 GF*</Badge>
                          )}
                        </td>
                        <td className="py-2 pr-3 text-right font-semibold text-nkz-accent-base">
                          {s.yield_conventional_kg_ha?.toLocaleString()}
                        </td>
                        {management === 'organic' && (
                          <td className="py-2 pr-3 text-right text-nkz-text-secondary">
                            {s.yield_organic_kg_ha?.toLocaleString() || '—'}
                            {s.recommendation_trust?.organic_warning && (
                              <span title={s.recommendation_trust.organic_warning}>
                                <AlertTriangle className="w-3 h-3 inline ml-1 text-nkz-text-warning" />
                              </span>
                            )}
                          </td>
                        )}
                        <td className="py-2 pr-3 text-right">
                          <div className="font-medium text-nkz-text-primary">{s.economic.net_margin_eur_ha?.toFixed(0)} €</div>
                          {s.economic.parcel_net_margin_eur != null && (
                            <div className="text-nkz-xs text-nkz-text-secondary">{s.economic.parcel_net_margin_eur.toFixed(0)} € {t('planning.marginParcel', { defaultValue: 'parcela' })}</div>
                          )}
                        </td>
                        <td className="py-2 pr-2 text-center">
                          <RecommendationTrustBadge trust={s.recommendation_trust} compact />
                        </td>
                        <td className="py-2 text-center">
                          {s.water_demand && (
                            <span title={`${s.water_demand.level} (${s.water_demand.ratio})`}>
                              {s.water_demand.level === 'low' ? '🟢' : s.water_demand.level === 'medium' ? '🟡' : '🔴'}
                            </span>
                          )}
                        </td>
                        <td className="py-2 text-center">
                          <Button variant="secondary" size="sm" onClick={() => handleAssignClick(s)}>
                            {t('planning.assignCrop', { defaultValue: 'Adjudicar' })}
                          </Button>
                        </td>
                      </tr>
                      {expandedEppo === s.crop_eppo &&
                        <tr key={`${s.crop_eppo}-expand`} className="border-b border-nkz-border-subtle bg-nkz-surface-sunken">
                          <td colSpan={9} className="py-2 px-3">
                            <CropExpandPanel suggestion={s} />
                          </td>
                        </tr>
                      }
                      </React.Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          );
          })()}

          {rankingSelection && view === 'results' && (
            <RotationPacPanel
              parcelId={selectedParcel}
              startingCrop={rankingSelection}
              management={management}
              economics={economics}
              api={api}
            />
          )}
        </>
      )}

      {/* Assign modal */}
      {assignVariety && (
        <AssignVarietyModal
          variety={assignVariety}
          parcelId={selectedParcel}
          onClose={() => setAssignVariety(null)}
          onAssigned={(_pid: string) => {
            setAssignVariety(null);
            setAssignedMessage(t('planning.assignCrop', { defaultValue: 'Adjudicado' }) + ': ' + assignVariety.name);
            setScenarioEnabled(false);
            setScenarioCrop(null);
            setTimeout(() => setAssignedMessage(''), 5000);
          }}
        />
      )}

      {/* Post-assign success */}
      {assignedMessage && (
        <Card padding="md">
          <div className="flex flex-col gap-2">
            <div className="flex items-center gap-2 text-nkz-success">
              <CheckCircle className="w-5 h-5" />
              <span className="font-medium">{assignedMessage}</span>
            </div>
            {onNavigateTool && (
              <div className="flex flex-wrap gap-2">
                <Button variant="ghost" size="sm" onClick={() => onNavigateTool('parcelStatus')}>
                  {t('planning.postAssignHealth', { defaultValue: 'Ver salud de parcela' })}
                </Button>
                <Button variant="ghost" size="sm" onClick={() => onNavigateTool('waterBudget')}>
                  {t('planning.postAssignWater', { defaultValue: 'Balance hídrico' })}
                </Button>
              </div>
            )}
          </div>
        </Card>
      )}
      </>
      )}

      {activeTab === 'optimize' && (
        <OptimizerPanel
          parcelId={selectedParcel}
          management={management}
          irrigation={irrigation || (inferredIrrigation || '')}
          economics={economics}
          glutenFreeOnly={glutenFreeOnly}
          api={api}
          initialLockEppo={rankingSelection}
          onAssign={(variety) => setAssignVariety(variety)}
        />
      )}
    </Stack>
  );
};

// ── PAC rotation panel (US-5: embedded in ranking tab) ─────────────────

interface RotationPacPanelProps {
  parcelId: string;
  startingCrop: string;
  management: string;
  economics: EconomicInputs;
  api: ReturnType<typeof useBioApi>;
}

const RotationPacPanel: React.FC<RotationPacPanelProps> = ({
  parcelId, startingCrop, management, economics, api,
}) => {
  const { t } = useTranslation('bioorchestrator');
  const [plan, setPlan] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError('');
    api.rotationPlan?.({
      parcel_id: parcelId,
      years: 4,
      starting_crop: startingCrop,
      management,
      seed_price: economics.seedPrice,
      harvest_price: economics.harvestPrice,
      operation_cost: economics.operationCost,
    })
      .then((data: any) => {
        if (cancelled) return;
        if (data?.error) {
          setError(data.error);
          setPlan(null);
        } else {
          setPlan(data);
        }
      })
      .catch((e: Error) => {
        if (!cancelled) setError(e.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [parcelId, startingCrop, management, economics.seedPrice, economics.harvestPrice, economics.operationCost]);

  return (
    <Card padding="md">
      <h3 className="text-nkz-sm font-semibold mb-2">
        🇪🇺 {t('planning.rotationTitle', { defaultValue: 'Rotación PAC (4 años)' })}
      </h3>
      <div className="text-nkz-xs text-nkz-text-muted mb-2">
        {t('planning.addToRotation', { defaultValue: 'Añadir a rotación' })}: <strong>{startingCrop}</strong>
      </div>
      {loading && <Skeleton variant="rect" height="80px" />}
      {error && <div className="text-nkz-error text-nkz-sm">{error}</div>}
      {plan?.plan && (
        <>
          <div className="flex gap-2 flex-wrap mb-3">
            {plan.plan.map((entry: any) => (
              <div
                key={entry.year}
                className={`flex-1 min-w-[140px] p-2 rounded-nkz-md border text-nkz-xs ${
                  entry.rotation_warning
                    ? 'bg-nkz-warning-soft border-nkz-warning'
                    : 'bg-nkz-info-soft border-nkz-border'
                }`}
              >
                <div className="text-nkz-text-muted">{t('rotationPlanner.year', { defaultValue: 'Año' })} {entry.year}</div>
                <div className="font-semibold">{entry.crop}</div>
                <div>{entry.expected_yield_kg_ha?.toLocaleString()} kg/ha</div>
                {entry.rotation_warning && (
                  <div className="text-nkz-warning mt-1">⚠️ {entry.rotation_warning}</div>
                )}
              </div>
            ))}
          </div>
          {plan.pac_compliance && (
            <div>
              <div className="flex items-center gap-3 mb-2">
                <span className="text-nkz-lg font-bold">{plan.pac_compliance.score}%</span>
                <ProgressBar
                  value={plan.pac_compliance.score}
                  intent={plan.pac_compliance.score >= 80 ? 'positive' : plan.pac_compliance.score >= 50 ? 'warning' : 'negative'}
                  showLabel
                />
              </div>
              {plan.pac_compliance.rules?.map((rule: any) => (
                <div key={rule.id} className="text-nkz-xs mb-1">
                  {rule.pass ? '✅' : '❌'} {t(`rotationPlanner.pac.rule.${rule.id}`, { defaultValue: rule.id })} — {rule.detail}
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </Card>
  );
};

// ── Optimizer sub-component ──────────────────────────────────────────────

interface OptimizerProps {
  parcelId: string;
  management: string;
  irrigation: string;
  economics: EconomicInputs;
  glutenFreeOnly: boolean;
  api: ReturnType<typeof useBioApi>;
  initialLockEppo?: string | null;
  onAssign?: (variety: { name: string; crop_eppo: string; variety_uri?: string; crop_uri?: string }) => void;
}

const OPT_DIMS = ['protein', 'carbon', 'n_fixation', 'margin', 'yield'] as const;

const OptimizerPanel: React.FC<OptimizerProps> = ({
  parcelId, management, irrigation, economics, glutenFreeOnly, api, initialLockEppo, onAssign,
}) => {
  const { t } = useTranslation('bioorchestrator');
  const [years, setYears] = useState(4);
  const [optGlutenFree, setOptGlutenFree] = useState(glutenFreeOnly);
  const [optManagement, setOptManagement] = useState(management);
  const [optIrrigation, setOptIrrigation] = useState(irrigation);
  const [lockedYears, setLockedYears] = useState<Record<number, string>>({});
  const [lockDraft, setLockDraft] = useState<Record<number, string>>({});
  const [expandedAlts, setExpandedAlts] = useState<number | null>(null);
  const [priorities, setPriorities] = useState<Record<number, Record<string, number>>>({
    1: { protein: 40, carbon: 10, n_fixation: 60, margin: 20, yield: 20 },
    2: { protein: 30, carbon: 50, n_fixation: 20, margin: 40, yield: 10 },
    3: { protein: 20, carbon: 70, n_fixation: 10, margin: 50, yield: 30 },
    4: { protein: 50, carbon: 20, n_fixation: 40, margin: 30, yield: 10 },
  });
  const [optResult, setOptResult] = useState<any>(null);
  const [optLoading, setOptLoading] = useState(false);
  const [optError, setOptError] = useState('');

  useEffect(() => {
    if (initialLockEppo) {
      setLockedYears(prev => ({ ...prev, 1: initialLockEppo }));
      setLockDraft(prev => ({ ...prev, 1: initialLockEppo }));
    }
  }, [initialLockEppo]);

  const dimLabel = (key: string) => {
    const map: Record<string, string> = {
      protein: t('rotationOptimizer.protein', { defaultValue: 'Proteína' }),
      carbon: t('rotationOptimizer.carbon', { defaultValue: 'Carbono' }),
      n_fixation: t('rotationOptimizer.nitrogen', { defaultValue: 'Nitrógeno' }),
      margin: t('rotationOptimizer.margin', { defaultValue: 'Margen' }),
      yield: t('rotationOptimizer.yield', { defaultValue: 'Rendimiento' }),
    };
    return map[key] || key;
  };

  const toggleLockYear = (yr: number) => {
    const eppo = (lockDraft[yr] || '').trim().toUpperCase();
    if (!eppo) return;
    setLockedYears(prev => {
      const next = { ...prev };
      if (next[yr] === eppo) {
        delete next[yr];
      } else {
        next[yr] = eppo;
      }
      return next;
    });
  };

  const unlockYear = (yr: number) => {
    setLockedYears(prev => {
      const next = { ...prev };
      delete next[yr];
      return next;
    });
  };

  const handleOptimize = async () => {
    setOptLoading(true);
    setOptError('');
    try {
      const prioArray = Array.from({ length: years }, (_, i) => ({
        year: i + 1,
        ...(priorities[i + 1] || { protein: 20, carbon: 20, n_fixation: 20, margin: 20, yield: 20 }),
      }));
      const data = await api.optimizeRotation?.({
        parcel_id: parcelId,
        years,
        constraints: {
          gluten_free_only: optGlutenFree,
          management: optManagement,
          irrigation_regime: optIrrigation || undefined,
        },
        priorities: prioArray,
        locked_years: Object.keys(lockedYears).length ? lockedYears : undefined,
        seed_price: economics.seedPrice,
        harvest_price: economics.harvestPrice,
        price_unit: economics.priceUnit,
        operation_cost: economics.operationCost,
      });
      if (!data || data.error) throw new Error(data?.error || t('rotationOptimizer.noValidRotation', { defaultValue: 'No valid rotation' }));
      setOptResult(data);
    } catch (e: any) {
      setOptError(e.message || String(e));
    } finally {
      setOptLoading(false);
    }
  };

  return (
    <>
      <Card padding="md">
        <h3 className="text-nkz-sm font-semibold mb-2">{t('rotationOptimizer.constraints', { defaultValue: 'Restricciones' })}</h3>
        <div className="flex flex-wrap gap-3 items-center">
          <label className="flex items-center gap-1 text-nkz-sm">
            <input type="checkbox" checked={optGlutenFree} onChange={e => setOptGlutenFree(e.target.checked)} />
            🌾 {t('rotationOptimizer.glutenFree', { defaultValue: 'Sin gluten' })}
          </label>
          <select value={optManagement} onChange={e => setOptManagement(e.target.value)} className="rounded-md border border-nkz-border px-2 py-1 text-nkz-sm">
            <option value="any">{t('planning.managementAny', { defaultValue: 'Cualquiera' })}</option>
            <option value="conventional">{t('planning.managementConventional', { defaultValue: 'Convencional' })}</option>
            <option value="organic">{t('planning.managementOrganic', { defaultValue: 'Ecológico' })}</option>
          </select>
        </div>
      </Card>

      <Card padding="md">
        <h3 className="text-nkz-sm font-semibold mb-2">{t('rotationOptimizer.priorities', { defaultValue: 'Prioridades por año' })}</h3>
        <div className="flex gap-1 mb-3">
          {[2, 3, 4, 5, 6].map(n => (
            <Button key={n} variant={years === n ? 'primary' : 'ghost'} size="sm" onClick={() => setYears(n)}>
              {t('rotationOptimizer.yearsShort', { n, defaultValue: `${n}a` })}
            </Button>
          ))}
        </div>
        {Array.from({ length: years }, (_, i) => {
          const yr = i + 1;
          const p = priorities[yr] || { protein: 20, carbon: 20, n_fixation: 20, margin: 20, yield: 20 };
          const isLocked = Boolean(lockedYears[yr]);
          return (
            <div key={yr} className="mb-3 pb-2 border-b border-nkz-border-subtle last:border-0">
              <div className="flex flex-wrap items-center gap-2 mb-1">
                <div className="text-nkz-xs font-medium text-nkz-text-secondary">
                  {t('rotationOptimizer.year', { n: yr, defaultValue: `Año ${yr}` })}
                </div>
                {isLocked && (
                  <Badge intent="warning">
                    🔒 {lockedYears[yr]} — <button type="button" className="underline ml-1" onClick={() => unlockYear(yr)}>
                      {t('rotationOptimizer.unlock', { defaultValue: 'Desbloquear' })}
                    </button>
                  </Badge>
                )}
                <input
                  type="text"
                  placeholder="EPPO"
                  value={lockDraft[yr] || ''}
                  onChange={e => setLockDraft(prev => ({ ...prev, [yr]: e.target.value.toUpperCase() }))}
                  className="w-20 rounded border border-nkz-border px-1 py-0.5 text-nkz-xs"
                />
                <Button variant="ghost" size="sm" onClick={() => toggleLockYear(yr)}>
                  {isLocked ? t('rotationOptimizer.unlock', { defaultValue: 'Desbloquear' }) : '🔒'}
                </Button>
              </div>
              <div className="flex flex-wrap gap-2">
                {OPT_DIMS.map(d => (
                  <div key={d} className="flex items-center gap-1" style={{ minWidth: '160px' }}>
                    <span className="text-nkz-xs w-20 truncate">{dimLabel(d)}</span>
                    <input
                      type="range"
                      min="0"
                      max="100"
                      value={p[d] || 0}
                      onChange={e => setPriorities(prev => ({ ...prev, [yr]: { ...prev[yr], [d]: Number(e.target.value) } }))}
                      className="flex-1 h-2"
                      disabled={isLocked}
                    />
                    <span className="text-nkz-xs w-8 text-right">{p[d] || 0}%</span>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </Card>

      <Button size="lg" onClick={handleOptimize} disabled={optLoading || !parcelId} loading={optLoading}>
        🔄 {t('rotationOptimizer.optimize', { defaultValue: 'Optimizar' })}
      </Button>

      {optError && <div className="text-nkz-error text-nkz-sm">{optError}</div>}

      {optResult?.plan && (
        <>
          {optResult.plan.map((year: any) => (
            <Card key={year.year} padding="md">
              <div className="flex items-center gap-2 mb-2 flex-wrap">
                <Badge intent="info">{t('rotationOptimizer.year', { n: year.year, defaultValue: `Año ${year.year}` })}</Badge>
                {year.locked && <Badge intent="warning">🔒 {t('rotationOptimizer.lockedYear', { defaultValue: 'Bloqueado' })}</Badge>}
                {year.scores && (
                  <span className="text-nkz-xs text-nkz-text-muted">
                    {t('rotationOptimizer.scores', { defaultValue: 'Puntuación' })}: {year.scores.composite}
                  </span>
                )}
              </div>
              <div className="grid grid-cols-2 gap-2 text-nkz-sm">
                <div>
                  <div className="text-nkz-xs text-nkz-text-muted">{t('rotationOptimizer.coverCrop', { defaultValue: 'Cobertura' })}</div>
                  <div className="font-medium">
                    🌱 {year.cover_crop?.name || '—'}
                    {year.cover_crop?.termination_method === 'roller_crimper' ? ' 🔄' : ''}
                  </div>
                </div>
                <div>
                  <div className="text-nkz-xs text-nkz-text-muted">{t('rotationOptimizer.cashCrop', { defaultValue: 'Cultivo' })}</div>
                  <div className="font-medium">
                    🌾 {year.cash_crop?.name} · {year.cash_crop?.expected_yield_kg_ha?.toLocaleString()} kg/ha
                  </div>
                  <div className="text-nkz-xs text-nkz-text-secondary">
                    🧪 {year.cash_crop?.protein_kg_ha} {t('rotationOptimizer.proteinUnit', { defaultValue: 'kg prot' })} ·
                    🌍 {year.cash_crop?.carbon_fixed_tco2e_ha} tCO₂e ·
                    💰 {year.cash_crop?.net_margin_eur_ha?.toFixed(0)} €/ha
                  </div>
                  {year.cash_crop?.protein_source && (
                    <div className="text-nkz-xs text-nkz-text-muted">
                      {t('rotationOptimizer.proteinSource', { source: year.cash_crop.protein_source, defaultValue: year.cash_crop.protein_source })}
                    </div>
                  )}
                </div>
              </div>
              {year.scores && !year.locked && (
                <div className="flex flex-wrap gap-2 mt-2 text-nkz-xs">
                  {OPT_DIMS.map(d => (
                    <Badge key={d} intent="info">{dimLabel(d)}: {year.scores[d === 'n_fixation' ? 'n_fixation' : d] ?? '—'}</Badge>
                  ))}
                </div>
              )}
              {year.alternatives?.length > 0 && (
                <div className="mt-2">
                  <Button variant="ghost" size="sm" onClick={() => setExpandedAlts(expandedAlts === year.year ? null : year.year)}>
                    {t('rotationOptimizer.alternatives', { defaultValue: 'Ver alternativas' })}
                  </Button>
                  {expandedAlts === year.year && (
                    <ul className="mt-1 text-nkz-xs text-nkz-text-secondary list-disc pl-4">
                      {year.alternatives.map((alt: any) => (
                        <li key={alt.eppo}>
                          {alt.eppo} ({alt.composite_score}) — {alt.reason}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              )}
              {onAssign && year.cash_crop && (
                <Button
                  variant="secondary"
                  size="sm"
                  className="mt-2"
                  onClick={() => onAssign({
                    name: year.cash_crop.variety || year.cash_crop.name,
                    crop_eppo: year.cash_crop.eppo,
                  })}
                >
                  {t('planning.assignCrop', { defaultValue: 'Adjudicar' })}
                </Button>
              )}
              {year.rotation_warning && (
                <div className="text-nkz-xs text-nkz-warning mt-1">⚠️ {year.rotation_warning}</div>
              )}
            </Card>
          ))}

          {optResult.cumulative && (
            <Card padding="md" className="bg-nkz-positive-soft">
              <strong className="text-nkz-sm">{t('rotationOptimizer.cumulative', { defaultValue: 'Acumulado' })}</strong>
              <div className="grid grid-cols-2 gap-2 mt-2 text-nkz-sm">
                <div>🧪 {optResult.cumulative.total_protein_kg_ha} {t('rotationOptimizer.totalProtein', { defaultValue: 'kg proteína' })}</div>
                <div>🌍 {optResult.cumulative.total_carbon_fixed_tco2e} tCO₂e</div>
                <div>🧪 {optResult.cumulative.total_n_fixation_kg_ha} {t('rotationOptimizer.totalNitrogen', { defaultValue: 'kg N fijado' })}</div>
                <div>💰 {optResult.cumulative.total_net_margin_eur_ha?.toLocaleString()} €/ha</div>
                <div>{t('rotationOptimizer.nPool', { defaultValue: 'N pool final' })}: {optResult.cumulative.final_soil_n_pool_kg_ha} kg/ha</div>
              </div>
            </Card>
          )}

          {optResult.pac_compliance && (
            <Card padding="md">
              <strong className="text-nkz-sm">🇪🇺 {t('rotationPlanner.pac.title', { defaultValue: 'Cumplimiento PAC' })}</strong>
              <div className="flex items-center gap-4 mt-2">
                <div className="text-nkz-xl font-bold">{optResult.pac_compliance.score}%</div>
                <ProgressBar
                  value={optResult.pac_compliance.score}
                  intent={optResult.pac_compliance.score >= 80 ? 'positive' : optResult.pac_compliance.score >= 50 ? 'warning' : 'negative'}
                  showLabel
                />
              </div>
              {optResult.pac_compliance.rules?.map((rule: any) => (
                <div key={rule.id} className="text-nkz-xs mt-1">
                  {rule.pass ? '✅' : '❌'} {t(`rotationPlanner.pac.rule.${rule.id}`, { defaultValue: rule.id })} — {rule.detail}
                </div>
              ))}
            </Card>
          )}
        </>
      )}
    </>
  );
};

export default CropPlanner;
