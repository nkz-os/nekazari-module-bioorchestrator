import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Button, Stack, EmptyState, Skeleton, Select } from '@nekazari/ui-kit';
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
  { value: 'any', key: 'Cualquiera' },
  { value: 'conventional', key: 'Convencional' },
  { value: 'organic', key: 'Ecológico' },
];

const CropPlanner: React.FC = () => {
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
  const [view, setView] = useState<ViewState>('idle');
  const [result, setResult] = useState<SuggestResult | null>(null);
  const [error, setError] = useState('');
  const [envBadge, setEnvBadge] = useState<string>('');

  // Assign modal
  const [assignVariety, setAssignVariety] = useState<any>(null);
  const [assignedMessage, setAssignedMessage] = useState('');
  const [expandedEppo, setExpandedEppo] = useState<string | null>(null);

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

      {/* Filters */}
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
              options={MANAGEMENT_OPTIONS.map(o => ({ value: o.value, label: o.key }))}
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
            🌾 Sin gluten solamente
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
                          onClick={() => setExpandedEppo(expandedEppo === s.crop_eppo ? null : s.crop_eppo)}>
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
          <div className="flex items-center gap-2 text-nkz-success">
            <CheckCircle className="w-5 h-5" />
            <span className="font-medium">{assignedMessage}</span>
          </div>
        </Card>
      )}
    </Stack>
  );
};

export default CropPlanner;
