import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Stack, Spinner, Panel, DetailGrid, DetailItem, Surface } from '@nekazari/ui-kit';
import { Sprout, Thermometer, Beaker, BookOpen } from 'lucide-react';
import { useBioApi } from '../services/api';
import PhenologyContribute from './PhenologyContribute';

interface PhenologyParams {
  species: string; scientific_name?: string; stage: string; stage_description?: string;
  kc: number; d1: number; d2: number; mds_ref: number;
  kc_confidence_interval?: [number, number]; d1_confidence_interval?: [number, number];
  d2_confidence_interval?: [number, number]; mds_ref_confidence_interval?: [number, number];
  cultivar?: string; management?: string; match_level: string; is_default: boolean;
  provenance?: { doi?: string; short?: string; author?: string; year?: number; institution?: string; method?: string; conditions?: string; };
  alternatives?: Array<{ kc: number; sourceShort?: string; sourceDoi?: string; conditions?: string; }>;
}
interface SpeciesInfo { name: string; scientific_name?: string; stage_count: number; params_count: number; has_phenology: boolean; }

const MATCH_STYLES: Record<string, string> = {
  exact: 'border-nkz-success bg-nkz-success-soft', management: 'border-nkz-info bg-nkz-info-soft',
  generic: 'border-nkz-warning bg-nkz-warning-soft', species_only: 'border-nkz-danger bg-nkz-danger-soft',
};
const FALLBACK = ['olive', 'almond', 'grapevine', 'wheat'];
const STAGES = ['vegetative', 'pit_hardening', 'fruit_growth', 'kernel_fill', 'veraison', 'stem_elongation'];
const CULTIVARS = ['Picual', 'Nonpareil', 'Tempranillo'];
const MGMT = ['', 'deficit_irrigation', 'regulated_deficit_irrigation'];
const MGMT_LABELS: Record<string, string> = { '': 'Standard (full/rainfed)', deficit_irrigation: 'Deficit Irrigation', regulated_deficit_irrigation: 'Regulated Deficit (RDI)' };

const selectCls = "w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base";

const PhenologyBrowser: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [speciesList, setSpeciesList] = useState<SpeciesInfo[]>([]);
  const [species, setSpecies] = useState('olive');
  const [stage, setStage] = useState('');
  const [cultivar, setCultivar] = useState('');
  const [management, setManagement] = useState('');
  const [data, setData] = useState<PhenologyParams | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showContribute, setShowContribute] = useState(false);

  useEffect(() => { api.getSpecies().then((d: any) => { if (d) { const list = Array.isArray(d) ? d : (d?.species || []); setSpeciesList(list); } }).catch(() => {}); }, []);

  const fetchParams = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const params = new URLSearchParams({ species });
      if (stage) params.set('stage', stage);
      if (cultivar) params.set('cultivar', cultivar);
      if (management) params.set('management', management);
      setData(await api.getPhenologyParams(params));
    } catch (e: any) { setData(null); setError(e.message?.includes('404') ? t('phenology.notFound') : e.message); }
    finally { setLoading(false); }
  }, [species, stage, cultivar, management, t]);

  useEffect(() => { fetchParams(); }, [fetchParams]);

  const speciesOptions = speciesList.length > 0 ? speciesList : FALLBACK.map((n) => ({ name: n, scientific_name: undefined, stage_count: 0, params_count: 0, has_phenology: false }));
  const ml = data?.match_level || '';
  const paramAvailable = data ? {
    kc: data.kc != null && !isNaN(data.kc),
    d1: data.d1 != null && !isNaN(data.d1),
    d2: data.d2 != null && !isNaN(data.d2),
    mds: data.mds_ref != null && !isNaN(data.mds_ref),
  } : { kc: false, d1: false, d2: false, mds: false };
  const genericForLabel = data?.is_default ? ['Kc','D1','D2','MDS'].filter((_, i) => [paramAvailable.kc, paramAvailable.d1, paramAvailable.d2, paramAvailable.mds][i]).join(', ') : '';
  const sourceLabel = data?.is_default ? t('phenology.genericSource') : (data?.provenance?.short || '');

  return (
    <Stack gap="section">
      {/* Onboarding */}
      <div className="rounded-nkz-md bg-nkz-info-soft border border-nkz-info p-nkz-stack text-nkz-xs text-nkz-text-secondary">
        <strong className="text-nkz-text-primary">💡 Phenology:</strong>{' '}
        {t('onboarding.phenologyBrowser')}
      </div>

      <div className="flex flex-wrap gap-3 items-end">
        <div className="min-w-[160px]"><label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('phenology.species')}</label><select className={selectCls} value={species} onChange={(e) => setSpecies(e.target.value)}>{speciesOptions.map((s: any) => <option key={s.name} value={s.name}>{s.scientific_name ? `${s.name} (${s.scientific_name})` : s.name}{s.has_phenology ? ` — ${s.params_count} params` : ` — ${t('varietyFinder.noDataAvailable')}`}</option>)}</select></div>
        <div className="min-w-[140px]"><label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('phenology.stage')}</label><select className={selectCls} value={stage} onChange={(e) => setStage(e.target.value)}><option value="">{t('phenology.anyStage')}</option>{STAGES.map((s) => <option key={s} value={s}>{s}</option>)}</select></div>
        <div className="min-w-[140px]"><label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('phenology.cultivar')}</label><select className={selectCls} value={cultivar} onChange={(e) => setCultivar(e.target.value)}><option value="">{t('phenology.anyCultivar')}</option>{CULTIVARS.map((c) => <option key={c} value={c}>{c}</option>)}</select></div>
        <div className="min-w-[160px]"><label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('phenology.management')}</label><select className={selectCls} value={management} onChange={(e) => setManagement(e.target.value)}>{MGMT.map((m) => <option key={m} value={m}>{MGMT_LABELS[m] || m || t('phenology.standardIrrigation')}</option>)}</select></div>
      </div>

      <button className="inline-flex items-center gap-1.5 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack py-nkz-tight text-nkz-xs font-medium text-nkz-accent-base hover:bg-nkz-accent-soft transition-colors duration-nkz-fast" onClick={() => setShowContribute(true)}><BookOpen className="w-3.5 h-3.5" />{t('phenology.contribute.button')}</button>

      {loading && <div className="flex justify-center py-nkz-section"><Spinner size="md" /></div>}
      {error && <div className="text-center py-nkz-section"><Sprout className="w-8 h-8 text-nkz-text-muted mx-auto mb-2" /><p className="text-nkz-sm">{error}</p></div>}
      {showContribute && <PhenologyContribute onClose={() => setShowContribute(false)} />}

      {data && (
        <Stack gap="section">
          <div className={`rounded-nkz-md border-l-4 p-nkz-stack ${MATCH_STYLES[ml] || 'border-nkz-border bg-nkz-surface-sunken'}`}>
            <div className="flex items-center gap-2">
              <Badge intent={ml === 'exact' ? 'positive' : ml === 'generic' ? 'warning' : 'info'}>{ml.toUpperCase() || 'UNKNOWN'}</Badge>
              <span className="text-nkz-sm text-nkz-text-primary">{data.scientific_name && <em>{data.scientific_name}</em>}{data.stage && ` — ${data.stage}`}</span>
            </div>
          </div>
          {data.is_default && <div className="text-nkz-xs text-nkz-text-muted bg-nkz-surface-sunken rounded-nkz-md p-nkz-inline">{t('phenology.usingDefaultsDetail', { params: genericForLabel })}</div>}

          <Panel>
            <Panel.Header><Panel.Title><Sprout className="w-4 h-4 text-nkz-accent-base" />Parameters</Panel.Title></Panel.Header>
            <Panel.Body>
              <DetailGrid columns={2}>
                <DetailItem label="Kc" value={<>{data.kc?.toFixed(2) || '—'} <Badge intent={paramAvailable.kc ? 'positive' : 'default'}>{paramAvailable.kc ? '✓' : '✗'}</Badge>{sourceLabel && <span className="text-nkz-text-muted text-nkz-xs ml-1">{sourceLabel}</span>}</>} />
                <DetailItem label="D1 (NWSB)" value={<>{data.d1 != null ? `${data.d1?.toFixed(1)}°C` : '—'} <Badge intent={paramAvailable.d1 ? 'positive' : 'default'}>{paramAvailable.d1 ? '✓' : '✗'}</Badge>{sourceLabel && <span className="text-nkz-text-muted text-nkz-xs ml-1">{sourceLabel}</span>}</>} />
                <DetailItem label="D2 (Max Stress)" value={<>{data.d2 != null ? `${data.d2?.toFixed(1)}°C` : '—'} <Badge intent={paramAvailable.d2 ? 'positive' : 'default'}>{paramAvailable.d2 ? '✓' : '✗'}</Badge>{sourceLabel && <span className="text-nkz-text-muted text-nkz-xs ml-1">{sourceLabel}</span>}</>} />
                <DetailItem label="MDS Ref" value={<>{data.mds_ref != null ? `${data.mds_ref?.toFixed(0)}µm` : '—'} <Badge intent={paramAvailable.mds ? 'positive' : 'default'}>{paramAvailable.mds ? '✓' : '✗'}</Badge>{sourceLabel && <span className="text-nkz-text-muted text-nkz-xs ml-1">{sourceLabel}</span>}</>} />
              </DetailGrid>
            </Panel.Body>
          </Panel>

          {data.provenance && (
            <Surface variant="sunken" padding="stack">
              <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider mb-1">{t('phenology.source')}</h4>
              <p className="text-nkz-sm text-nkz-text-primary"><strong>{data.provenance.short}</strong>{data.provenance.author && ` — ${data.provenance.author}`}{data.provenance.year && ` (${data.provenance.year})`}</p>
              {data.provenance.institution && <p className="text-nkz-xs text-nkz-text-secondary">{data.provenance.institution}</p>}
              {data.provenance.doi && <p className="text-nkz-xs">DOI: <a href={`https://doi.org/${data.provenance.doi}`} target="_blank" rel="noopener" className="text-nkz-accent-base hover:underline">{data.provenance.doi}</a></p>}
            </Surface>
          )}

          {(data.alternatives || []).length > 0 && (
            <Stack gap="tight">
              <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider">{t('phenology.alternatives')}</h4>
              {(data.alternatives || []).map((alt, i) => (
                <Card key={i} padding="sm"><span className="font-medium text-nkz-sm">Kc = {alt.kc?.toFixed(2)}</span>{alt.sourceShort && <span className="text-nkz-text-muted ml-2">— {alt.sourceShort}</span>}</Card>
              ))}
            </Stack>
          )}

          <HeatToleranceSection species={species} t={t} />
          <NutrientProfileSection species={species} stage={data.stage} t={t} />
        </Stack>
      )}
    </Stack>
  );
};

const HeatToleranceSection: React.FC<{ species: string; t: any }> = ({ species, t }) => {
  const api = useBioApi(); const [ht, setHt] = useState<any>(null);
  useEffect(() => { api.getHeatTolerance(species).then(setHt).catch(() => {}); }, [species]);
  if (!ht) return null;
  return (
    <Panel>
      <Panel.Header><Panel.Title><Thermometer className="w-4 h-4 text-nkz-accent-base" />{t('phenology.thermal') || 'Thermal Tolerance'}</Panel.Title></Panel.Header>
      <Panel.Body><DetailGrid columns={2}><DetailItem label="Heat damage" value={<>&gt; {ht.heat_damage_c}&deg;C (foliar)</>} /><DetailItem label="Frost damage" value={<>&lt; {ht.frost_damage_c}&deg;C (air)</>} /><DetailItem label="Accumulation" value={<>{ht.heat_accum_hours}h to alert</>} /></DetailGrid></Panel.Body>
    </Panel>
  );
};

const NutrientProfileSection: React.FC<{ species: string; stage?: string; t: any }> = ({ species, stage, t }) => {
  const api = useBioApi(); const [items, setItems] = useState<any[]>([]);
  useEffect(() => { api.getNutrientProfile(species, stage).then((d: any) => setItems(Array.isArray(d) ? d : [])).catch(() => {}); }, [species, stage]);
  if (!items.length) return null;
  return (
    <Panel>
      <Panel.Header><Panel.Title><Beaker className="w-4 h-4 text-nkz-accent-base" />{t('phenology.nutrients') || 'Nutrient Uptake'}</Panel.Title></Panel.Header>
      <Panel.Body>
        <table className="w-full text-nkz-xs"><thead><tr className="text-nkz-text-muted text-left"><th className="pb-1 pr-2">Nutrient</th><th className="pb-1 pr-2">Stage</th><th className="pb-1">kg/ha/day</th></tr></thead><tbody>{items.map((d: any, i: number) => (<tr key={i} className="border-t border-nkz-border"><td className="py-1 pr-2 text-nkz-text-primary font-medium">{d.element?.toUpperCase()}</td><td className="py-1 pr-2 text-nkz-text-muted">{d.stage}</td><td className="py-1 text-nkz-text-primary">{d.n_uptake || d.p_uptake || d.k_uptake || '—'}</td></tr>))}</tbody></table>
      </Panel.Body>
    </Panel>
  );
};

export default PhenologyBrowser;
