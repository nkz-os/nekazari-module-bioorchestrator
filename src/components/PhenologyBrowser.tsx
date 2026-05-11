import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Panel, Stack, Surface, DetailGrid, DetailItem, Card, Badge, Skeleton, EmptyState, Spinner } from '@nekazari/ui-kit';
import { Sprout, Thermometer, Beaker, BookOpen } from 'lucide-react';
import { useBioApi } from '../services/api';
import PhenologyContribute from './PhenologyContribute';

interface PhenologyParams {
  species: string; scientific_name?: string; stage: string; stage_description?: string;
  kc: number; d1: number; d2: number; mds_ref: number;
  kc_confidence_interval?: [number, number]; d1_confidence_interval?: [number, number];
  d2_confidence_interval?: [number, number]; mds_ref_confidence_interval?: [number, number];
  cultivar?: string; management?: string; climate_zone?: string; match_level: string; is_default: boolean;
  provenance?: { doi?: string; short?: string; author?: string; year?: number; institution?: string; method?: string; conditions?: string; };
  alternatives?: Array<{ kc: number; sourceShort?: string; sourceDoi?: string; conditions?: string; }>;
}
interface SpeciesInfo { name: string; scientific_name?: string; stage_count: number; params_count: number; has_phenology: boolean; }

const MATCH_STYLES: Record<string, string> = { exact: 'border-nkz-success bg-nkz-success-soft', management: 'border-nkz-info bg-nkz-info-soft', generic: 'border-nkz-warning bg-nkz-warning-soft', species_only: 'border-nkz-danger bg-nkz-danger-soft' };
const FALLBACK = ['olive', 'almond', 'grapevine', 'wheat'];

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

  useEffect(() => { api.getSpecies().then((d: any) => { if (Array.isArray(d)) setSpeciesList(d); }).catch(() => {}); }, []);

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

  const speciesOptions = speciesList.length > 0 ? speciesList : FALLBACK.map((n) => ({ name: n, scientific_name: undefined as string | undefined, stage_count: 0, params_count: 0, has_phenology: false }));
  const ml = data?.match_level || '';

  const selectClass = "w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm";

  return (
    <Stack gap="section">
      <div className="flex flex-wrap gap-3 items-end">
        {[
          { label: t('phenology.species'), value: species, set: setSpecies, options: speciesOptions.map((s: any) => ({ value: s.name, label: (s.scientific_name ? `${s.name} (${s.scientific_name})` : s.name) + (!s.has_phenology ? ' *' : '') })) },
          { label: t('phenology.stage'), value: stage, set: setStage, options: [{ value: '', label: t('phenology.anyStage') }, { value: 'vegetative', label: 'Vegetative' }, { value: 'pit_hardening', label: 'Pit Hardening' }, { value: 'fruit_growth', label: 'Fruit Growth' }, { value: 'kernel_fill', label: 'Kernel Fill' }, { value: 'veraison', label: 'Veraison' }, { value: 'stem_elongation', label: 'Stem Elongation' }] },
          { label: t('phenology.cultivar'), value: cultivar, set: setCultivar, options: [{ value: '', label: t('phenology.anyCultivar') }, { value: 'Picual', label: 'Picual' }, { value: 'Nonpareil', label: 'Nonpareil' }, { value: 'Tempranillo', label: 'Tempranillo' }] },
          { label: t('phenology.management'), value: management, set: setManagement, options: [{ value: '', label: t('phenology.standardIrrigation') }, { value: 'deficit_irrigation', label: 'Deficit Irrigation' }, { value: 'regulated_deficit_irrigation', label: 'Regulated Deficit (RDI)' }] },
        ].map((f) => (
          <div key={f.label} className="min-w-[140px]">
            <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{f.label}</label>
            <select className={selectClass} value={f.value} onChange={(e) => f.set(e.target.value)}>
              {f.options.map((o: any) => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>
        ))}
      </div>

      <button className="inline-flex items-center gap-1.5 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack py-nkz-tight text-nkz-xs font-medium text-nkz-accent-base hover:bg-nkz-accent-soft transition-colors duration-nkz-fast" onClick={() => setShowContribute(true)}>
        <BookOpen className="w-3.5 h-3.5" />{t('phenology.contribute.button')}
      </button>

      {loading && <div className="flex items-center justify-center py-nkz-section"><Spinner size="md" /></div>}
      {error && <EmptyState icon={<Sprout className="w-8 h-8 text-nkz-text-muted" />} title={error} />}
      {showContribute && <PhenologyContribute onClose={() => setShowContribute(false)} />}

      {data && (
        <Stack gap="section">
          <div className={`rounded-nkz-md border-l-4 p-nkz-stack ${MATCH_STYLES[ml] || 'border-nkz-border bg-nkz-surface-sunken'}`}>
            <div className="flex items-center gap-2">
              <Badge intent={ml === 'exact' ? 'positive' : ml === 'generic' ? 'warning' : 'info'}>{ml.toUpperCase() || 'UNKNOWN'}</Badge>
              <span className="text-nkz-sm text-nkz-text-primary">{data.scientific_name && <em>{data.scientific_name}</em>}{data.stage && ` — ${data.stage}`}</span>
            </div>
          </div>
          {data.is_default && <div className="text-nkz-xs text-nkz-text-muted bg-nkz-surface-sunken rounded-nkz-md p-nkz-inline">{t('phenology.usingDefaults')}</div>}

          <DetailGrid columns={2}>
            <DetailItem label="Kc" value={data.kc?.toFixed(2)} />
            <DetailItem label="D1 (NWSB)" value={<>{data.d1?.toFixed(1)}&deg;C</>} />
            <DetailItem label="D2 (Max Stress)" value={<>{data.d2?.toFixed(1)}&deg;C</>} />
            <DetailItem label="MDS Ref" value={<>{data.mds_ref?.toFixed(0)}&micro;m</>} />
          </DetailGrid>

          {data.provenance && (
            <Surface variant="sunken" padding="stack">
              <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider mb-1">{t('phenology.source')}</h4>
              <p className="text-nkz-sm text-nkz-text-primary"><strong>{data.provenance.short}</strong>{data.provenance.author && ` — ${data.provenance.author}`}</p>
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
  const api = useBioApi();
  const [ht, setHt] = useState<any>(null);
  useEffect(() => { api.getHeatTolerance(species).then(setHt).catch(() => {}); }, [species]);
  if (!ht) return null;
  return (
    <Stack gap="tight">
      <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Thermometer className="w-3.5 h-3.5 text-nkz-accent-base" />{t('phenology.thermal') || 'Thermal Tolerance'}</h4>
      <DetailGrid columns={2}>
        <DetailItem label="Heat damage" value={<>&gt; {ht.heat_damage_c}&deg;C (foliar)</>} />
        <DetailItem label="Frost damage" value={<>&lt; {ht.frost_damage_c}&deg;C (air)</>} />
        <DetailItem label="Accumulation" value={<>{ht.heat_accum_hours}h to alert</>} />
      </DetailGrid>
    </Stack>
  );
};

const NutrientProfileSection: React.FC<{ species: string; stage?: string; t: any }> = ({ species, stage, t }) => {
  const api = useBioApi();
  const [items, setItems] = useState<any[]>([]);
  useEffect(() => { api.getNutrientProfile(species, stage).then((d: any) => setItems(Array.isArray(d) ? d : [])).catch(() => {}); }, [species, stage]);
  if (!items.length) return null;
  return (
    <Stack gap="tight">
      <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Beaker className="w-3.5 h-3.5 text-nkz-accent-base" />{t('phenology.nutrients') || 'Nutrient Uptake'}</h4>
      <table className="w-full text-nkz-xs"><thead><tr className="text-nkz-text-muted text-left"><th className="pb-1 pr-2">Nutrient</th><th className="pb-1 pr-2">Stage</th><th className="pb-1">kg/ha/day</th></tr></thead>
        <tbody>{items.map((d: any, i: number) => (<tr key={i} className="border-t border-nkz-border"><td className="py-1 pr-2 text-nkz-text-primary font-medium">{d.element?.toUpperCase()}</td><td className="py-1 pr-2 text-nkz-text-muted">{d.stage}</td><td className="py-1 text-nkz-text-primary">{d.n_uptake || d.p_uptake || d.k_uptake || '—'}</td></tr>))}</tbody>
      </table>
    </Stack>
  );
};

export default PhenologyBrowser;
