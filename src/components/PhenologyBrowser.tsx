import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Stack, Spinner } from '@nekazari/ui-kit';
import { Sprout, Thermometer, Beaker, BookOpen } from 'lucide-react';
import { useBioApi } from '../services/api';
import PhenologyContribute from './PhenologyContribute';

interface PhenologyParams {
  species: string;
  scientific_name?: string;
  stage: string;
  kc: number;
  d1: number;
  d2: number;
  mds_ref: number;
  kc_confidence_interval?: [number, number];
  d1_confidence_interval?: [number, number];
  d2_confidence_interval?: [number, number];
  mds_ref_confidence_interval?: [number, number];
  cultivar?: string;
  management?: string;
  match_level: string;
  is_default: boolean;
  provenance?: { doi?: string; short?: string; author?: string; year?: number; institution?: string; method?: string; conditions?: string; };
  alternatives?: Array<{ kc: number; sourceShort?: string; sourceDoi?: string; conditions?: string; }>;
  stage_description?: string;
  climate_zone?: string;
}

interface SpeciesInfo { name: string; scientific_name?: string; stage_count: number; params_count: number; has_phenology: boolean; }

const MATCH_STYLES: Record<string, string> = {
  exact: 'border-nkz-success bg-nkz-success-soft',
  management: 'border-nkz-info bg-nkz-info-soft',
  generic: 'border-nkz-warning bg-nkz-warning-soft',
  species_only: 'border-nkz-danger bg-nkz-danger-soft',
};

const FALLBACK_SPECIES = ['olive', 'almond', 'grapevine', 'wheat'];

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

  useEffect(() => {
    api.getSpecies().then((d: any) => { if (Array.isArray(d)) setSpeciesList(d); }).catch(() => {});
  }, []);

  const fetchParams = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({ species });
      if (stage) params.set('stage', stage);
      if (cultivar) params.set('cultivar', cultivar);
      if (management) params.set('management', management);
      const result = await api.getPhenologyParams(params);
      setData(result);
    } catch (e: any) {
      setData(null);
      setError(e.message?.includes('404') ? t('phenology.notFound') : e.message);
    } finally { setLoading(false); }
  }, [species, stage, cultivar, management, t]);

  useEffect(() => { fetchParams(); }, [fetchParams]);

  const speciesOptions = speciesList.length > 0 ? speciesList : FALLBACK_SPECIES.map((n) => ({
    name: n, scientific_name: undefined as string | undefined, stage_count: 0, params_count: 0, has_phenology: false,
  }));

  const ml = data?.match_level || '';

  return (
    <Stack gap="section">
      {/* Controls */}
      <div className="flex flex-wrap gap-3 items-end">
        <div className="min-w-[160px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('phenology.species')}</label>
          <select className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm" value={species} onChange={(e) => setSpecies(e.target.value)}>
            {speciesOptions.map((s) => (
              <option key={s.name} value={s.name}>{s.scientific_name ? `${s.name} (${s.scientific_name})` : s.name}{!s.has_phenology ? ' *' : ''}</option>
            ))}
          </select>
        </div>
        <div className="min-w-[140px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('phenology.stage')}</label>
          <select className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm" value={stage} onChange={(e) => setStage(e.target.value)}>
            <option value="">{t('phenology.anyStage')}</option>
            <option value="vegetative">Vegetative</option>
            <option value="pit_hardening">Pit Hardening</option>
            <option value="fruit_growth">Fruit Growth</option>
            <option value="kernel_fill">Kernel Fill</option>
            <option value="veraison">Veraison</option>
            <option value="stem_elongation">Stem Elongation</option>
          </select>
        </div>
        <div className="min-w-[140px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('phenology.cultivar')}</label>
          <select className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm" value={cultivar} onChange={(e) => setCultivar(e.target.value)}>
            <option value="">{t('phenology.anyCultivar')}</option>
            <option value="Picual">Picual</option>
            <option value="Nonpareil">Nonpareil</option>
            <option value="Tempranillo">Tempranillo</option>
          </select>
        </div>
        <div className="min-w-[160px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('phenology.management')}</label>
          <select className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm" value={management} onChange={(e) => setManagement(e.target.value)}>
            <option value="">{t('phenology.standardIrrigation')}</option>
            <option value="deficit_irrigation">Deficit Irrigation</option>
            <option value="regulated_deficit_irrigation">Regulated Deficit (RDI)</option>
          </select>
        </div>
      </div>

      <button
        className="inline-flex items-center gap-1.5 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack py-nkz-tight text-nkz-xs font-medium text-nkz-accent-base hover:bg-nkz-accent-soft transition-colors duration-nkz-fast"
        onClick={() => setShowContribute(true)}
      >
        <BookOpen className="w-3.5 h-3.5" />{t('phenology.contribute.button')}
      </button>

      {loading && <div className="flex items-center justify-center py-nkz-section"><Spinner size="md" /></div>}
      {error && <div className="text-center py-nkz-section"><Sprout className="w-8 h-8 text-nkz-text-muted mb-nkz-stack mx-auto" /><p className="text-nkz-sm text-nkz-text-primary">{error}</p></div>}
      {showContribute && <PhenologyContribute onClose={() => setShowContribute(false)} />}

      {data && (
        <Stack gap="section">
          <div className={`rounded-nkz-md border-l-4 p-nkz-stack ${MATCH_STYLES[ml] || 'border-nkz-border bg-nkz-surface-sunken'}`}>
            <div className="flex items-center gap-2">
              <Badge intent={ml === 'exact' ? 'positive' : ml === 'generic' ? 'warning' : 'info'}>{ml.toUpperCase() || 'UNKNOWN'}</Badge>
              <span className="text-nkz-sm text-nkz-text-primary">
                {data.scientific_name && <em>{data.scientific_name}</em>}{data.stage && ` — ${data.stage}`}{data.stage_description && ` (${data.stage_description})`}
              </span>
            </div>
          </div>
          {data.is_default && <div className="text-nkz-xs text-nkz-text-muted bg-nkz-surface-sunken rounded-nkz-md p-nkz-inline">{t('phenology.usingDefaults')}</div>}

          {/* Parameters */}
          <div className="bg-nkz-surface border border-nkz-border rounded-nkz-md p-nkz-stack divide-y divide-nkz-border text-nkz-sm">
            {[
              ['Kc', data.kc?.toFixed(2), data.kc_confidence_interval ? `${data.kc_confidence_interval[0]?.toFixed(2)} – ${data.kc_confidence_interval[1]?.toFixed(2)}` : '—'],
              ['D1 (NWSB)', `${data.d1?.toFixed(1)}°C`, data.d1_confidence_interval ? `${data.d1_confidence_interval[0]?.toFixed(1)} – ${data.d1_confidence_interval[1]?.toFixed(1)}` : '—'],
              ['D2 (Max Stress)', `${data.d2?.toFixed(1)}°C`, data.d2_confidence_interval ? `${data.d2_confidence_interval[0]?.toFixed(1)} – ${data.d2_confidence_interval[1]?.toFixed(1)}` : '—'],
              ['MDS Ref', `${data.mds_ref?.toFixed(0)}µm`, data.mds_ref_confidence_interval ? `${data.mds_ref_confidence_interval[0]?.toFixed(0)} – ${data.mds_ref_confidence_interval[1]?.toFixed(0)}` : '—'],
            ].map(([label, value, ci]) => (
              <div key={label} className="flex justify-between py-1">
                <span className="text-nkz-text-secondary">{label}</span>
                <span className="text-nkz-text-primary font-medium">{value} <span className="text-nkz-xs text-nkz-text-muted ml-2">{ci}</span></span>
              </div>
            ))}
          </div>

          {/* Provenance */}
          {data.provenance && (
            <div className="bg-nkz-surface-sunken rounded-nkz-md p-nkz-stack">
              <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider mb-1">{t('phenology.source')}</h4>
              <p className="text-nkz-sm text-nkz-text-primary"><strong>{data.provenance.short}</strong>{data.provenance.author && ` — ${data.provenance.author}`}{data.provenance.year && ` (${data.provenance.year})`}</p>
              {data.provenance.doi && <p className="text-nkz-xs">DOI: <a href={`https://doi.org/${data.provenance.doi}`} target="_blank" rel="noopener" className="text-nkz-accent-base hover:underline">{data.provenance.doi}</a></p>}
            </div>
          )}

          {/* Alternatives */}
          {(data.alternatives || []).length > 0 && (
            <div>
              <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider mb-2">{t('phenology.alternatives')}</h4>
              <Stack gap="tight">
                {(data.alternatives || []).map((alt, i) => (
                  <Card key={i} padding="sm">
                    <span className="font-medium text-nkz-sm">Kc = {alt.kc?.toFixed(2)}</span>
                    {alt.sourceShort && <span className="text-nkz-text-muted ml-2">— {alt.sourceShort}</span>}
                    {alt.sourceDoi && <a href={`https://doi.org/${alt.sourceDoi}`} target="_blank" rel="noopener" className="text-nkz-accent-base text-nkz-xs ml-2 hover:underline">DOI</a>}
                  </Card>
                ))}
              </Stack>
            </div>
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
    <div>
      <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5 mb-2">
        <Thermometer className="w-3.5 h-3.5 text-nkz-accent-base" />{t('phenology.thermal') || 'Thermal Tolerance'}
      </h4>
      <div className="bg-nkz-surface border border-nkz-border rounded-nkz-md p-nkz-stack divide-y divide-nkz-border text-nkz-sm">
        {[
          ['Heat damage', `> ${ht.heat_damage_c}°C (foliar)`],
          ['Frost damage', `< ${ht.frost_damage_c}°C (air)`],
          ['Accumulation', `${ht.heat_accum_hours}h to alert`],
        ].map(([label, value]) => (
          <div key={label} className="flex justify-between py-1"><span className="text-nkz-text-secondary">{label}</span><span className="text-nkz-text-primary font-medium">{value}</span></div>
        ))}
      </div>
    </div>
  );
};

const NutrientProfileSection: React.FC<{ species: string; stage?: string; t: any }> = ({ species, stage, t }) => {
  const api = useBioApi();
  const [items, setItems] = useState<any[]>([]);
  useEffect(() => {
    api.getNutrientProfile(species, stage).then((d: any) => setItems(Array.isArray(d) ? d : [])).catch(() => {});
  }, [species, stage]);
  if (!items.length) return null;
  return (
    <div>
      <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5 mb-2">
        <Beaker className="w-3.5 h-3.5 text-nkz-accent-base" />{t('phenology.nutrients') || 'Nutrient Uptake'}
      </h4>
      <table className="w-full text-nkz-xs">
        <thead><tr className="text-nkz-text-muted text-left"><th className="pb-1 pr-2">Nutrient</th><th className="pb-1 pr-2">Stage</th><th className="pb-1">kg/ha/day</th></tr></thead>
        <tbody>
          {items.map((d: any, i: number) => (
            <tr key={i} className="border-t border-nkz-border"><td className="py-1 pr-2 text-nkz-text-primary font-medium">{d.element?.toUpperCase()}</td><td className="py-1 pr-2 text-nkz-text-muted">{d.stage}</td><td className="py-1 text-nkz-text-primary">{d.n_uptake || d.p_uptake || d.k_uptake || '—'}</td></tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default PhenologyBrowser;
