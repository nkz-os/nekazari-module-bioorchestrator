import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Panel, Stack, Surface, Card, Badge, Spinner } from '@nekazari/ui-kit';
import { Sprout, Thermometer, Beaker, BookOpen } from 'lucide-react';
import { useBioApi } from '../services/api';
import PhenologyContribute from './PhenologyContribute';

interface PhenologyParams {
  species: string;
  scientific_name?: string;
  stage: string;
  stage_description?: string;
  kc: number;
  kc_confidence_interval?: [number, number];
  d1: number;
  d1_confidence_interval?: [number, number];
  d2: number;
  d2_confidence_interval?: [number, number];
  mds_ref: number;
  mds_ref_confidence_interval?: [number, number];
  cultivar?: string;
  management?: string;
  climate_zone?: string;
  match_level: string;
  is_default: boolean;
  provenance?: {
    doi?: string;
    short?: string;
    author?: string;
    year?: number;
    institution?: string;
    method?: string;
    conditions?: string;
  };
  alternatives?: Array<{
    kc: number;
    sourceShort?: string;
    sourceDoi?: string;
    conditions?: string;
  }>;
}

interface SpeciesInfo {
  name: string;
  scientific_name?: string;
  stage_count: number;
  params_count: number;
  has_phenology: boolean;
}

const MATCH_STYLES: Record<string, string> = {
  exact: 'border-nkz-success bg-nkz-success-soft',
  management: 'border-nkz-info bg-nkz-info-soft',
  generic: 'border-nkz-warning bg-nkz-warning-soft',
  species_only: 'border-nkz-danger bg-nkz-danger-soft',
};

const FALLBACK_SPECIES = [
  'olive', 'almond', 'grapevine', 'wheat',
];

// ── Inline detail grid (replaces DetailGrid/DetailItem) ─────────────────

const DetailRow: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
  <div className="flex items-baseline justify-between py-1">
    <span className="text-nkz-xs text-nkz-text-secondary font-medium uppercase tracking-wider">
      {label}
    </span>
    <span className="text-nkz-sm text-nkz-text-primary font-medium">
      {value}
    </span>
  </div>
);

const DetailCard: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div className="bg-nkz-surface border border-nkz-border rounded-nkz-md p-nkz-stack">
    <div className="divide-y divide-nkz-border">{children}</div>
  </div>
);

// ── Main component ──────────────────────────────────────────────────────

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
    api.getSpecies()
      .then(setSpeciesList)
      .catch(() => {});
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
      if (e.message?.includes('404')) {
        setData(null);
        setError(t('phenology.notFound'));
      } else {
        setError(e.message);
        setData(null);
      }
    } finally {
      setLoading(false);
    }
  }, [species, stage, cultivar, management, t]);

  useEffect(() => {
    fetchParams();
  }, [fetchParams]);

  const matchBorderClass = data ? (MATCH_STYLES[data.match_level] || 'border-nkz-border bg-nkz-surface-sunken') : '';

  return (
    <Stack gap="section">
      {/* Controls */}
      <div className="flex flex-wrap gap-3 items-end">
        <div className="min-w-[160px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
            {t('phenology.species')}
          </label>
          <select
            className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
            value={species}
            onChange={(e) => setSpecies(e.target.value)}
          >
            {(speciesList.length > 0 ? speciesList : FALLBACK_SPECIES.map((n) => ({ name: n, scientific_name: undefined as string | undefined, stage_count: 0, params_count: 0, has_phenology: false }))).map((s) => (
              <option key={s.name} value={s.name}>
                {s.scientific_name ? `${s.name} (${s.scientific_name})` : s.name}
                {!s.has_phenology ? ' *' : ''}
              </option>
            ))}
          </select>
        </div>

        <div className="min-w-[140px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
            {t('phenology.stage')}
          </label>
          <select
            className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
            value={stage}
            onChange={(e) => setStage(e.target.value)}
          >
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
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
            {t('phenology.cultivar')}
          </label>
          <select
            className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
            value={cultivar}
            onChange={(e) => setCultivar(e.target.value)}
          >
            <option value="">{t('phenology.anyCultivar')}</option>
            <option value="Picual">Picual</option>
            <option value="Nonpareil">Nonpareil</option>
            <option value="Tempranillo">Tempranillo</option>
          </select>
        </div>

        <div className="min-w-[160px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
            {t('phenology.management')}
          </label>
          <select
            className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
            value={management}
            onChange={(e) => setManagement(e.target.value)}
          >
            <option value="">{t('phenology.standardIrrigation')}</option>
            <option value="deficit_irrigation">Deficit Irrigation</option>
            <option value="regulated_deficit_irrigation">Regulated Deficit (RDI)</option>
          </select>
        </div>
      </div>

      {/* Contribute button */}
      <div>
        <button
          className="inline-flex items-center gap-1.5 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack py-nkz-tight text-nkz-xs font-medium text-nkz-accent-base hover:bg-nkz-accent-soft transition-colors duration-nkz-fast"
          onClick={() => setShowContribute(true)}
        >
          <BookOpen className="w-3.5 h-3.5" />
          {t('phenology.contribute.button')}
        </button>
      </div>

      {loading && (
        <div className="flex items-center justify-center py-nkz-section">
          <Spinner size="md" />
        </div>
      )}

      {error && (
        <div className="flex flex-col items-center justify-center py-nkz-section text-center">
          <Sprout className="w-8 h-8 text-nkz-text-muted mb-nkz-stack" />
          <p className="text-nkz-sm text-nkz-text-primary font-medium">{error}</p>
        </div>
      )}

      {showContribute && <PhenologyContribute onClose={() => setShowContribute(false)} />}

      {data && (
        <Stack gap="section">
          {/* Match banner */}
          <div className={`rounded-nkz-md border-l-4 p-nkz-stack ${matchBorderClass}`}>
            <div className="flex items-center gap-2">
              <Badge intent={data.match_level === 'exact' ? 'positive' : data.match_level === 'generic' ? 'warning' : 'info'}>
                {data.match_level.toUpperCase()}
              </Badge>
              <span className="text-nkz-sm text-nkz-text-primary">
                {data.scientific_name && <em>{data.scientific_name}</em>}
                {data.stage && ` — ${data.stage}`}
                {data.stage_description && ` (${data.stage_description})`}
              </span>
            </div>
          </div>

          {data.is_default && (
            <div className="text-nkz-xs text-nkz-text-muted bg-nkz-surface-sunken rounded-nkz-md p-nkz-inline">
              {t('phenology.usingDefaults')}
            </div>
          )}

          {/* Parameters */}
          <DetailCard>
            <DetailRow label="Kc" value={data.kc?.toFixed(2)} />
            <DetailRow
              label={t('phenology.ci')}
              value={data.kc_confidence_interval
                ? `${data.kc_confidence_interval[0]?.toFixed(2)} – ${data.kc_confidence_interval[1]?.toFixed(2)}`
                : '—'}
            />
            <DetailRow label="D1 (NWSB)" value={<>{data.d1?.toFixed(1)}&deg;C</>} />
            <DetailRow
              label={t('phenology.ci')}
              value={data.d1_confidence_interval
                ? `${data.d1_confidence_interval[0]?.toFixed(1)} – ${data.d1_confidence_interval[1]?.toFixed(1)}`
                : '—'}
            />
            <DetailRow label="D2 (Max Stress)" value={<>{data.d2?.toFixed(1)}&deg;C</>} />
            <DetailRow
              label={t('phenology.ci')}
              value={data.d2_confidence_interval
                ? `${data.d2_confidence_interval[0]?.toFixed(1)} – ${data.d2_confidence_interval[1]?.toFixed(1)}`
                : '—'}
            />
            <DetailRow label="MDS Ref" value={<>{data.mds_ref?.toFixed(0)}&micro;m</>} />
            <DetailRow
              label={t('phenology.ci')}
              value={data.mds_ref_confidence_interval
                ? `${data.mds_ref_confidence_interval[0]?.toFixed(0)} – ${data.mds_ref_confidence_interval[1]?.toFixed(0)}`
                : '—'}
            />
          </DetailCard>

          {/* Provenance */}
          {data.provenance && (
            <Surface variant="sunken" padding="stack">
              <Stack gap="tight">
                <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider">
                  {t('phenology.source')}
                </h4>
                <p className="text-nkz-sm text-nkz-text-primary">
                  <strong>{data.provenance.short}</strong>
                  {data.provenance.author && ` — ${data.provenance.author}`}
                  {data.provenance.year && ` (${data.provenance.year})`}
                </p>
                {data.provenance.institution && (
                  <p className="text-nkz-xs text-nkz-text-secondary">{data.provenance.institution}</p>
                )}
                {data.provenance.doi && (
                  <p className="text-nkz-xs">
                    DOI:{' '}
                    <a
                      href={`https://doi.org/${data.provenance.doi}`}
                      target="_blank"
                      rel="noopener"
                      className="text-nkz-accent-base hover:underline"
                    >
                      {data.provenance.doi}
                    </a>
                  </p>
                )}
                {data.provenance.method && (
                  <p className="text-nkz-xs text-nkz-text-muted">{data.provenance.method}</p>
                )}
                {data.provenance.conditions && (
                  <p className="text-nkz-xs text-nkz-text-muted">{data.provenance.conditions}</p>
                )}
              </Stack>
            </Surface>
          )}

          {/* Alternatives */}
          {(data.alternatives || []).length > 0 && (
            <Stack gap="tight">
              <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider">
                {t('phenology.alternatives')}
              </h4>
              {data.alternatives!.map((alt, i) => (
                <Card key={i} padding="sm">
                  <div className="flex items-center gap-2 text-nkz-sm">
                    <span className="font-medium text-nkz-text-primary">
                      Kc = {alt.kc?.toFixed(2)}
                    </span>
                    {alt.sourceShort && (
                      <span className="text-nkz-text-muted">— {alt.sourceShort}</span>
                    )}
                    {alt.sourceDoi && (
                      <a
                        href={`https://doi.org/${alt.sourceDoi}`}
                        target="_blank"
                        rel="noopener"
                        className="text-nkz-accent-base text-nkz-xs hover:underline"
                      >
                        DOI
                      </a>
                    )}
                  </div>
                </Card>
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

// ── Heat Tolerance ──────────────────────────────────────────────────────

const HeatToleranceSection: React.FC<{ species: string; t: any }> = ({ species, t }) => {
  const api = useBioApi();
  const [data, setData] = useState<any>(null);

  useEffect(() => {
    api.getHeatTolerance(species)
      .then(setData)
      .catch(() => {});
  }, [species]);

  if (!data) return null;

  return (
    <Stack gap="tight">
      <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
        <Thermometer className="w-3.5 h-3.5 text-nkz-accent-base" />
        {t('phenology.thermal') || 'Thermal Tolerance'}
      </h4>
      <DetailCard>
        <DetailRow label="Heat damage" value={<>&gt; {data.heat_damage_c}&deg;C (foliar)</>} />
        <DetailRow label="Frost damage" value={<>&lt; {data.frost_damage_c}&deg;C (air)</>} />
        <DetailRow label="Accumulation" value={<>{data.heat_accum_hours}h to alert</>} />
      </DetailCard>
      {data.source_short && (
        <p className="text-nkz-xs text-nkz-text-muted">
          {data.source_short}{data.source_doi ? ` · DOI: ${data.source_doi}` : ''}
        </p>
      )}
    </Stack>
  );
};

// ── Nutrient Profile ────────────────────────────────────────────────────

const NutrientProfileSection: React.FC<{ species: string; stage?: string; t: any }> = ({ species, stage, t }) => {
  const api = useBioApi();
  const [data, setData] = useState<any[]>([]);

  useEffect(() => {
    api.getNutrientProfile(species, stage)
      .then((d: any) => setData(Array.isArray(d) ? d : []))
      .catch(() => {});
  }, [species, stage]);

  if (!data.length) return null;

  return (
    <Stack gap="tight">
      <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
        <Beaker className="w-3.5 h-3.5 text-nkz-accent-base" />
        {t('phenology.nutrients') || 'Nutrient Uptake'}
      </h4>
      <table className="w-full text-nkz-xs">
        <thead>
          <tr className="text-nkz-text-muted text-left">
            <th className="pb-1 pr-2">Nutrient</th>
            <th className="pb-1 pr-2">Stage</th>
            <th className="pb-1">kg/ha/day</th>
          </tr>
        </thead>
        <tbody>
          {data.map((d: any, i: number) => (
            <tr key={i} className="border-t border-nkz-border">
              <td className="py-1 pr-2 text-nkz-text-primary font-medium">
                {d.element?.toUpperCase()}
              </td>
              <td className="py-1 pr-2 text-nkz-text-muted">{d.stage}</td>
              <td className="py-1 text-nkz-text-primary">
                {d.n_uptake || d.p_uptake || d.k_uptake || '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Stack>
  );
};

export default PhenologyBrowser;
