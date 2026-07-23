import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Stack, Card, Badge, Button, DetailGrid, DetailItem, Skeleton } from '@nekazari/ui-kit';
import { SlotShell } from '@nekazari/viewer-kit';
import { useTranslation } from '@nekazari/sdk';
import { buildBioorchestratorToolUrl } from '../utils/navigation';
import { resolveParcelContext, type ParcelEntityData } from '../utils/entityData';
import { resolveCropTypeFromContext } from '../utils/cropContext';
import {
  RefreshCw, Globe, Thermometer, MapPin, Sprout, Bug, Beaker,
  AlertTriangle, CheckCircle, XCircle, ChevronDown, ChevronRight, Info,
} from 'lucide-react';
import { useBioApi, useCropApi, getCropContext } from '../services/api';
import type { VegetationData, SoilData as ParcelSoilData, SoilHorizon } from '../services/api';
import ContextEmptyState from './shared/ContextEmptyState';

const bioAccent = { base: '#14B8A6', soft: '#CCFBF1', strong: '#0D9488' };

interface RecCrop { name: string; scientific_name?: string; }
interface CropSoilReq { ph_min: number; ph_max: number; textures: string[]; drainage: string[]; depth_min_cm: number; salinity_max_ds_m: number; source_short?: string; }
interface Props { entityData?: ParcelEntityData; }

const PESTICIDE_INTENT: Record<string, 'positive' | 'negative' | 'warning'> = { approved: 'positive', not_approved: 'negative', withdrawn: 'warning' };

const RecommendationsPanel: React.FC<Props> = ({ entityData }) => {
  const { parcelId, parcelName, lat, lon } = resolveParcelContext(entityData);
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const navigate = useNavigate();
  const [cropType, setCropType] = useState<string | null>(null);
  const [cropContextLoading, setCropContextLoading] = useState(true);
  const [nextCrops, setNextCrops] = useState<RecCrop[]>([]);
  const [soil, setSoil] = useState<CropSoilReq | null>(null);
  const [realSoil, setRealSoil] = useState<any>(null);
  const [protectedArea, setProtectedArea] = useState<any>(null);
  const [varieties, setVarieties] = useState<any[]>([]);
  const [pesticides, setPesticides] = useState<any[]>([]);
  const [pollinators, setPollinators] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [dataGaps, setDataGaps] = useState<string[]>([]);
  const [dataAvail, setDataAvail] = useState<Record<string, boolean>>({});
  const [cropNotFound, setCropNotFound] = useState(false);
  const { getCropDetail } = useCropApi();

  // ── Vegetation / Soil (Plan C) ──────────────────────────────────────────
  const [vegIndex, setVegIndex] = useState('ndvi');
  const [vegPeriod, setVegPeriod] = useState('3m');
  const [vegData, setVegData] = useState<VegetationData | null>(null);
  const [vegLoading, setVegLoading] = useState(false);

  const [parcelSoil, setParcelSoil] = useState<ParcelSoilData | null>(null);
  const [soilLoading, setSoilLoading] = useState(false);

  // Resolve the real crop commitment (AgriParcel.hasAgriCrop) via
  // BioOrchestrator's own crop-context endpoint. NOT entityData.cropType —
  // that's the host's legacy free-text field, never updated by assign-crop,
  // so it would keep showing "no crop" after a real, successful assignment.
  useEffect(() => {
    if (!parcelId) { setCropType(null); setCropContextLoading(false); return; }
    let cancelled = false;
    setCropContextLoading(true);
    getCropContext(parcelId)
      .then((ctx) => { if (!cancelled) setCropType(resolveCropTypeFromContext(ctx)); })
      .catch(() => { if (!cancelled) setCropType(null); })
      .finally(() => { if (!cancelled) setCropContextLoading(false); });
    return () => { cancelled = true; };
  }, [parcelId]);

  useEffect(() => {
    if (!parcelId) return;
    let cancelled = false;
    setVegLoading(true);
    api.getParcelVegetation(parcelId, vegIndex, vegPeriod)
      .then(d => { if (!cancelled) setVegData(d); })
      .catch(() => { if (!cancelled) setVegData(null); })
      .finally(() => { if (!cancelled) setVegLoading(false); });
    return () => { cancelled = true; };
  }, [parcelId, vegIndex, vegPeriod]);

  useEffect(() => {
    if (!parcelId) return;
    let cancelled = false;
    setSoilLoading(true);
    api.getParcelSoil(parcelId)
      .then(d => { if (!cancelled) setParcelSoil(d); })
      .catch(() => { if (!cancelled) setParcelSoil(null); })
      .finally(() => { if (!cancelled) setSoilLoading(false); });
    return () => { cancelled = true; };
  }, [parcelId]);

  useEffect(() => {
    if (!cropType) return;
    setCropNotFound(false);
    (async () => {
      try {
        // Look up the crop URI from the species catalog first
        const species = await api.getSpecies();
        const match = Array.isArray(species)
          ? species.find((s: any) =>
              (s.name || '').toLowerCase() === cropType.toLowerCase() ||
              (s.scientificName || '').toLowerCase() === cropType.toLowerCase() ||
              (s.eppoCode || '').toLowerCase() === cropType.toLowerCase() ||
              (s.uri || '').endsWith(cropType.replace(/ /g, '_'))
            )
          : null;
        const cropId = match?.uri || cropType;
        const detail = await getCropDetail(cropId);
        if (detail?.data_available) {
          setDataAvail(detail.data_available);
          const gaps: string[] = [];
          if (!detail.data_available.kc) gaps.push('kc');
          if (!detail.data_available.d1_d2) gaps.push('d1_d2');
          if (!detail.data_available.thermal) gaps.push('thermal');
          if (!detail.data_available.npk) gaps.push('npk');
          setDataGaps(gaps);
        }
      } catch {
        setCropNotFound(true);
      }
    })();
  }, [cropType, api]);

  useEffect(() => {
    const crop = cropType;
    if (!crop) return;
    let cancelled = false;
    (async () => {
      setLoading(true);
      const safe = async <T,>(fn: () => Promise<T>): Promise<T | null> => { try { return await fn(); } catch { return null; } };
      const nc = await safe(() => api.getNextCrop(crop));
      const s = await safe(() => api.getSoilSuitability(crop));
      if (!cancelled) { setNextCrops(nc?.suggested_crops || []); if (s) setSoil(s); }
      if (lat != null && lon != null && !cancelled) {
        const [rs, pa, vars, pests, polls] = await Promise.all([
          safe(() => api.getSoilData(lat, lon)), safe(() => api.getProtectedArea(lat, lon)),
          safe(() => api.getVarieties(crop)), safe(() => api.getPesticides(crop)), safe(() => api.getPollinators(lat, lon)),
        ]);
        if (!cancelled) { if (rs) setRealSoil(rs); if (pa) setProtectedArea(pa); if (vars) setVarieties(vars.varieties || []); if (pests) setPesticides(pests.substances || []); if (polls) setPollinators(polls.pollinators || []); }
      }
      if (!cancelled) setLoading(false);
    })();
    return () => { cancelled = true; };
  }, [cropType, lat, lon]);

  if (cropContextLoading) {
    return (
      <SlotShell moduleId="bioorchestrator" title={t('panel.title')} icon={<RefreshCw className="w-4 h-4" />} accent={bioAccent}>
        <Stack gap="stack"><Skeleton variant="rect" height="60px" /></Stack>
      </SlotShell>
    );
  }

  // No-crop state
  if (!cropType) {
    return (
      <SlotShell moduleId="bioorchestrator" title={t('panel.title')} icon={<RefreshCw className="w-4 h-4" />} accent={bioAccent}>
        <ContextEmptyState
          message={t('panel.noCrop')}
          actionLabel={t('panel.assignCrop')}
          onAction={() => navigate(buildBioorchestratorToolUrl(parcelId, 'varietyFinder'))}
          variant="warning"
        />
      </SlotShell>
    );
  }

  if (loading) return (
    <SlotShell moduleId="bioorchestrator" title={t('panel.title')} icon={<RefreshCw className="w-4 h-4" />} accent={bioAccent}>
      <Stack gap="stack"><Skeleton variant="rect" height="60px" /><Skeleton variant="rect" height="80px" /><Skeleton variant="rect" height="80px" /></Stack>
    </SlotShell>
  );

  // Crop not in catalog
  if (cropNotFound) {
    return (
      <SlotShell moduleId="bioorchestrator" title={t('panel.title')} icon={<RefreshCw className="w-4 h-4" />} accent={bioAccent}>
        <ContextEmptyState
          message={t('panel.cropNotInCatalog', { crop: cropType })}
          actionLabel={t('panel.addToCatalog')}
          variant="warning"
        />
      </SlotShell>
    );
  }

  const dataKeys = ['kc', 'd1_d2', 'thermal', 'soil_suitability', 'npk', 'rotation'] as const;
  const completedCount = dataKeys.filter(k => dataAvail[k]).length;

  return (
    <SlotShell moduleId="bioorchestrator" title={t('panel.title')} icon={<RefreshCw className="w-4 h-4" />} accent={bioAccent}>
      <Stack gap="stack">
      {parcelName && <p className="text-nkz-sm text-nkz-text-secondary">{parcelName}</p>}

      {/* Section 1: Data Availability — always expanded */}
      <Card padding="md">
        <Stack gap="stack">
          <div className="flex justify-between items-center">
            <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider">{t('panel.dataAvailability')}</h4>
            <Badge intent={completedCount === dataKeys.length ? 'positive' : completedCount === 0 ? 'negative' : 'warning'}>
              {t('panel.complete', { n: completedCount, total: dataKeys.length })}
            </Badge>
          </div>
          <Stack gap="tight">
            <DataRow label="Kc" available={dataAvail.kc} />
            <DataRow label="D1/D2" available={dataAvail.d1_d2} />
            <DataRow label={t('thermal.title')} available={dataAvail.thermal} />
            <DataRow label={t('soil.title')} available={dataAvail.soil_suitability} />
            <DataRow label={t('npk.title')} available={dataAvail.npk} />
            <DataRow label={t('rotationPlanner.title')} available={dataAvail.rotation} />
          </Stack>
          {completedCount === 0 && (
            <div className="text-nkz-warning text-sm"><AlertTriangle size={14} className="inline mr-1" />{t('panel.none')}</div>
          )}
          {completedCount > 0 && completedCount < dataKeys.length && (
            <div className="text-nkz-warning text-sm"><AlertTriangle size={14} className="inline mr-1" />{t('panel.partial', { n: dataKeys.length - completedCount })}</div>
          )}
          {completedCount === dataKeys.length && (
            <div className="text-nkz-success text-sm"><CheckCircle size={14} className="inline mr-1" />{t('panel.allComplete')}</div>
          )}
        </Stack>
      </Card>

      {/* Section 2: Soil & Environment */}
      <CollapsibleSection title={t('panel.soilAndEnvironment')}>
        <Stack gap="stack">
          {/* New soil data from soil-module (replaces deprecated SoilGrids proxy) */}
          {parcelId && parcelSoil?.available && (
            <Card padding="md"><Stack gap="tight">
              <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Beaker className="w-3.5 h-3.5 text-nkz-accent-base" />{t('soilPanel.title')}</h4>
              <div className="grid gap-2" style={{ gridTemplateColumns: `repeat(${Math.min(parcelSoil.horizons.length, 3)}, 1fr)` }}>
                {parcelSoil.horizons.slice(0, 3).map((h: SoilHorizon, i: number) => (
                  <div key={i} className="border border-nkz-border rounded-nkz-md p-2 text-nkz-sm">
                    <div className="font-medium text-nkz-text-muted mb-1">{t('soilPanel.depthColumn', { from: h.depthFrom, to: h.depthTo })}</div>
                    {h.usdaTextureClass && <div className="mb-1">{h.usdaTextureClass}</div>}
                    {h.sand != null && <div>{t('soilPanel.sand')}: {h.sand.toFixed(0)}%</div>}
                    {h.silt != null && <div>{t('soilPanel.silt')}: {h.silt.toFixed(0)}%</div>}
                    {h.clay != null && <div>{t('soilPanel.clay')}: {h.clay.toFixed(0)}%</div>}
                    {h.organicCarbon != null && <div>{t('soilPanel.organicMatter')}: {(h.organicCarbon * 1.724).toFixed(1)}%</div>}
                    {h.ph != null && <div>{t('soilPanel.ph')}: {h.ph.toFixed(1)}</div>}
                    {h.cec != null && <div>{t('soilPanel.cec')}: {h.cec.toFixed(1)}</div>}
                    <div className="mt-1 text-nkz-text-muted text-xs">
                      {h.availableWaterCapacity != null && <div>{t('soilPanel.awc')}: {h.availableWaterCapacity.toFixed(2)}</div>}
                      {h.fieldCapacity != null && <div>{t('soilPanel.fc')}: {h.fieldCapacity.toFixed(2)}</div>}
                      {h.wiltingPoint != null && <div>{t('soilPanel.pwp')}: {h.wiltingPoint.toFixed(2)}</div>}
                    </div>
                  </div>
                ))}
              </div>
              {parcelSoil.hydrologicGroup && <p className="text-nkz-text-muted text-xs">{t('soilPanel.hydroGroup')}: {parcelSoil.hydrologicGroup}</p>}
              <p className="text-nkz-text-muted text-xs">{parcelSoil.source || t('soilPanel.source')}</p>
            </Stack></Card>
          )}
          {soil && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Globe className="w-3.5 h-3.5 text-nkz-accent-base" />Soil Requirements ({cropType})</h4><DetailGrid columns={2}><DetailItem label="pH" value={<>{soil.ph_min} – {soil.ph_max}</>} /><DetailItem label="Texture" value={(soil.textures || []).join(', ')} /><DetailItem label="Drainage" value={(soil.drainage || []).join(', ')} /></DetailGrid></Stack></Card>}
          {protectedArea?.in_protected_area && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><MapPin className="w-3.5 h-3.5 text-nkz-success" />Protected Area</h4><p className="text-nkz-sm font-medium">{protectedArea.site_name} ({protectedArea.site_code})</p></Stack></Card>}
          {lat != null && lon != null && <TerrainSection lat={lat} lon={lon} />}
          {lat != null && lon != null && <ClimateSection lat={lat} lon={lon} />}
        </Stack>
      </CollapsibleSection>

      {/* Vegetation Indices (Plan C) */}
      {parcelId && (
        <CollapsibleSection title={t('vegetation.title')} loading={vegLoading}>
          {!vegData?.available ? (
            <p className="text-nkz-sm text-nkz-text-muted">{vegData?.message || t('vegetation.noEntity')}</p>
          ) : (
            <Stack gap="tight">
              <div className="flex gap-2">
                <select className="h-8 rounded-nkz-md border border-nkz-border bg-nkz-surface text-nkz-sm" value={vegIndex} onChange={e => setVegIndex(e.target.value)}>
                  <option value="ndvi">NDVI</option>
                  <option value="evi">EVI</option>
                  <option value="savi">SAVI</option>
                  <option value="gndvi">GNDVI</option>
                  <option value="ndre">NDRE</option>
                  <option value="ndwi">NDWI</option>
                </select>
                <select className="h-8 rounded-nkz-md border border-nkz-border bg-nkz-surface text-nkz-sm" value={vegPeriod} onChange={e => setVegPeriod(e.target.value)}>
                  <option value="1m">{t('vegetation.periods.1m')}</option>
                  <option value="3m">{t('vegetation.periods.3m')}</option>
                  <option value="6m">{t('vegetation.periods.6m')}</option>
                  <option value="1y">{t('vegetation.periods.1y')}</option>
                  <option value="season">{t('vegetation.periods.season')}</option>
                </select>
              </div>
              {vegData.observations.length > 0 && (
                <Sparkline data={vegData.observations} width={280} height={70} />
              )}
              {vegData.current != null && (
                <DetailGrid columns={2}>
                  <DetailItem label={t('vegetation.current')} value={vegData.current.toFixed(4)} />
                  <DetailItem label={t('vegetation.trend')} value={vegData.trend?.label || '—'} />
                </DetailGrid>
              )}
              {vegData.count > 0 && vegData.count < 5 && (
                <div className="text-nkz-warning text-xs">
                  <AlertTriangle size={12} className="inline mr-1" />
                  {t('vegetation.lowCount', { count: vegData.count })}
                </div>
              )}
              <p className="text-nkz-text-muted text-xs">{t('vegetation.source')} · {t('vegetation.processor')}</p>
            </Stack>
          )}
        </CollapsibleSection>
      )}

      {/* Section 3: Rotation & Crop */}
      <CollapsibleSection title={t('panel.rotationAndCrop')}>
        <Stack gap="stack">
          <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><RefreshCw className="w-3.5 h-3.5 text-nkz-accent-base" />Rotation</h4><p className="text-nkz-sm">Current: <strong>{cropType}</strong></p>{nextCrops.length > 0 ? <div className="flex flex-wrap gap-1.5">{nextCrops.map((c) => <Badge key={c.name} intent="info">{c.scientific_name || c.name}</Badge>)}</div> : <p className="text-nkz-xs text-nkz-text-muted">No rotation restrictions.</p>}</Stack></Card>
          {varieties.length > 0 && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Sprout className="w-3.5 h-3.5 text-nkz-accent-base" />Registered Varieties (CPVO)</h4><div className="flex flex-wrap gap-1.5">{varieties.slice(0, 6).map((v: any, i: number) => <Badge key={i} intent="default">{v.variety_name || v.denomination}</Badge>)}</div></Stack></Card>}
          {pesticides.length > 0 && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Bug className="w-3.5 h-3.5 text-nkz-accent-base" />Authorised Pesticides (EU)</h4>{pesticides.slice(0, 5).map((p: any, i: number) => <div key={i} className="flex items-center justify-between text-nkz-sm"><Badge intent={PESTICIDE_INTENT[p.status] || 'default'}>{p.status}</Badge><span className="text-nkz-text-primary">{p.substance}</span></div>)}</Stack></Card>}
          {pollinators.length > 0 && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Sprout className="w-3.5 h-3.5 text-nkz-accent-base" />Pollinators (GBIF)</h4>{pollinators.slice(0, 5).map((p: any, i: number) => <div key={i} className="flex items-center justify-between text-nkz-sm"><span>{p.species}</span><span className="text-nkz-xs text-nkz-text-muted">{p.record_count} records</span></div>)}</Stack></Card>}
        </Stack>
      </CollapsibleSection>

      {/* Section 4: Scenario Simulator */}
      <CollapsibleSection title={t('panel.scenarioSimulator')}>
        <ScenarioSimulator currentCrop={cropType} />
      </CollapsibleSection>
      </Stack>
    </SlotShell>
  );
};

const TerrainSection: React.FC<{ lat: number; lon: number }> = ({ lat, lon }) => {
  const api = useBioApi(); const [data, setData] = useState<any>(null);
  useEffect(() => { api.getTerrain(lat, lon).then(setData).catch(() => {}); }, [lat, lon]);
  if (!data || data.error) return null;
  return <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><MapPin className="w-3.5 h-3.5 text-nkz-accent-base" />Terrain (Copernicus DEM)</h4><DetailGrid columns={2}>{data.elevation_m != null && <DetailItem label="Elevation" value={<>{data.elevation_m} m</>} />}{data.slope_degrees != null && <DetailItem label="Slope" value={<>{data.slope_degrees}&deg;</>} />}</DetailGrid></Stack></Card>;
};

const ClimateSection: React.FC<{ lat: number; lon: number }> = ({ lat, lon }) => {
  const api = useBioApi(); const [data, setData] = useState<any>(null);
  useEffect(() => { api.getClimateReference(lat, lon).then(setData).catch(() => {}); }, [lat, lon]);
  if (!data || data.error) return null;
  return <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Thermometer className="w-3.5 h-3.5 text-nkz-accent-base" />Climate (ERA5-Land)</h4><p className="text-nkz-xs text-nkz-text-muted">{data.source} {data.period_days ? `· ${data.period_days} days` : ''}</p></Stack></Card>;
};

const ScenarioSimulator: React.FC<{ currentCrop: string }> = ({ currentCrop }) => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [scenario, setScenario] = useState('');
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [speciesList, setSpeciesList] = useState<{ name: string; scientificName: string }[]>([]);

  useEffect(() => {
    api.getGraphSpecies()
      .then((data: any) => {
        const crops = Array.isArray(data) ? data : (data?.crops || []);
        setSpeciesList(crops.map((c: any) => ({
          name: c.name || c.scientificName || '',
          scientificName: c.scientificName || c.name || '',
        })).filter((c: { name: string }) => c.name));
      })
      .catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const run = async () => { if (!scenario) return; setLoading(true); try { setResult(await api.simulateCrop(currentCrop, scenario)); } catch {} finally { setLoading(false); } };

  const scenarioOptions = speciesList.filter((c) => c.name !== currentCrop);

  return (
    <Stack gap="stack">
      <div className="flex gap-2 items-center">
        <select className="h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm" value={scenario} onChange={(e) => setScenario(e.target.value)}>
          <option value="">{t('panel.alternativeCrop')}</option>
          {scenarioOptions.map((c) => (
            <option key={c.name} value={c.name}>{c.scientificName || c.name}</option>
          ))}
        </select>
        <Button variant="secondary" size="sm" onClick={run} disabled={!scenario || loading} loading={loading}>{t('panel.compareAction')}</Button>
      </div>
      {result && (
        <div className="rounded-nkz-md bg-nkz-surface-sunken p-nkz-stack text-nkz-sm">
          <div className="flex items-center gap-1.5 mb-1">
            {result.rotation_ok ? <CheckCircle className="w-4 h-4 text-nkz-success" /> : <AlertTriangle className="w-4 h-4 text-nkz-warning" />}
            <span className="font-medium">{result.recommendation}</span>
          </div>
          {(result.baseline_data_gaps?.length > 0 || result.scenario_data_gaps?.length > 0) && (
            <div className="mt-2 pt-2 border-t border-nkz-border text-nkz-xs text-nkz-text-muted">
              <div className="flex justify-between">
                <span>{currentCrop}: {(result.baseline_data_gaps || []).join(', ') || '✓'}</span>
                <span>{scenario}: {(result.scenario_data_gaps || []).join(', ') || '✓'}</span>
              </div>
              <div className="text-nkz-warning mt-1">
                <AlertTriangle size={12} className="inline mr-1" />
                {t('panel.scenarioIncomplete')}
              </div>
            </div>
          )}
        </div>
      )}
    </Stack>
  );
};

function DataRow({ label, available }: { label: string; available?: boolean }) {
  return (
    <div className="flex justify-between items-center py-1">
      <span>{label}</span>
      {available ? (
        <CheckCircle size={14} className="text-nkz-success" />
      ) : (
        <XCircle size={14} className="text-nkz-text-muted" />
      )}
    </div>
  );
}

function CollapsibleSection({ title, defaultOpen = false, children, loading, error, onRetry }: {
  title: string; defaultOpen?: boolean; children: React.ReactNode;
  loading?: boolean; error?: string | null; onRetry?: () => void;
}) {
  const { t } = useTranslation('bioorchestrator');
  const [open, setOpen] = useState(defaultOpen);
  return (
    <Card padding="md">
      <div className="flex items-center gap-2 cursor-pointer select-none" onClick={() => setOpen(!open)}>
        {open ? <ChevronDown size={16} className="text-nkz-text-muted" /> : <ChevronRight size={16} className="text-nkz-text-muted" />}
        <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex-1">{title}</h4>
      </div>
      {open && (
        <div className="mt-3">
          {loading ? <Skeleton variant="rect" height="60px" /> :
           error ? <div className="text-nkz-error text-sm"><AlertTriangle size={14} className="inline mr-1" />{t('panel.sectionError', { section: title })}{onRetry && <Button variant="ghost" size="sm" onClick={onRetry} className="ml-2">{t('panel.retry')}</Button>}</div> :
           children}
        </div>
      )}
    </Card>
  );
}

function Sparkline({ data, width, height }: { data: { value: number }[]; width: number; height: number }) {
  if (data.length < 2) return null;
  const min = Math.min(...data.map(d => d.value));
  const max = Math.max(...data.map(d => d.value));
  const range = max - min || 1;
  const padding = 4;
  const points = data.map((d, i) => {
    const x = padding + (i / (data.length - 1)) * (width - 2 * padding);
    const y = height - padding - ((d.value - min) / range) * (height - 2 * padding);
    return `${x},${y}`;
  }).join(' ');
  return (
    <svg width={width} height={height} className="text-nkz-success">
      <polyline points={points} fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export default RecommendationsPanel;
