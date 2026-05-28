import React, { useEffect, useState } from 'react';
import { Stack, Card, Badge, Button, DetailGrid, DetailItem, Skeleton } from '@nekazari/ui-kit';
import { RefreshCw, Globe, Thermometer, MapPin, Sprout, Bug, Beaker, AlertTriangle, CheckCircle, XCircle } from 'lucide-react';
import { useBioApi, useCropApi } from '../services/api';

interface RecCrop { name: string; scientific_name?: string; }
interface SoilData { ph_min: number; ph_max: number; textures: string[]; drainage: string[]; depth_min_cm: number; salinity_max_ds_m: number; source_short?: string; }
interface Props { parcelId?: string; parcelName?: string; cropType?: string; lat?: number; lon?: number; }

const PESTICIDE_INTENT: Record<string, 'positive' | 'negative' | 'warning'> = { approved: 'positive', not_approved: 'negative', withdrawn: 'warning' };
const SCENARIO_CROPS = ['wheat', 'sunflower', 'almond', 'olive', 'grapevine', 'legume'];

const RecommendationsPanel: React.FC<Props> = ({ parcelId, parcelName, cropType = 'olive', lat, lon }) => {
  const api = useBioApi();
  const [nextCrops, setNextCrops] = useState<RecCrop[]>([]);
  const [soil, setSoil] = useState<SoilData | null>(null);
  const [realSoil, setRealSoil] = useState<any>(null);
  const [protectedArea, setProtectedArea] = useState<any>(null);
  const [varieties, setVarieties] = useState<any[]>([]);
  const [pesticides, setPesticides] = useState<any[]>([]);
  const [pollinators, setPollinators] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [dataGaps, setDataGaps] = useState<string[]>([]);
  const [dataAvail, setDataAvail] = useState<Record<string, boolean>>({});
  const { getCropDetail } = useCropApi();

  useEffect(() => {
    if (!cropType) return;
    getCropDetail(`urn:ngsi-ld:AgriCrop:${cropType.replace(/ /g, '_')}`)
      .then(detail => {
        if (detail?.data_available) {
          setDataAvail(detail.data_available);
          const gaps: string[] = [];
          if (!detail.data_available.kc) gaps.push('kc');
          if (!detail.data_available.d1_d2) gaps.push('d1_d2');
          if (!detail.data_available.thermal) gaps.push('thermal');
          if (!detail.data_available.npk) gaps.push('npk');
          setDataGaps(gaps);
        }
      })
      .catch(() => { /* crop not in catalog yet */ });
  }, [cropType]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      const safe = async <T,>(fn: () => Promise<T>): Promise<T | null> => { try { return await fn(); } catch { return null; } };
      const nc = await safe(() => api.getNextCrop(cropType));
      const s = await safe(() => api.getSoilSuitability(cropType));
      if (!cancelled) { setNextCrops(nc?.suggested_crops || []); if (s) setSoil(s); }
      if (lat != null && lon != null && !cancelled) {
        const [rs, pa, vars, pests, polls] = await Promise.all([
          safe(() => api.getSoilData(lat, lon)), safe(() => api.getProtectedArea(lat, lon)),
          safe(() => api.getVarieties(cropType)), safe(() => api.getPesticides(cropType)), safe(() => api.getPollinators(lat, lon)),
        ]);
        if (!cancelled) { if (rs) setRealSoil(rs); if (pa) setProtectedArea(pa); if (vars) setVarieties(vars.varieties || []); if (pests) setPesticides(pests.substances || []); if (polls) setPollinators(polls.pollinators || []); }
      }
      if (!cancelled) setLoading(false);
    })();
    return () => { cancelled = true; };
  }, [cropType, lat, lon]);

  if (loading) return <Stack gap="stack"><Skeleton variant="rect" height="60px" /><Skeleton variant="rect" height="80px" /><Skeleton variant="rect" height="80px" /></Stack>;

  return (
    <Stack gap="stack">
      <div className="flex items-center gap-2"><RefreshCw className="w-4 h-4 text-nkz-accent-base" /><h3 className="text-nkz-md font-semibold text-nkz-text-primary">Recommendations</h3></div>
      {parcelName && <p className="text-nkz-sm text-nkz-text-secondary">{parcelName}</p>}

      <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><RefreshCw className="w-3.5 h-3.5 text-nkz-accent-base" />Rotation</h4><p className="text-nkz-sm">Current: <strong>{cropType}</strong></p>{nextCrops.length > 0 ? <div className="flex flex-wrap gap-1.5">{nextCrops.map((c) => <Badge key={c.name} intent="info">{c.scientific_name || c.name}</Badge>)}</div> : <p className="text-nkz-xs text-nkz-text-muted">No rotation restrictions.</p>}</Stack></Card>

      {realSoil && !realSoil.error && (
        <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Beaker className="w-3.5 h-3.5 text-nkz-accent-base" />Real Soil (SoilGrids 2.0)</h4><DetailGrid columns={2}>{realSoil.ph != null && <DetailItem label="pH" value={realSoil.ph} />}{realSoil.texture_class && <DetailItem label="Texture" value={`${realSoil.texture_class} (${realSoil.sand_pct}% sand)`} />}{realSoil.cec_cmol_kg != null && <DetailItem label="CEC" value={<>{realSoil.cec_cmol_kg} cmol/kg</>} />}</DetailGrid><p className="text-nkz-xs text-nkz-text-muted">{realSoil.source}</p></Stack></Card>
      )}

      {soil && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Globe className="w-3.5 h-3.5 text-nkz-accent-base" />Soil Requirements ({cropType})</h4><DetailGrid columns={2}><DetailItem label="pH" value={<>{soil.ph_min} – {soil.ph_max}</>} /><DetailItem label="Texture" value={(soil.textures || []).join(', ')} /><DetailItem label="Drainage" value={(soil.drainage || []).join(', ')} /></DetailGrid></Stack></Card>}

      {protectedArea?.in_protected_area && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><MapPin className="w-3.5 h-3.5 text-nkz-success" />Protected Area</h4><p className="text-nkz-sm font-medium">{protectedArea.site_name} ({protectedArea.site_code})</p></Stack></Card>}

      {varieties.length > 0 && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Sprout className="w-3.5 h-3.5 text-nkz-accent-base" />Registered Varieties (CPVO)</h4><div className="flex flex-wrap gap-1.5">{varieties.slice(0, 6).map((v: any, i: number) => <Badge key={i} intent="default">{v.variety_name || v.denomination}</Badge>)}</div></Stack></Card>}

      {pesticides.length > 0 && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Bug className="w-3.5 h-3.5 text-nkz-accent-base" />Authorised Pesticides (EU)</h4>{pesticides.slice(0, 5).map((p: any, i: number) => <div key={i} className="flex items-center justify-between text-nkz-sm"><Badge intent={PESTICIDE_INTENT[p.status] || 'default'}>{p.status}</Badge><span className="text-nkz-text-primary">{p.substance}</span></div>)}</Stack></Card>}

      {pollinators.length > 0 && <Card padding="md"><Stack gap="tight"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Sprout className="w-3.5 h-3.5 text-nkz-accent-base" />Pollinators (GBIF)</h4>{pollinators.slice(0, 5).map((p: any, i: number) => <div key={i} className="flex items-center justify-between text-nkz-sm"><span>{p.species}</span><span className="text-nkz-xs text-nkz-text-muted">{p.record_count} records</span></div>)}</Stack></Card>}

      {lat != null && lon != null && <TerrainSection lat={lat} lon={lon} />}
      {lat != null && lon != null && <ClimateSection lat={lat} lon={lon} />}

      <Card padding="md">
        <Stack gap="tight">
          <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider">
            Crop Data — {cropType || '—'}
          </h4>
          <DataRow label="Kc" available={dataAvail.kc} />
          <DataRow label="Thermal" available={dataAvail.thermal} />
          <DataRow label="NPK" available={dataAvail.npk} />
          <DataRow label="Rotation" available={dataAvail.rotation} />
          {dataGaps.length > 0 && (
            <div className="text-nkz-warning text-sm mt-2">
              <AlertTriangle size={14} className="inline mr-1" />
              Incomplete data. Yield estimation is unavailable until missing data is completed.
            </div>
          )}
        </Stack>
      </Card>

      <Card padding="md"><Stack gap="stack"><h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5"><Beaker className="w-3.5 h-3.5 text-nkz-accent-base" />Simulate Scenario</h4><ScenarioSimulator currentCrop={cropType} /></Stack></Card>
    </Stack>
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
  const api = useBioApi(); const [scenario, setScenario] = useState(''); const [result, setResult] = useState<any>(null); const [loading, setLoading] = useState(false);
  const run = async () => { if (!scenario) return; setLoading(true); try { setResult(await api.simulateCrop(currentCrop, scenario)); } catch {} finally { setLoading(false); } };
  return <Stack gap="stack"><div className="flex gap-2 items-center"><select className="h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm" value={scenario} onChange={(e) => setScenario(e.target.value)}><option value="">Alternative...</option>{SCENARIO_CROPS.filter((c) => c !== currentCrop).map((c) => <option key={c} value={c}>{c}</option>)}</select><Button variant="secondary" size="sm" onClick={run} disabled={!scenario || loading} loading={loading}>Compare</Button></div>{result && <div className="rounded-nkz-md bg-nkz-surface-sunken p-nkz-stack text-nkz-sm"><div className="flex items-center gap-1.5 mb-1">{result.rotation_ok ? <CheckCircle className="w-4 h-4 text-nkz-success" /> : <AlertTriangle className="w-4 h-4 text-nkz-warning" />}<span className="font-medium">{result.recommendation}</span></div></div>}</Stack>;
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

export default RecommendationsPanel;
