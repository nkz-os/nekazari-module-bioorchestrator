import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Stack, Card, Badge, Button, Spinner } from '@nekazari/ui-kit';
import {
  RefreshCw, Globe, Thermometer, MapPin, Sprout,
  Bug, Beaker, AlertTriangle, CheckCircle, XCircle,
} from 'lucide-react';
import { useBioApi } from '../services/api';

interface RecCrop { name: string; scientific_name?: string; }
interface SoilData { ph_min: number; ph_max: number; textures: string[]; drainage: string[]; depth_min_cm: number; salinity_max_ds_m: number; source_short?: string; }

interface Props { parcelId?: string; parcelName?: string; cropType?: string; lat?: number; lon?: number; }

const PESTICIDE_INTENT: Record<string, 'positive' | 'negative' | 'warning'> = {
  approved: 'positive',
  not_approved: 'negative',
  withdrawn: 'warning',
};

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
  const [sectionErrors, setSectionErrors] = useState<string[]>([]);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setSectionErrors([]);
      const errors: string[] = [];

      const safe = async <T,>(fn: () => Promise<T>, label: string): Promise<T | null> => {
        try { return await fn(); } catch (e: any) {
          errors.push(`${label}: ${e.message}`);
          return null;
        }
      };

      const [ncResult, soilResult] = await Promise.all([
        safe(() => api.getNextCrop(cropType), 'Next crop'),
        safe(() => api.getSoilSuitability(cropType), 'Soil suitability'),
      ]);

      if (!cancelled) {
        setNextCrops(ncResult?.suggested_crops || []);
        if (soilResult) setSoil(soilResult);
      }

      if (lat != null && lon != null && !cancelled) {
        const [rs, pa, vars, pests, polls] = await Promise.all([
          safe(() => api.getSoilData(lat, lon), 'Soil data'),
          safe(() => api.getProtectedArea(lat, lon), 'Protected area'),
          safe(() => api.getVarieties(cropType), 'Varieties'),
          safe(() => api.getPesticides(cropType), 'Pesticides'),
          safe(() => api.getPollinators(lat, lon), 'Pollinators'),
        ]);
        if (!cancelled) {
          if (rs) setRealSoil(rs);
          if (pa) setProtectedArea(pa);
          if (vars) setVarieties(vars.varieties || []);
          if (pests) setPesticides(pests.substances || []);
          if (polls) setPollinators(polls.pollinators || []);
        }
      }

      if (!cancelled) setSectionErrors(errors);
      if (!cancelled) setLoading(false);
    };
    load();
    return () => { cancelled = true; };
  }, [cropType, lat, lon]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-nkz-section">
        <Spinner size="md" />
      </div>
    );
  }

  return (
    <Stack gap="stack">
      <div className="flex items-center gap-2">
        <RefreshCw className="w-4 h-4 text-nkz-accent-base" />
        <h3 className="text-nkz-md font-semibold text-nkz-text-primary">Recommendations</h3>
      </div>

      {parcelName && <p className="text-nkz-sm text-nkz-text-secondary">{parcelName}</p>}

      {/* Rotation */}
      <Card padding="md">
        <Stack gap="tight">
          <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
            <RefreshCw className="w-3.5 h-3.5 text-nkz-accent-base" />
            Rotation
          </h4>
          <p className="text-nkz-sm text-nkz-text-primary">
            Current: <strong>{cropType}</strong>
          </p>
          {nextCrops.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {nextCrops.map((c) => (
                <Badge key={c.name} intent="info">{c.scientific_name || c.name}</Badge>
              ))}
            </div>
          ) : (
            <p className="text-nkz-xs text-nkz-text-muted">No rotation restrictions.</p>
          )}
        </Stack>
      </Card>

      {/* Real Soil (SoilGrids) */}
      {realSoil && !realSoil.error && (
        <Card padding="md">
          <Stack gap="tight">
            <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
              <Beaker className="w-3.5 h-3.5 text-nkz-accent-base" />
              Real Soil (SoilGrids 2.0)
            </h4>
            <div className="bg-nkz-surface border border-nkz-border rounded-nkz-md p-nkz-stack divide-y divide-nkz-border text-nkz-sm">
              {realSoil.ph != null && (
                <div className="flex justify-between py-1"><span className="text-nkz-text-secondary">pH</span><span className="text-nkz-text-primary font-medium">{realSoil.ph}</span></div>
              )}
              {realSoil.texture_class && (
                <div className="flex justify-between py-1"><span className="text-nkz-text-secondary">Texture</span><span className="text-nkz-text-primary font-medium">{realSoil.texture_class} ({realSoil.sand_pct}% sand, {realSoil.clay_pct}% clay)</span></div>
              )}
              {realSoil.cec_cmol_kg != null && (
                <div className="flex justify-between py-1"><span className="text-nkz-text-secondary">CEC</span><span className="text-nkz-text-primary font-medium">{realSoil.cec_cmol_kg} cmol/kg</span></div>
              )}
            </div>
            <p className="text-nkz-xs text-nkz-text-muted">{realSoil.source}</p>
          </Stack>
        </Card>
      )}

      {/* Soil Suitability */}
      {soil && (
        <Card padding="md">
          <Stack gap="tight">
            <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
              <Globe className="w-3.5 h-3.5 text-nkz-accent-base" />
              Soil Requirements ({cropType})
            </h4>
            <div className="bg-nkz-surface border border-nkz-border rounded-nkz-md p-nkz-stack divide-y divide-nkz-border text-nkz-sm">
              <div className="flex justify-between py-1"><span className="text-nkz-text-secondary">pH</span><span className="text-nkz-text-primary font-medium">{soil.ph_min} – {soil.ph_max}</span></div>
              <div className="flex justify-between py-1"><span className="text-nkz-text-secondary">Texture</span><span className="text-nkz-text-primary font-medium">{(soil.textures || []).join(', ')}</span></div>
              <div className="flex justify-between py-1"><span className="text-nkz-text-secondary">Drainage</span><span className="text-nkz-text-primary font-medium">{(soil.drainage || []).join(', ')}</span></div>
            </div>
          </Stack>
        </Card>
      )}

      {/* Natura 2000 */}
      {protectedArea && protectedArea.in_protected_area && (
        <Card padding="md">
          <Stack gap="tight">
            <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
              <MapPin className="w-3.5 h-3.5 text-nkz-success" />
              Protected Area
            </h4>
            <p className="text-nkz-sm text-nkz-text-primary font-medium">
              {protectedArea.site_name} ({protectedArea.site_code})
            </p>
            {protectedArea.restrictions && (
              <div className="flex items-center gap-1.5 text-nkz-xs text-nkz-warning bg-nkz-warning-soft rounded-nkz-md px-nkz-inline py-nkz-tight">
                <AlertTriangle className="w-3.5 h-3.5" />
                {protectedArea.restrictions}
              </div>
            )}
          </Stack>
        </Card>
      )}

      {/* Varieties */}
      {varieties.length > 0 && (
        <Card padding="md">
          <Stack gap="tight">
            <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
              <Sprout className="w-3.5 h-3.5 text-nkz-accent-base" />
              Registered Varieties (CPVO)
            </h4>
            <div className="flex flex-wrap gap-1.5">
              {varieties.slice(0, 6).map((v: any, i: number) => (
                <Badge key={i} intent="default">{v.variety_name || v.denomination}
                  {v.variety_name || v.denomination}
                </Badge>
              ))}
            </div>
            {varieties.length > 6 && (
              <p className="text-nkz-xs text-nkz-text-muted">+{varieties.length - 6} more</p>
            )}
          </Stack>
        </Card>
      )}

      {/* Pesticides */}
      {pesticides.length > 0 && (
        <Card padding="md">
          <Stack gap="tight">
            <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
              <Bug className="w-3.5 h-3.5 text-nkz-accent-base" />
              Authorised Pesticides (EU)
            </h4>
            {pesticides.slice(0, 5).map((p: any, i: number) => (
              <div key={i} className="flex items-center justify-between text-nkz-sm">
                <div className="flex items-center gap-1.5">
                  <Badge intent={PESTICIDE_INTENT[p.status] || 'default'}>
                    {p.status || 'unknown'}
                  </Badge>
                  <span className="text-nkz-text-primary">{p.substance}</span>
                </div>
                {p.mrl_mg_kg != null && (
                  <span className="text-nkz-xs text-nkz-text-muted">
                    MRL: {p.mrl_mg_kg} mg/kg
                  </span>
                )}
              </div>
            ))}
          </Stack>
        </Card>
      )}

      {/* Pollinators */}
      {pollinators.length > 0 && (
        <Card padding="md">
          <Stack gap="tight">
            <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
              <Sprout className="w-3.5 h-3.5 text-nkz-accent-base" />
              Pollinators (GBIF, 5km)
            </h4>
            {pollinators.slice(0, 5).map((p: any, i: number) => (
              <div key={i} className="flex items-center justify-between text-nkz-sm">
                <span className="text-nkz-text-primary">{p.species}</span>
                <span className="text-nkz-xs text-nkz-text-muted">
                  {p.record_count} records
                </span>
              </div>
            ))}
          </Stack>
        </Card>
      )}

      {/* Terrain */}
      {lat != null && lon != null && <TerrainSection lat={lat} lon={lon} />}

      {/* Climate */}
      {lat != null && lon != null && <ClimateSection lat={lat} lon={lon} />}

      {/* Scenario Simulator */}
      <Card padding="md">
        <Stack gap="stack">
          <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
            <Beaker className="w-3.5 h-3.5 text-nkz-accent-base" />
            Simulate Scenario
          </h4>
          <ScenarioSimulator currentCrop={cropType} />
        </Stack>
      </Card>

      {/* Section errors (non-blocking) */}
      {sectionErrors.length > 0 && (
        <div className="text-nkz-xs text-nkz-text-muted space-y-0.5">
          {sectionErrors.map((e, i) => (
            <p key={i}>{e}</p>
          ))}
        </div>
      )}
    </Stack>
  );
};

// ── Terrain sub-component ─────────────────────────────────────────────────

const TerrainSection: React.FC<{ lat: number; lon: number }> = ({ lat, lon }) => {
  const api = useBioApi();
  const [data, setData] = useState<any>(null);

  useEffect(() => {
    api.getTerrain(lat, lon)
      .then(setData)
      .catch(() => {});
  }, [lat, lon]);

  if (!data || data.error) return null;

  return (
    <Card padding="md">
      <Stack gap="tight">
        <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
          <MapPin className="w-3.5 h-3.5 text-nkz-accent-base" />
          Terrain (Copernicus DEM)
        </h4>
        <div className="bg-nkz-surface border border-nkz-border rounded-nkz-md p-nkz-stack divide-y divide-nkz-border text-nkz-sm">
          {data.elevation_m != null && <div className="flex justify-between py-1"><span className="text-nkz-text-secondary">Elevation</span><span className="text-nkz-text-primary font-medium">{data.elevation_m} m</span></div>}
          {data.slope_degrees != null && <div className="flex justify-between py-1"><span className="text-nkz-text-secondary">Slope</span><span className="text-nkz-text-primary font-medium">{data.slope_degrees}&deg;</span></div>}
        </div>
        <p className="text-nkz-xs text-nkz-text-muted">{data.source}</p>
      </Stack>
    </Card>
  );
};

// ── Climate sub-component ─────────────────────────────────────────────────

const ClimateSection: React.FC<{ lat: number; lon: number }> = ({ lat, lon }) => {
  const api = useBioApi();
  const [data, setData] = useState<any>(null);

  useEffect(() => {
    api.getClimateReference(lat, lon)
      .then(setData)
      .catch(() => {});
  }, [lat, lon]);

  if (!data || data.error) return null;

  return (
    <Card padding="md">
      <Stack gap="tight">
        <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider flex items-center gap-1.5">
          <Thermometer className="w-3.5 h-3.5 text-nkz-accent-base" />
          Climate (ERA5-Land)
        </h4>
        <p className="text-nkz-xs text-nkz-text-muted">
          {data.source} {data.period_days ? `· ${data.period_days} days ref.` : ''}
        </p>
      </Stack>
    </Card>
  );
};

// ── Scenario Simulator ────────────────────────────────────────────────────

const ScenarioSimulator: React.FC<{ currentCrop: string }> = ({ currentCrop }) => {
  const api = useBioApi();
  const [scenario, setScenario] = useState('');
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const runSimulation = async () => {
    if (!scenario) return;
    setLoading(true);
    try {
      const data = await api.simulateCrop(currentCrop, scenario);
      setResult(data);
    } catch {} finally {
      setLoading(false);
    }
  };

  return (
    <Stack gap="stack">
      <div className="flex gap-2 items-center">
        <select
          className="h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
          value={scenario}
          onChange={(e) => setScenario(e.target.value)}
        >
          <option value="">Alternative...</option>
          {SCENARIO_CROPS.filter((c) => c !== currentCrop).map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
        <Button
          variant="secondary"
          size="sm"
          onClick={runSimulation}
          disabled={!scenario || loading}
          loading={loading}
        >
          Compare
        </Button>
      </div>
      {result && (
        <div className="rounded-nkz-md bg-nkz-surface-sunken p-nkz-stack text-nkz-sm">
          <div className="flex items-center gap-1.5 mb-1">
            {result.rotation_ok
              ? <CheckCircle className="w-4 h-4 text-nkz-success" />
              : <AlertTriangle className="w-4 h-4 text-nkz-warning" />
            }
            <span className="text-nkz-text-primary font-medium">{result.recommendation}</span>
          </div>
          {result.rotation_issue && (
            <p className="text-nkz-xs text-nkz-text-secondary flex items-center gap-1">
              <RefreshCw className="w-3 h-3" />
              {result.rotation_issue}
            </p>
          )}
          {result.fertilizer_delta?.map((f: any, i: number) => (
            <p key={i} className="text-nkz-xs text-nkz-text-secondary flex items-center gap-1 mt-0.5">
              <Beaker className="w-3 h-3" />
              {f.element}: {f.delta_kg_ha_day > 0 ? '+' : ''}{f.delta_kg_ha_day} kg/ha/day — {f.note}
            </p>
          ))}
        </div>
      )}
    </Stack>
  );
};

export default RecommendationsPanel;
