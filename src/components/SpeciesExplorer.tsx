import React, { useState, useEffect } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Stack, EmptyState, Skeleton, DetailGrid, DetailItem, Tabs } from '@nekazari/ui-kit';
import { Leaf, Search, AlertTriangle, BookOpen, Thermometer, Mountain, Droplets } from 'lucide-react';
import { useBioApi } from '../services/api';

interface SpeciesItem {
  uri: string;
  eppo_code?: string;
  name: string;
  scientificName?: string;
  scientific_name?: string;
  dataProvider?: string;
  variety_count?: number;
  has_kc?: boolean;
  has_thermal?: boolean;
  has_npk?: boolean;
  has_soil?: boolean;
}

interface PhenologyParam {
  stage: string;
  kc: number;
  ky: number;
  d1: number;
  d2: number;
  source_short?: string;
}

interface ThermalParam {
  heatDamageThresholdC?: number;
  frostDamageThresholdC?: number;
  heatAccumHours?: string;
  sourceType?: string;
  sourceShort?: string;
}

interface SoilParam {
  phMin?: number;
  phMax?: number;
  textures?: string;
  drainage?: string;
  salinityMax?: number;
  sourceShort?: string;
}

interface NutrientParam {
  stage?: string;
  element?: string;
  uptakeKgHaDay?: number;
  sourceShort?: string;
}

export default function SpeciesExplorer() {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();

  const [search, setSearch] = useState('');
  const [species, setSpecies] = useState<SpeciesItem[]>([]);
  const [filtered, setFiltered] = useState<SpeciesItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<SpeciesItem | null>(null);
  const [detailTab, setDetailTab] = useState('phenology');

  // Detail data
  const [phenology, setPhenology] = useState<PhenologyParam[] | null>(null);
  const [thermal, setThermal] = useState<ThermalParam[] | null>(null);
  const [soilData, setSoilData] = useState<SoilParam[] | null>(null);
  const [nutrients, setNutrients] = useState<NutrientParam[] | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    api.getSpecies()
      .then((d: any) => {
        const list: SpeciesItem[] = Array.isArray(d) ? d : (d?.species || d?.crops || []);
        setSpecies(list);
        setFiltered(list);
      })
      .catch(() => setSpecies([]))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!search.trim()) {
      setFiltered(species);
      return;
    }
    const q = search.toLowerCase();
    setFiltered(species.filter(s =>
      (s.name || '').toLowerCase().includes(q) ||
      (s.scientificName || s.scientific_name || '').toLowerCase().includes(q) ||
      (s.eppo_code || '').toLowerCase().includes(q) ||
      (s.uri || '').toLowerCase().includes(q)
    ));
  }, [search, species]);

  const loadDetail = async (s: SpeciesItem) => {
    setSelected(s);
    setDetailLoading(true);
    setPhenology(null); setThermal(null); setSoilData(null); setNutrients(null);

    const slug = s.eppo_code || s.uri?.split('/')?.pop() || s.name;
    const params = new URLSearchParams({ species: slug });

    try {
      const [ph, th, so, nu] = await Promise.all([
        api.getPhenologyParams(params).catch(() => null),
        api.getHeatTolerance(slug).catch(() => null),
        api.getSoilSuitability(slug).catch(() => null),
        api.getNutrientProfile(slug).catch(() => null),
      ]);
      if (Array.isArray(ph)) setPhenology(ph);
      else if (ph?.parameters) setPhenology(ph.parameters);
      else if (ph?.phenology) setPhenology(ph.phenology);

      if (Array.isArray(th)) setThermal(th);
      if (Array.isArray(so)) setSoilData(so);
      if (Array.isArray(nu)) setNutrients(nu);
    } catch {
      // partial data is fine
    } finally {
      setDetailLoading(false);
    }
  };

  const detailTabs = [
    { id: 'phenology', label: t('speciesExplorer.tabs.phenology'), icon: BookOpen },
    { id: 'thermal', label: t('speciesExplorer.tabs.thermal'), icon: Thermometer },
    { id: 'soil', label: t('speciesExplorer.tabs.soil'), icon: Mountain },
    { id: 'nutrients', label: t('speciesExplorer.tabs.nutrients'), icon: Droplets },
  ];

  return (
    <Stack gap="section">
      {/* Header */}
      <div>
        <div className="flex items-center gap-2 mb-1">
          <Leaf className="w-5 h-5 text-nkz-accent-base" />
          <h2 className="text-nkz-xl font-bold text-nkz-text-primary">{t('speciesExplorer.title')}</h2>
        </div>
        <p className="text-nkz-base text-nkz-text-muted">{t('speciesExplorer.subtitle')}</p>
      </div>

      {/* Search bar */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-nkz-text-muted" />
        <input
          type="text"
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder={t('speciesExplorer.searchPlaceholder')}
          className="w-full pl-10 pr-4 py-2.5 rounded-nkz-lg border border-nkz-border bg-transparent
                     text-nkz-sm text-nkz-text-primary placeholder:text-nkz-text-muted
                     focus:outline-none focus:ring-2 focus:ring-nkz-accent-base focus:border-transparent"
        />
      </div>

      {/* Loading */}
      {loading && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} variant="rect" height="120px" />
          ))}
        </div>
      )}

      {/* Empty */}
      {!loading && filtered.length === 0 && (
        <EmptyState
          icon={<Leaf className="w-8 h-8" />}
          title={t('speciesExplorer.noResults')}
        />
      )}

      {/* Species grid + detail panel */}
      {!loading && filtered.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {/* Left: species grid */}
          <div className="lg:col-span-1 space-y-2 max-h-[600px] overflow-y-auto">
            {filtered.map(s => {
              const name = s.name || s.uri?.split('/')?.pop() || '?';
              const sci = s.scientificName || s.scientific_name || '';
              const eppo = s.eppo_code || '';
              return (
                <Card
                  key={s.uri || name}
                  padding="md"
                  role="button"
                  tabIndex={0}
                  className={`cursor-pointer transition-all hover:border-nkz-accent-base ${
                    selected?.uri === s.uri ? 'border-nkz-accent-base bg-nkz-accent-soft' : ''
                  }`}
                  onClick={() => loadDetail(s)}
                  onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); loadDetail(s); } }}
                >
                  <div className="flex items-start gap-2">
                    <Leaf className="w-4 h-4 mt-0.5 text-nkz-accent-base shrink-0" />
                    <div className="min-w-0">
                      <p className="text-nkz-sm font-semibold text-nkz-text-primary truncate">
                        {t(`crops.${eppo}`, { defaultValue: name })}
                      </p>
                      {sci && <p className="text-nkz-xs text-nkz-text-muted italic truncate">{sci}</p>}
                      <div className="flex flex-wrap gap-1 mt-1">
                        {eppo && <Badge intent="info">{eppo}</Badge>}
                        {s.variety_count != null && (
                          <Badge intent="default">{s.variety_count} var.</Badge>
                        )}
                      </div>
                    </div>
                  </div>
                </Card>
              );
            })}
          </div>

          {/* Right: detail panel */}
          <div className="lg:col-span-2">
            {!selected && (
              <Card padding="lg" className="flex items-center justify-center min-h-[300px]">
                <p className="text-nkz-text-muted">{t('speciesExplorer.selectSpecies')}</p>
              </Card>
            )}

            {selected && (
              <Card padding="lg">
                {/* Detail header */}
                <div className="mb-4">
                  <h3 className="text-nkz-lg font-bold text-nkz-text-primary">
                    {t(`crops.${selected.eppo_code || ''}`, { defaultValue: selected.name })}
                  </h3>
                  {(selected.scientificName || selected.scientific_name) && (
                    <p className="text-nkz-sm text-nkz-text-muted italic">
                      {selected.scientificName || selected.scientific_name}
                    </p>
                  )}
                  <div className="flex flex-wrap gap-1 mt-2">
                    {selected.eppo_code && <Badge intent="info">{selected.eppo_code}</Badge>}
                    {selected.dataProvider && <Badge intent="default">{selected.dataProvider}</Badge>}
                    <Badge intent={selected.has_kc ? 'positive' : 'default'}>
                      Kc {selected.has_kc ? '✓' : '✗'}
                    </Badge>
                    <Badge intent={selected.has_thermal ? 'positive' : 'default'}>
                      {t('speciesExplorer.tabs.thermal')} {selected.has_thermal ? '✓' : '✗'}
                    </Badge>
                  </div>
                </div>

                {/* Detail tabs */}
                <Tabs defaultValue="phenology" value={detailTab} onValueChange={setDetailTab}>
                  <Tabs.List>
                    {detailTabs.map(tab => (
                      <Tabs.Trigger key={tab.id} value={tab.id}>
                        <tab.icon className="w-4 h-4 mr-1" />
                        {tab.label}
                      </Tabs.Trigger>
                    ))}
                  </Tabs.List>

                  {detailLoading && <Skeleton variant="rect" height="200px" className="mt-3" />}

                  {/* Phenology */}
                  {!detailLoading && (
                    <Tabs.Content value="phenology">
                      {phenology && phenology.length > 0 ? (
                        <div className="overflow-x-auto mt-3">
                          <table className="w-full text-nkz-sm">
                            <thead>
                              <tr className="border-b border-nkz-border text-nkz-text-secondary">
                                <th className="text-left py-2 pr-3">{t('speciesExplorer.stage')}</th>
                                <th className="text-right py-2 pr-3">Kc</th>
                                <th className="text-right py-2 pr-3">Ky</th>
                                <th className="text-right py-2 pr-3">D1–D2 (d)</th>
                                <th className="text-left py-2">{t('speciesExplorer.source')}</th>
                              </tr>
                            </thead>
                            <tbody>
                              {phenology.map((p, i) => (
                                <tr key={i} className="border-b border-nkz-border-subtle">
                                  <td className="py-2 pr-3 font-medium">{p.stage || `Stage ${i + 1}`}</td>
                                  <td className="py-2 pr-3 text-right">{p.kc?.toFixed(2) ?? '—'}</td>
                                  <td className="py-2 pr-3 text-right">{p.ky?.toFixed(2) ?? '—'}</td>
                                  <td className="py-2 pr-3 text-right">
                                    {p.d1 != null ? `${p.d1}–${p.d2 ?? '?'}` : '—'}
                                  </td>
                                  <td className="py-2 text-nkz-text-secondary text-xs">
                                    {p.source_short || '—'}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <EmptyState
                          icon={<BookOpen className="w-6 h-6" />}
                          title={t('speciesExplorer.noPhenology')}
                        />
                      )}
                    </Tabs.Content>
                  )}

                  {/* Thermal */}
                  {!detailLoading && (
                    <Tabs.Content value="thermal">
                      {thermal && thermal.length > 0 ? (
                        <div className="overflow-x-auto mt-3">
                          <table className="w-full text-nkz-sm">
                            <thead>
                              <tr className="border-b border-nkz-border text-nkz-text-secondary">
                                <th className="text-left py-2 pr-3">{t('speciesExplorer.heatThreshold')}</th>
                                <th className="text-left py-2 pr-3">{t('speciesExplorer.frostThreshold')}</th>
                                <th className="text-left py-2 pr-3">{t('speciesExplorer.accumHours')}</th>
                                <th className="text-left py-2">{t('speciesExplorer.source')}</th>
                              </tr>
                            </thead>
                            <tbody>
                              {thermal.map((th, i) => (
                                <tr key={i} className="border-b border-nkz-border-subtle">
                                  <td className="py-2 pr-3">
                                    {th.heatDamageThresholdC != null ? `${th.heatDamageThresholdC}°C` : '—'}
                                  </td>
                                  <td className="py-2 pr-3">
                                    {th.frostDamageThresholdC != null ? `${th.frostDamageThresholdC}°C` : '—'}
                                  </td>
                                  <td className="py-2 pr-3">{th.heatAccumHours || '—'}</td>
                                  <td className="py-2 text-nkz-text-secondary text-xs">
                                    {th.sourceType || th.sourceShort || '—'}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <EmptyState
                          icon={<Thermometer className="w-6 h-6" />}
                          title={t('speciesExplorer.noThermal')}
                        />
                      )}
                    </Tabs.Content>
                  )}

                  {/* Soil */}
                  {!detailLoading && (
                    <Tabs.Content value="soil">
                      {soilData && soilData.length > 0 ? (
                        <div className="overflow-x-auto mt-3">
                          <table className="w-full text-nkz-sm">
                            <thead>
                              <tr className="border-b border-nkz-border text-nkz-text-secondary">
                                <th className="text-left py-2 pr-3">pH</th>
                                <th className="text-left py-2 pr-3">{t('speciesExplorer.texture')}</th>
                                <th className="text-left py-2 pr-3">{t('speciesExplorer.drainage')}</th>
                                <th className="text-left py-2">{t('speciesExplorer.source')}</th>
                              </tr>
                            </thead>
                            <tbody>
                              {soilData.map((s, i) => (
                                <tr key={i} className="border-b border-nkz-border-subtle">
                                  <td className="py-2 pr-3">
                                    {s.phMin != null ? `${s.phMin}–${s.phMax}` : '—'}
                                  </td>
                                  <td className="py-2 pr-3 text-nkz-text-secondary text-xs">
                                    {s.textures || '—'}
                                  </td>
                                  <td className="py-2 pr-3">{s.drainage || '—'}</td>
                                  <td className="py-2 text-nkz-text-secondary text-xs">
                                    {s.sourceShort || '—'}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <EmptyState
                          icon={<Mountain className="w-6 h-6" />}
                          title={t('speciesExplorer.noSoil')}
                        />
                      )}
                    </Tabs.Content>
                  )}

                  {/* Nutrients */}
                  {!detailLoading && (
                    <Tabs.Content value="nutrients">
                      {nutrients && nutrients.length > 0 ? (
                        <div className="overflow-x-auto mt-3">
                          <table className="w-full text-nkz-sm">
                            <thead>
                              <tr className="border-b border-nkz-border text-nkz-text-secondary">
                                <th className="text-left py-2 pr-3">{t('speciesExplorer.element')}</th>
                                <th className="text-left py-2 pr-3">{t('speciesExplorer.stage')}</th>
                                <th className="text-right py-2 pr-3">{t('speciesExplorer.uptake')}</th>
                                <th className="text-left py-2">{t('speciesExplorer.source')}</th>
                              </tr>
                            </thead>
                            <tbody>
                              {nutrients.map((n, i) => (
                                <tr key={i} className="border-b border-nkz-border-subtle">
                                  <td className="py-2 pr-3 font-medium">{n.element || 'N'}</td>
                                  <td className="py-2 pr-3">{n.stage || '—'}</td>
                                  <td className="py-2 pr-3 text-right">
                                    {n.uptakeKgHaDay != null ? `${n.uptakeKgHaDay} kg/ha/day` : '—'}
                                  </td>
                                  <td className="py-2 text-nkz-text-secondary text-xs">
                                    {n.sourceShort || '—'}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <EmptyState
                          icon={<Droplets className="w-6 h-6" />}
                          title={t('speciesExplorer.noNutrients')}
                        />
                      )}
                    </Tabs.Content>
                  )}
                </Tabs>
              </Card>
            )}
          </div>
        </div>
      )}
    </Stack>
  );
}
