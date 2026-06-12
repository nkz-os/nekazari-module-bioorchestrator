import React, { useEffect, useState, useMemo, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Stack, DataTable, Badge, Input } from '@nekazari/ui-kit';
import { Search } from 'lucide-react';
import { useBioApi, useCropApi } from '../services/api';
import DataTableSkeleton from './shared/DataTableSkeleton';
import ContextEmptyState from './shared/ContextEmptyState';

interface SpeciesItem {
  uri: string;
  name: string;
  scientificName?: string;
}

interface ThermalRow {
  [key: string]: unknown;
  name: string;
  scientificName?: string;
  heatThresholdC?: number;
  frostThresholdC?: number;
  accumHours?: number;
  sourceShort?: string;
  sourceType?: string;
}

interface ThermalSummary {
  total_species: number;
  with_thermal: number;
  without_thermal: number;
}

const ThermalTolerance: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const cropApi = useCropApi();
  const [rows, setRows] = useState<ThermalRow[]>([]);
  const [speciesCount, setSpeciesCount] = useState(0);
  const [summary, setSummary] = useState<ThermalSummary | null>(null);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deriving, setDeriving] = useState(false);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const [speciesResp, thermalSummary] = await Promise.all([
          api.getGraphSpecies(),
          cropApi.getThermalSummary().catch(() => null),
        ]);
        if (cancelled) return;

        const speciesList: SpeciesItem[] = speciesResp?.species ?? speciesResp ?? [];
        setSpeciesCount(speciesList.length);
        setSummary(thermalSummary);

        // Fetch thermal data for each species in parallel
        const results = await Promise.allSettled(
          speciesList.map((s: SpeciesItem) => api.getHeatTolerance(s.name)),
        );
        if (cancelled) return;

        const thermalRows: ThermalRow[] = [];
        results.forEach((result, idx) => {
          if (result.status === 'fulfilled' && result.value != null) {
            const ht = result.value;
            thermalRows.push({
              name: speciesList[idx].name,
              scientificName: speciesList[idx].scientificName,
              heatThresholdC: ht.heatDamageThresholdC ?? ht.heat_damage_c,
              frostThresholdC: ht.frostDamageThresholdC ?? ht.frost_damage_c,
              accumHours: ht.heatAccumHours ?? ht.heat_accum_hours,
              sourceShort: ht.sourceShort ?? ht.source_short,
              sourceType: ht.sourceType ?? ht.source_type,
            });
          }
        });
        setRows(thermalRows);
      } catch (e: any) {
        if (!cancelled) setError(e.message ?? 'Failed to load thermal data');
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  const handleDerive = useCallback(async () => {
    setDeriving(true);
    try {
      await cropApi.triggerDeriveThermal();
      window.location.reload();
    } catch {
      setDeriving(false);
    }
  }, []);

  const filteredRows = useMemo(() => {
    if (!search) return rows;
    const q = search.toLowerCase();
    return rows.filter(
      (r) =>
        r.name.toLowerCase().includes(q) ||
        (r.scientificName && r.scientificName.toLowerCase().includes(q)),
    );
  }, [rows, search]);

  const columns = useMemo(
    () => [
      {
        accessorKey: 'name',
        header: t('thermal.columns.species'),
        cell: (info: { getValue: () => unknown; row: { original: Record<string, unknown> } }) => (
          <div>
            <span className="text-nkz-text-primary font-medium">
              {info.row.original.name as string}
            </span>
            {(info.row.original.scientificName as string) && (
              <span className="text-nkz-text-muted text-xs block italic">
                {info.row.original.scientificName as string}
              </span>
            )}
          </div>
        ),
      },
      {
        accessorKey: 'heatThresholdC',
        header: t('thermal.columns.heatThreshold'),
        cell: (info: { getValue: () => unknown }) =>
          info.getValue() != null ? `${info.getValue()}°C` : '—',
      },
      {
        accessorKey: 'frostThresholdC',
        header: t('thermal.columns.frostThreshold'),
        cell: (info: { getValue: () => unknown }) =>
          info.getValue() != null ? `${info.getValue()}°C` : '—',
      },
      {
        accessorKey: 'accumHours',
        header: t('thermal.columns.accumHours'),
        cell: (info: { getValue: () => unknown }) =>
          info.getValue() != null ? `${info.getValue()}h` : '—',
      },
      {
        accessorKey: 'sourceType',
        header: t('thermal.columns.source'),
        cell: (info: { getValue: () => unknown; row: { original: Record<string, unknown> } }) => {
          const st = info.getValue() as string | undefined;
          const src = (info.row.original.sourceShort ?? '') as string;
          const isDerived =
            st === 'derived' || src.toLowerCase().includes('ecocrop');
          return (
            <Badge intent={isDerived ? 'info' : 'positive'}>
              {isDerived ? t('thermal.derivedSource') : t('thermal.publishedSource')}
            </Badge>
          );
        },
      },
    ],
    [t],
  );

  if (loading) {
    return (
      <Stack gap="section">
        <p className="text-nkz-text-muted text-sm mb-3 leading-relaxed max-w-3xl">{t('thermal.subtitle')}</p>
        <DataTableSkeleton columns={5} />
      </Stack>
    );
  }

  if (error) {
    return (
      <Stack gap="section">
        <p className="text-nkz-text-muted text-sm mb-3 leading-relaxed max-w-3xl">{t('thermal.subtitle')}</p>
        <ContextEmptyState
          message={error}
          variant="warning"
          actionLabel={t('panel.retry')}
          onAction={() => window.location.reload()}
        />
      </Stack>
    );
  }

  return (
    <Stack gap="section">
      <p className="text-nkz-text-muted text-sm mb-3 leading-relaxed max-w-3xl">{t('thermal.subtitle')}</p>

      <div className="relative max-w-sm">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-nkz-text-muted" />
        <input
          placeholder={t('catalog.search')}
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface pl-9 px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
        />
      </div>

      {filteredRows.length > 0 ? (
        <DataTable columns={columns} data={filteredRows} />
      ) : (
        <ContextEmptyState
          message={t('thermal.empty')}
          variant="info"
          actionLabel={deriving ? undefined : t('thermal.deriveAction')}
          onAction={handleDerive}
        />
      )}

      <p className="text-nkz-xs text-nkz-text-muted">
        {t('thermal.count', { shown: rows.length, total: speciesCount })}
        {summary &&
          summary.without_thermal > 0 &&
          ` · ${t('thermal.pendingDerivation', { count: summary.without_thermal })}`}
      </p>
    </Stack>
  );
};

export default ThermalTolerance;
