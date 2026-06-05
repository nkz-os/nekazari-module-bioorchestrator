import React, { useEffect, useState, useMemo } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Stack, DataTable, Badge } from '@nekazari/ui-kit';
import { Search } from 'lucide-react';
import { useBioApi } from '../services/api';
import TabSubtitle from './shared/TabSubtitle';
import DataTableSkeleton from './shared/DataTableSkeleton';
import ContextEmptyState from './shared/ContextEmptyState';

interface SpeciesItem {
  uri: string;
  name: string;
  scientificName?: string;
}

interface SoilRow {
  [key: string]: unknown;
  name: string;
  scientificName?: string;
  phMin: number | null;
  phMax: number | null;
  textures: string[];
  drainage: string;
  depthMinCm: number | null;
  salinityMaxDsM: number | null;
}

const SoilSuitability: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [rows, setRows] = useState<SoilRow[]>([]);
  const [speciesCount, setSpeciesCount] = useState(0);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const speciesResp = await api.getGraphSpecies();
        if (cancelled) return;

        const speciesList: SpeciesItem[] = speciesResp?.species ?? speciesResp ?? [];
        setSpeciesCount(speciesList.length);

        const results = await Promise.allSettled(
          speciesList.map((s: SpeciesItem) => api.getSoilSuitability(s.name)),
        );
        if (cancelled) return;

        const soilRows: SoilRow[] = [];
        results.forEach((result, idx) => {
          if (result.status === 'fulfilled' && result.value != null) {
            const s = result.value;
            soilRows.push({
              name: speciesList[idx].name,
              scientificName: speciesList[idx].scientificName,
              phMin: s.phMin ?? s.ph_min ?? null,
              phMax: s.phMax ?? s.ph_max ?? null,
              textures: Array.isArray(s.textures)
                ? s.textures
                : s.texture && Array.isArray(s.texture)
                  ? s.texture
                  : [],
              drainage:
                typeof s.drainage === 'string'
                  ? s.drainage
                  : Array.isArray(s.drainage)
                    ? s.drainage.join(', ')
                    : '—',
              depthMinCm: s.depthMinCm ?? s.depth_min_cm ?? null,
              salinityMaxDsM: s.salinityMaxDsM ?? s.salinity_max_ds_m ?? null,
            });
          }
        });
        setRows(soilRows);
      } catch (e: any) {
        if (!cancelled) setError(e.message ?? 'Failed to load soil data');
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
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
        header: t('soil.columns.species'),
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
        accessorKey: 'phMin',
        header: t('soil.columns.phMin'),
        cell: (info: { getValue: () => unknown }) => (info.getValue() != null ? `${info.getValue()}` : '—'),
      },
      {
        accessorKey: 'phMax',
        header: t('soil.columns.phMax'),
        cell: (info: { getValue: () => unknown }) => (info.getValue() != null ? `${info.getValue()}` : '—'),
      },
      {
        accessorKey: 'textures',
        header: t('soil.columns.textures'),
        cell: (info: { getValue: () => unknown }) => {
          const txs = (info.getValue() ?? []) as string[];
          return txs.length > 0 ? txs.join(', ') : '—';
        },
      },
      {
        accessorKey: 'drainage',
        header: t('soil.columns.drainage'),
        cell: (info: { getValue: () => unknown }) => info.getValue() || '—',
      },
      {
        accessorKey: 'depthMinCm',
        header: t('soil.columns.depthMin'),
        cell: (info: { getValue: () => unknown }) =>
          info.getValue() != null ? `${info.getValue()} cm` : '—',
      },
      {
        accessorKey: 'salinityMaxDsM',
        header: t('soil.columns.salinityMax'),
        cell: (info: { getValue: () => unknown }) =>
          info.getValue() != null ? `${info.getValue()} dS/m` : '—',
      },
    ],
    [t],
  );

  if (loading) {
    return (
      <Stack gap="section">
        <TabSubtitle>{t('soil.subtitle')}</TabSubtitle>
        <DataTableSkeleton columns={7} />
      </Stack>
    );
  }

  if (error) {
    return (
      <Stack gap="section">
        <TabSubtitle>{t('soil.subtitle')}</TabSubtitle>
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
      <TabSubtitle>{t('soil.subtitle')}</TabSubtitle>

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
        <ContextEmptyState message={t('soil.empty')} variant="info" />
      )}

      <p className="text-nkz-xs text-nkz-text-muted">
        {t('soil.count', { shown: rows.length, total: speciesCount })}
        {rows.length > 0 && ` · ${t('soil.allPresent')}`}
      </p>
    </Stack>
  );
};

export default SoilSuitability;
