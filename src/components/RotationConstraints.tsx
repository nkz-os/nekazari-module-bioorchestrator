import React, { useEffect, useState, useMemo } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Stack, DataTable, Badge } from '@nekazari/ui-kit';
import { Search, AlertTriangle } from 'lucide-react';
import { useBioApi } from '../services/api';
import TabSubtitle from './shared/TabSubtitle';
import DataTableSkeleton from './shared/DataTableSkeleton';
import ContextEmptyState from './shared/ContextEmptyState';

interface SpeciesItem {
  uri: string;
  name: string;
  scientificName?: string;
}

interface RotationRow {
  cropA: string;
  cropB: string;
  intervalYears: number;
  reason: string;
  sourceShort?: string;
}

const LOW_COVERAGE_THRESHOLD = 30;

const RotationConstraints: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [rows, setRows] = useState<RotationRow[]>([]);
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

        const results = await Promise.allSettled(
          speciesList.map((s: SpeciesItem) => api.getRotationConstraints(s.name)),
        );
        if (cancelled) return;

        const rotationRows: RotationRow[] = [];
        const seen = new Set<string>();

        results.forEach((result) => {
          if (result.status === 'fulfilled' && result.value != null) {
            const data = result.value;
            const constraints = Array.isArray(data)
              ? data
              : data.constraints ?? [];

            for (const c of constraints) {
              const cropA = c.cropA ?? c.from ?? '';
              const cropB = c.cropB ?? c.to ?? '';
              const key = `${cropA}|${cropB}`;
              if (!cropA || !cropB || seen.has(key)) continue;
              seen.add(key);

              rotationRows.push({
                cropA,
                cropB,
                intervalYears: c.intervalYears ?? c.interval_years ?? 0,
                reason: c.reason ?? c.rationale ?? '',
                sourceShort: c.sourceShort ?? c.source_short,
              });
            }
          }
        });

        setRows(rotationRows);
      } catch (e: any) {
        if (!cancelled) setError(e.message ?? 'Failed to load rotation data');
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
        r.cropA.toLowerCase().includes(q) ||
        r.cropB.toLowerCase().includes(q) ||
        r.reason.toLowerCase().includes(q),
    );
  }, [rows, search]);

  const columns = useMemo(
    () => [
      { accessorKey: 'cropA', header: t('rotation.columns.cropA') },
      { accessorKey: 'cropB', header: t('rotation.columns.cropB') },
      {
        accessorKey: 'intervalYears',
        header: t('rotation.columns.interval'),
        cell: (info: { getValue: () => any }) => {
          const years = info.getValue() as number;
          return years === 0
            ? t('rotation.perennial')
            : `${years} ${years === 1 ? 'year' : 'years'}`;
        },
      },
      {
        accessorKey: 'reason',
        header: t('rotation.columns.reason'),
        cell: (info: { getValue: () => any; row: { original: RotationRow } }) => {
          const reason: string = info.getValue() ?? '';
          const row = info.row.original;
          if (!reason && !row.sourceShort) return '—';
          return (
            <div className="flex flex-col gap-0.5">
              {reason && (
                <span className="text-nkz-text-primary text-xs">{reason}</span>
              )}
              {row.sourceShort && (
                <Badge intent="info" size="sm">
                  {row.sourceShort}
                </Badge>
              )}
            </div>
          );
        },
      },
    ],
    [t],
  );

  if (loading) {
    return (
      <Stack gap="section">
        <TabSubtitle>{t('rotation.subtitle')}</TabSubtitle>
        <DataTableSkeleton columns={4} />
      </Stack>
    );
  }

  if (error) {
    return (
      <Stack gap="section">
        <TabSubtitle>{t('rotation.subtitle')}</TabSubtitle>
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
      <TabSubtitle>{t('rotation.subtitle')}</TabSubtitle>

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
      ) : search ? (
        <ContextEmptyState
          message={t('rotation.empty')}
          variant="info"
          actionLabel={t('rotation.contributeAction')}
        />
      ) : (
        <ContextEmptyState
          message={t('rotation.empty')}
          variant="warning"
          actionLabel={t('rotation.contributeAction')}
        />
      )}

      {rows.length > 0 && rows.length < LOW_COVERAGE_THRESHOLD && (
        <div className="flex items-center gap-2 rounded-nkz-md bg-nkz-warning-soft border border-nkz-warning p-nkz-stack text-nkz-xs text-nkz-warning">
          <AlertTriangle className="w-4 h-4 flex-shrink-0" />
          <span>
            {t('rotation.lowCoverage', { count: rows.length })}
          </span>
        </div>
      )}
    </Stack>
  );
};

export default RotationConstraints;
