import React, { useEffect, useState, useMemo } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Stack, DataTable, Badge } from '@nekazari/ui-kit';

import { useBioApi } from '../services/api';
import TabSubtitle from './shared/TabSubtitle';
import DataTableSkeleton from './shared/DataTableSkeleton';
import ContextEmptyState from './shared/ContextEmptyState';

interface SpeciesItem {
  uri: string;
  name: string;
  scientificName?: string;
}

interface NpkRow {
  stage: string;
  n: number | null;
  p: number | null;
  k: number | null;
  sourceShort?: string;
}

const selectCls =
  'w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base';

const NutrientProfile: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [speciesList, setSpeciesList] = useState<SpeciesItem[]>([]);
  const [selectedSpecies, setSelectedSpecies] = useState('');
  const [rows, setRows] = useState<NpkRow[]>([]);
  const [loadingSpecies, setLoadingSpecies] = useState(true);
  const [loadingNpk, setLoadingNpk] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api
      .getGraphSpecies()
      .then((d: any) => {
        const list: SpeciesItem[] = d?.species ?? d ?? [];
        setSpeciesList(list);
      })
      .catch(() => {})
      .finally(() => setLoadingSpecies(false));
  }, []);

  useEffect(() => {
    if (!selectedSpecies) {
      setRows([]);
      setError(null);
      return;
    }
    let cancelled = false;
    (async () => {
      setLoadingNpk(true);
      setError(null);
      try {
        const data = await api.getNutrientProfile(selectedSpecies);
        if (cancelled) return;

        if (data == null) {
          setRows([]);
          return;
        }

        if (Array.isArray(data)) {
          // Direct array of npk entries
          setRows(
            data.map((d: any) => ({
              stage: d.stage ?? d.stage_name ?? '—',
              n: d.n ?? d.n_uptake ?? null,
              p: d.p ?? d.p_uptake ?? null,
              k: d.k ?? d.k_uptake ?? null,
              sourceShort: d.sourceShort ?? d.source_short,
            })),
          );
        } else if (data.profiles && Array.isArray(data.profiles)) {
          // Wrapped in profiles key
          setRows(
            data.profiles.map((p: any) => ({
              stage: p.stage ?? '—',
              n: p.n ?? null,
              p: p.p ?? null,
              k: p.k ?? null,
              sourceShort: p.sourceShort ?? p.source_short,
            })),
          );
        } else if (data.stage) {
          // Single object
          setRows([
            {
              stage: data.stage ?? '—',
              n: data.n ?? null,
              p: data.p ?? null,
              k: data.k ?? null,
              sourceShort: data.sourceShort ?? data.source_short,
            },
          ]);
        } else {
          setRows([]);
        }
      } catch (e: any) {
        if (!cancelled) setError(e.message ?? 'Failed to load NPK data');
      } finally {
        if (!cancelled) setLoadingNpk(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedSpecies]);

  const totals = useMemo(() => {
    const acc = { n: 0, p: 0, k: 0 };
    for (const r of rows) {
      if (r.n != null) acc.n += r.n;
      if (r.p != null) acc.p += r.p;
      if (r.k != null) acc.k += r.k;
    }
    return acc;
  }, [rows]);

  const columns = useMemo(
    () => [
      { accessorKey: 'stage', header: t('npk.columns.stage') },
      {
        accessorKey: 'n',
        header: t('npk.columns.n'),
        cell: (info: { getValue: () => any }) =>
          info.getValue() != null ? `${Number(info.getValue()).toFixed(1)}` : '—',
      },
      {
        accessorKey: 'p',
        header: t('npk.columns.p'),
        cell: (info: { getValue: () => any }) =>
          info.getValue() != null ? `${Number(info.getValue()).toFixed(1)}` : '—',
      },
      {
        accessorKey: 'k',
        header: t('npk.columns.k'),
        cell: (info: { getValue: () => any }) =>
          info.getValue() != null ? `${Number(info.getValue()).toFixed(1)}` : '—',
      },
      {
        accessorKey: 'sourceShort',
        header: t('npk.columns.source'),
        cell: (info: { getValue: () => any }) =>
          info.getValue() ? (
            <Badge intent="info" size="sm">
              {info.getValue() as string}
            </Badge>
          ) : (
            '—'
          ),
      },
    ],
    [t],
  );

  return (
    <Stack gap="section">
      <TabSubtitle>{t('npk.subtitle')}</TabSubtitle>

      {loadingSpecies ? (
        <div className="h-9 rounded-nkz-md bg-nkz-surface-sunken animate-pulse" />
      ) : (
        <div className="max-w-xs">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
            {t('npk.selectSpecies')}
          </label>
          <select
            className={selectCls}
            value={selectedSpecies}
            onChange={(e) => setSelectedSpecies(e.target.value)}
          >
            <option value="">{t('npk.selectSpecies')}...</option>
            {speciesList.map((s) => (
              <option key={s.name} value={s.name}>
                {s.scientificName ? `${s.name} (${s.scientificName})` : s.name}
              </option>
            ))}
          </select>
        </div>
      )}

      {!selectedSpecies && (
        <ContextEmptyState
          message="Select a species to view NPK profile"
          variant="info"
        />
      )}

      {selectedSpecies && loadingNpk && <DataTableSkeleton columns={5} />}

      {selectedSpecies && error && (
        <ContextEmptyState
          message={error}
          variant="warning"
          actionLabel={t('panel.retry')}
          onAction={() => setSelectedSpecies(selectedSpecies)}
        />
      )}

      {selectedSpecies && !loadingNpk && !error && rows.length === 0 && (
        <ContextEmptyState
          message={t('npk.empty')}
          variant="info"
          actionLabel={t('npk.contributeAction')}
        />
      )}

      {selectedSpecies && !loadingNpk && !error && rows.length > 0 && (
        <>
          <DataTable columns={columns} data={rows} />

          <div className="rounded-nkz-md bg-nkz-surface-sunken p-nkz-stack text-nkz-xs text-nkz-text-primary">
            {t('npk.totalRow', {
              n: totals.n.toFixed(1),
              p: totals.p.toFixed(1),
              k: totals.k.toFixed(1),
            })}
          </div>

          <p className="text-nkz-xs text-nkz-text-muted italic">
            {t('npk.disclaimer')}
          </p>
        </>
      )}
    </Stack>
  );
};

export default NutrientProfile;
