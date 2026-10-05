import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Card, EmptyState, Inline, Skeleton, Stack } from '@nekazari/ui-kit';
import { fetchEvidence, type QueryParams } from '../../services/recommendApi';
import type { EvidencePage, Similarity } from '../../types/recommend';
import { EVIDENCE_PAGE_SIZE } from './pageModel';
import { kgUnitKey, yieldUnit } from './viewModel';

interface EvidenceDialogProps {
  cropName: string;
  eppo: string;
  conditions: QueryParams;
  similarity: Similarity;
  onClose: () => void;
}

/** Paginated trials behind one recommendation. */
export default function EvidenceDialog({ cropName, eppo, conditions, similarity, onClose }: EvidenceDialogProps) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const [page, setPage] = useState(1);
  const [data, setData] = useState<EvidencePage | null>(null);
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(true);
  const [retry, setRetry] = useState(0);
  const noData = t('whatToSow.noData');

  useEffect(() => {
    const ctrl = new AbortController();
    setLoading(true);
    setError(false);
    fetchEvidence({ ...conditions, page_size: EVIDENCE_PAGE_SIZE }, eppo, page, similarity, ctrl.signal)
      .then((res) => setData(res))
      .catch((e: unknown) => {
        if ((e as { name?: string })?.name !== 'AbortError') setError(true);
      })
      .finally(() => {
        if (!ctrl.signal.aborted) setLoading(false);
      });
    return () => ctrl.abort();
  }, [conditions, eppo, page, similarity, retry]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const pages = data ? Math.max(1, Math.ceil(data.total / (data.page_size || EVIDENCE_PAGE_SIZE))) : 1;
  // Forage rows are kg of dry matter (or fresh matter) per ha, not grain: say so in the column.
  const unitLabel = t(kgUnitKey(yieldUnit(data?.items.find((it) => it.basis)?.basis)));
  const cell = (v: string | number | null) => (v == null || v === '' ? noData : v);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={onClose}>
      <div
        role="dialog"
        aria-modal="true"
        aria-label={t('whatToSow.evidence.title', { crop: cropName })}
        className="w-full max-w-2xl max-h-[80vh] overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        <Card padding="md">
          <Stack gap="stack">
            <div className="flex items-center justify-between gap-2">
              <h3 className="text-nkz-base font-semibold text-nkz-text-primary">
                {t('whatToSow.evidence.title', { crop: cropName })}
              </h3>
              <Button variant="ghost" size="sm" onClick={onClose}>{t('whatToSow.evidence.close')}</Button>
            </div>

            {loading && <Skeleton variant="rect" height={160} />}
            {!loading && error && (
              <Inline gap="inline" align="center">
                <p className="text-nkz-sm text-nkz-danger">{t('whatToSow.error.title')}</p>
                <Button variant="secondary" size="sm" onClick={() => setRetry((n) => n + 1)}>
                  {t('whatToSow.error.retry')}
                </Button>
              </Inline>
            )}
            {!loading && !error && data && data.items.length === 0 && (
              <EmptyState title={t('whatToSow.evidence.empty')} />
            )}
            {!loading && !error && data && data.items.length > 0 && (
              <div className="overflow-x-auto">
                <table className="w-full text-nkz-sm">
                  <thead>
                    <tr className="text-left text-nkz-text-muted border-b border-nkz-border">
                      <th className="px-2 py-1">{t('whatToSow.evidence.variety')}</th>
                      <th className="px-2 py-1">{t('whatToSow.evidence.site')}</th>
                      <th className="px-2 py-1">{t('whatToSow.evidence.year')}</th>
                      <th className="px-2 py-1 text-right">{t('whatToSow.evidence.yield', { unit: unitLabel })}</th>
                      <th className="px-2 py-1">{t('whatToSow.evidence.irrigation')}</th>
                      <th className="px-2 py-1">{t('whatToSow.evidence.system')}</th>
                      <th className="px-2 py-1">{t('whatToSow.evidence.source')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.items.map((it) => (
                      <tr key={it.trial_id} className="border-b border-nkz-border text-nkz-text-primary">
                        <td className="px-2 py-1">{cell(it.variety)}</td>
                        <td className="px-2 py-1">{cell(it.site)}</td>
                        <td className="px-2 py-1">{cell(it.year)}</td>
                        <td className="px-2 py-1 text-right tabular-nums">
                          {it.yield_kg_ha == null ? noData : it.yield_kg_ha.toLocaleString(i18n.language, { maximumFractionDigits: 0 })}
                        </td>
                        <td className="px-2 py-1">{cell(it.irrigation_regime)}</td>
                        <td className="px-2 py-1">{cell(it.production_system)}</td>
                        <td className="px-2 py-1">{cell(it.source_id)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {data && data.total > 0 && (
              <div className="flex items-center justify-between gap-2">
                <Button variant="secondary" size="sm" disabled={page <= 1 || loading} onClick={() => setPage((p) => p - 1)}>
                  {t('whatToSow.evidence.prev')}
                </Button>
                <span className="text-nkz-sm text-nkz-text-muted">
                  {t('whatToSow.evidence.pageOf', { page, pages, total: data.total })}
                </span>
                <Button variant="secondary" size="sm" disabled={page >= pages || loading} onClick={() => setPage((p) => p + 1)}>
                  {t('whatToSow.evidence.next')}
                </Button>
              </div>
            )}
          </Stack>
        </Card>
      </div>
    </div>
  );
}
