import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Stack } from '@nekazari/ui-kit';
import SourceAttribution from '../components/shared/SourceAttribution';
import { canRetry, moistureLabel, progressLabel, unavailableKey, sortEstimates, uniqueAttributions, validationTone } from './estimatesModel';
import { formatIsoDate, formatT } from './format';
import type { Estimate, EstimatesResponse } from './types';

/** "low – high t/ha" from the percentiles (P10–P90) or, for a range-only figure, from its range. */
function spread(e: Estimate, lang: string): string {
  const [low, high] = e.percentiles ? [e.percentiles.p10, e.percentiles.p90] : e.range ? [e.range.low, e.range.high] : [null, null];
  return low === null || high === null ? '—' : `${formatT(low, lang)} – ${formatT(high, lang)} ${e.unit}`;
}

export interface SimulationProgress { done: number; total: number | null }

export default function EstimatesTable({ data, progress = null, onRetry }: { data: EstimatesResponse; progress?: SimulationProgress | null; onRetry?: () => void }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const rows = sortEstimates(data.estimates);
  const { inputs } = data;
  return (
    <Stack gap="stack">
      <p className="text-nkz-xs text-nkz-text-muted">{t('planning.estimate.inputs', {
        regime: t(`planning.regime.${inputs.regime}`), management: t(`planning.management.${inputs.management}`),
        sowing: inputs.sowing_date ? formatIsoDate(inputs.sowing_date, lang) : t('planning.estimate.noSowing'),
      })}</p>
      {data.status === 'needs_climate' && <p className="text-nkz-sm text-nkz-text-muted">{t('planning.variety.needsClimate')}</p>}
      {data.disagreement && <p className="text-nkz-sm font-medium text-nkz-warning">{t('planning.estimate.disagreement')}</p>}
      {rows.length === 0 && data.unavailable.length === 0 && <p className="text-nkz-sm text-nkz-text-muted">{t('planning.estimate.none')}</p>}
      <div className="overflow-x-auto">
        <table className="w-full text-nkz-sm">
          <thead>
            <tr className="text-left text-nkz-text-muted">
              <th className="py-2 pr-4 font-medium">{t('planning.estimate.source')}</th>
              <th className="py-2 pr-4 font-medium">{t('planning.estimate.range')}</th>
              <th className="py-2 pr-4 font-medium">{t('planning.estimate.median')}</th>
              <th className="py-2 pr-4 font-medium">{t('planning.estimate.basisCol')}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((e) => (
              <tr key={`${e.provider_id}-${e.basis}`} className="border-t border-nkz-border">
                <td className="py-2 pr-4 font-medium text-nkz-text-primary">
                  {t(`planning.estimate.provider.${e.provider_id}`, { defaultValue: e.provider_id })}
                  {e.validation.status === 'experimental' && (
                    <> <Badge intent={validationTone(e.validation.status)}>{t('planning.estimate.experimental')}</Badge></>
                  )}
                  {e.warnings.map((w) => (
                    <Badge key={w} intent="warning">{t(`planning.gap.${w}`, { defaultValue: w })}</Badge>
                  ))}
                  {e.validation.status === 'validated' && e.validation.metric && (
                    <span className="block text-nkz-xs text-nkz-text-muted">{t('planning.estimate.validatedAs', { metric: e.validation.metric })}</span>
                  )}
                  {e.n > 0 && (
                    <span className="block text-nkz-xs text-nkz-text-muted">
                      {t(`planning.estimate.${e.n_kind}`, { count: e.n })}
                    </span>
                  )}
                  {e.basis === 'water_limited' && <span className="block text-nkz-xs text-nkz-text-muted">{t('planning.estimate.ceiling')}</span>}
                </td>
                <td className="py-2 pr-4">
                  {spread(e, lang)}
                  {e.coverage != null && (
                    <span className="block text-nkz-xs text-nkz-text-muted">{t('planning.estimate.coverage', { pct: Math.round(e.coverage * 100) })}</span>
                  )}
                  {moistureLabel(e.moisture) && (
                    <span className="block text-nkz-xs text-nkz-text-muted">{t(moistureLabel(e.moisture)!.key, moistureLabel(e.moisture)!.params)}</span>
                  )}
                </td>
                <td className="py-2 pr-4">{e.percentiles ? formatT(e.percentiles.p50, lang) : '—'}</td>
                <td className="py-2 pr-4">{t(`planning.estimate.basis.${e.basis}`)}</td>
              </tr>
            ))}
            {data.unavailable.map((u, i) => (
              <tr key={`${u.provider_id}-${u.reason}-${i}`} className="border-t border-nkz-border">
                <td className="py-2 pr-4 font-medium text-nkz-text-primary">
                  {t(`planning.estimate.provider.${u.provider_id}`, { defaultValue: u.provider_id })}
                </td>
                <td className="py-2 pr-4 text-nkz-text-muted" colSpan={3}
                  title={u.code ? t(`cropSimulation.errors.${u.code}`, { defaultValue: t('cropSimulation.errors.generic') }) : undefined}>
                  {u.reason === 'async' && progress !== null
                    ? t(progressLabel(progress).key, progressLabel(progress).params)
                    : t(unavailableKey(u.reason), { defaultValue: t('planning.estimate.unavailable.unknown') })}
                  {u.reason === 'async' && progress !== null && (
                    <span className="block text-nkz-xs text-nkz-text-muted">{t('planning.estimate.firstRunNote')}</span>
                  )}
                  {onRetry && canRetry(u) && (
                    <> <Button size="sm" variant="secondary" onClick={onRetry}>{t('planning.retry')}</Button></>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <SourceAttribution attributions={uniqueAttributions(rows)} />
    </Stack>
  );
}
