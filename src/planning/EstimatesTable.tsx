import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Stack } from '@nekazari/ui-kit';
import SourceAttribution from '../components/shared/SourceAttribution';
import { sortEstimates, uniqueAttributions, validationTone } from './estimatesModel';
import { formatT } from './format';
import type { EstimatesResponse } from './types';

export default function EstimatesTable({ data }: { data: EstimatesResponse }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const rows = sortEstimates(data.estimates);
  const { inputs } = data;
  return (
    <Stack gap="stack">
      <p className="text-nkz-xs text-nkz-text-muted">{t('planning.estimate.inputs', {
        regime: t(`planning.regime.${inputs.regime}`), management: t(`planning.management.${inputs.management}`),
        sowing: inputs.sowing_date ?? t('planning.estimate.noSowing'),
      })}</p>
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
              <tr key={`${e.provider_id}-${e.basis}`} className="border-t border-nkz-border align-top">
                <td className="py-2 pr-4 font-medium text-nkz-text-primary">
                  {t(`planning.estimate.provider.${e.provider_id}`, { defaultValue: e.provider_id })}
                  {e.validation.status === 'experimental' && (
                    <> <Badge intent={validationTone(e.validation.status)}>{t('planning.estimate.experimental')}</Badge></>
                  )}
                  {e.warnings.map((w) => (
                    <Badge key={w} intent="warning">{t(`planning.gap.${w}`, { defaultValue: w })}</Badge>
                  ))}
                  <span className="block text-nkz-xs text-nkz-text-muted">
                    {t(`planning.estimate.${e.n_kind}`, { count: e.n })}
                  </span>
                </td>
                <td className="py-2 pr-4">{`${formatT(e.percentiles.p10, lang)} – ${formatT(e.percentiles.p90, lang)} ${e.unit}`}</td>
                <td className="py-2 pr-4">{formatT(e.percentiles.p50, lang)}</td>
                <td className="py-2 pr-4">{t(`planning.estimate.basis.${e.basis}`)}</td>
              </tr>
            ))}
            {data.unavailable.map((u) => (
              <tr key={u.provider_id} className="border-t border-nkz-border">
                <td className="py-2 pr-4 font-medium text-nkz-text-primary">
                  {t(`planning.estimate.provider.${u.provider_id}`, { defaultValue: u.provider_id })}
                </td>
                <td className="py-2 pr-4 text-nkz-text-muted" colSpan={3}>
                  {t(`planning.estimate.unavailable.${u.reason}`, { defaultValue: u.reason })}
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
