import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Inline, Stack } from '@nekazari/ui-kit';
import SourceAttribution from '../components/shared/SourceAttribution';
import { safeLang } from './format';
import type { VarietiesResponse } from './types';

interface Props { data: VarietiesResponse; selected: string | null; onSelect: (variety: string | null) => void }

export default function VarietyTable({ data, selected, onSelect }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  if (data.status === 'no_trials') return <p className="text-nkz-sm text-nkz-text-muted">{t('planning.variety.noTrials')}</p>;
  if (data.status === 'needs_climate') return <p className="text-nkz-sm text-nkz-text-muted">{t('planning.variety.needsClimate')}</p>;
  return (
    <Stack gap="stack">
      <Inline gap="inline" wrap align="center">
        {data.tier && <Badge intent={data.tier === 'field' ? 'info' : 'warning'}>{t(`planning.variety.tier.${data.tier}`)}</Badge>}
        {data.data_gaps.map((g) => <Badge key={g} intent="warning">{t(`planning.gap.${g}`, { defaultValue: g })}</Badge>)}
      </Inline>
      {data.varieties.length > 0 && (
        <>
          <p className="text-nkz-xs text-nkz-text-muted">{t('planning.variety.relativeHint')}</p>
          <div className="overflow-x-auto">
            <table className="w-full text-nkz-sm">
              <thead>
                <tr className="text-left text-nkz-text-muted">
                  <th className="py-2 pr-4 font-medium">{t('planning.variety.variety')}</th>
                  <th className="py-2 pr-4 font-medium">{t('planning.variety.relative')}</th>
                  <th className="py-2 pr-4 font-medium">{t('planning.variety.stability')}</th>
                  <th className="py-2 pr-4 font-medium">{t('planning.variety.environments')}</th>
                  <th className="py-2 pr-4" />
                </tr>
              </thead>
              <tbody>
                {data.varieties.map((v) => (
                  <tr key={v.variety} className="border-t border-nkz-border">
                    <td className="py-2 pr-4 font-medium text-nkz-text-primary">{v.variety}</td>
                    <td className="py-2 pr-4">{`${v.relative_yield_pct.toLocaleString(safeLang(i18n.language), { maximumFractionDigits: 0 })} %`}</td>
                    <td className="py-2 pr-4">{v.stability ? t(`planning.variety.${v.stability}`) : '—'}</td>
                    <td className="py-2 pr-4">
                      {`${v.n_environments} / ${v.years.length ? `${v.years[0]}–${v.years[v.years.length - 1]}` : '—'}`}
                      {v.few_environments && <span className="block text-nkz-xs text-nkz-warning">{t('planning.variety.few')}</span>}
                    </td>
                    <td className="py-2 pr-4">
                      <Button size="sm" variant={selected === v.variety ? 'primary' : 'secondary'} aria-pressed={selected === v.variety}
                        onClick={() => onSelect(selected === v.variety ? null : v.variety)}>
                        {t(selected === v.variety ? 'planning.variety.chosen' : 'planning.variety.choose')}
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
      <SourceAttribution attributions={data.attributions} />
    </Stack>
  );
}
