import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Inline, Stack } from '@nekazari/ui-kit';
import SourceAttribution from '../components/shared/SourceAttribution';
import { safeLang } from './format';
import type { VarietiesResponse, VarietyStat } from './types';

interface Props { data: VarietiesResponse; selected: string | null; onSelect: (variety: string | null) => void }

export default function VarietyTable({ data, selected, onSelect }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const [showStale, setShowStale] = useState(false);
  const lang = safeLang(i18n.language);
  const pct = (value: number) => `${value.toLocaleString(lang, { maximumFractionDigits: 0 })} %`;
  if (data.status === 'no_trials') return <p className="text-nkz-sm text-nkz-text-muted">{t('planning.variety.noTrials')}</p>;
  if (data.status === 'needs_climate') return <p className="text-nkz-sm text-nkz-text-muted">{t('planning.variety.needsClimate')}</p>;
  const depth = data.evidence_depth ?? null;
  const depthDetail = depth ? t('planning.variety.depthDetail', { n_trials: depth.n_trials.toLocaleString(lang),
    n_sites: depth.n_sites.toLocaleString(lang),
    years: depth.first_year != null && depth.last_year != null
      ? (depth.first_year === depth.last_year ? `${depth.first_year}` : `${depth.first_year}–${depth.last_year}`) : '—' }) : null;
  // The main table holds the varieties the network still trials; the ones it retired or replaced stay
  // behind a toggle (they are still evidence, a grower may know them). When nothing is current, show all.
  const current = data.varieties.filter((v) => !v.stale);
  const main = current.length > 0 ? current : data.varieties;
  const stale = current.length > 0 ? data.varieties.filter((v) => v.stale) : [];

  const rows = (list: VarietyStat[]) => (
    <div className="overflow-x-auto">
      <table className="w-full text-nkz-sm">
        <thead>
          <tr className="text-left text-nkz-text-muted">
            <th className="py-2 pr-4 font-medium">{t('planning.variety.variety')}</th>
            <th className="py-2 pr-4 font-medium">{t('planning.variety.relative')}</th>
            <th className="py-2 pr-4 font-medium">{t('planning.variety.stability')}</th>
            <th className="py-2 pr-4 font-medium">{t('planning.variety.lowEnv')}</th>
            <th className="py-2 pr-4 font-medium">{t('planning.variety.environments')}</th>
            <th className="py-2 pr-4" />
          </tr>
        </thead>
        <tbody>
          {list.map((v) => (
            <tr key={v.variety} className="border-t border-nkz-border">
              <td className="py-2 pr-4 font-medium text-nkz-text-primary">{v.variety}</td>
              <td className="py-2 pr-4">{pct(v.relative_yield_pct)}</td>
              <td className="py-2 pr-4">{v.stability ? t(`planning.variety.${v.stability}`) : '—'}</td>
              <td className="py-2 pr-4">{v.low_env_relative_yield_pct != null ? pct(v.low_env_relative_yield_pct) : '—'}</td>
              <td className="py-2 pr-4">
                {`${v.n_environments} / ${v.years.length ? `${v.years[0]}–${v.years[v.years.length - 1]}` : '—'}`}
                {v.few_environments && <span className="block text-nkz-xs text-nkz-warning">{t('planning.variety.few')}</span>}
                {v.stale && <span className="block text-nkz-xs text-nkz-warning">{t('planning.variety.stale', { year: v.last_year ?? '—' })}</span>}
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
  );

  return (
    <Stack gap="stack">
      <Inline gap="inline" wrap align="center">
        {data.tier && <Badge intent={data.tier === 'field' ? 'info' : 'warning'}>{t(`planning.variety.tier.${data.tier}`)}</Badge>}
        {depth && depthDetail && (
          <>
            <span title={depthDetail}>
              <Badge intent={depth.level === 'pilot' ? 'warning' : 'info'}>{t(`planning.variety.depth.${depth.level}`, { defaultValue: depth.level })}</Badge>
            </span>
            <span className="text-nkz-xs text-nkz-text-muted">{depthDetail}</span>
          </>
        )}
        {data.data_gaps.map((g) => <Badge key={g} intent="warning">{t(`planning.gap.${g}`, { defaultValue: g })}</Badge>)}
      </Inline>
      {data.varieties.length > 0 && (
        <>
          <p className="text-nkz-xs text-nkz-text-muted">{t('planning.variety.relativeHint')}</p>
          <p className="text-nkz-xs text-nkz-text-muted">{t('planning.variety.lowEnvHint')}</p>
          {rows(main)}
          {stale.length > 0 && (
            <>
              <Button size="sm" variant="ghost" aria-expanded={showStale} onClick={() => setShowStale((s) => !s)}>
                {showStale ? t('planning.variety.staleHide') : t('planning.variety.staleShow', { count: stale.length })}
              </Button>
              {showStale && (
                <>
                  <p className="text-nkz-xs text-nkz-text-muted">{t('planning.variety.staleHint')}</p>
                  {rows(stale)}
                </>
              )}
            </>
          )}
        </>
      )}
      <SourceAttribution attributions={data.attributions} />
    </Stack>
  );
}
