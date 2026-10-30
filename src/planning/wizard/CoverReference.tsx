import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Stack } from '@nekazari/ui-kit';
import SourceAttribution from '../../components/shared/SourceAttribution';
import { fetchCoverReference } from '../api';
import { safeLang } from '../format';
import LoadingLine from '../LoadingLine';
import type { CoverReferenceResponse, CoverStat } from '../types';
import { useLoad, type LoadView } from '../useLoad';

export type CoverSeason = 'autumn' | 'spring';

/** Season of the cover trials that matches its sowing: a start in August to December is autumn, January to July spring. */
export function coverSeason(sowingStart: string): CoverSeason {
  return Number(sowingStart.slice(5, 7)) >= 8 ? 'autumn' : 'spring';
}

/** The view of a reference answer; pure, so it renders without a request. */
export function CoverReferenceCard({ view }: { view: LoadView<CoverReferenceResponse> }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = safeLang(i18n.language);
  const num = (v: number) => v.toLocaleString(lang, { maximumFractionDigits: 2 });
  const stat = (s: CoverStat | null | undefined) => (!s ? '—' : s.se == null
    ? t('planning.wizard.cover.ref.valueNoSe', { mean: num(s.mean), n: s.n ?? '?' })
    : t('planning.wizard.cover.ref.value', { mean: num(s.mean), se: num(s.se), n: s.n ?? '?' }));
  const data = view.data;
  return (
    <Card padding="md">
      <Stack gap="tight">
        <h4 className="text-nkz-base font-semibold text-nkz-text-primary">{t('planning.wizard.cover.ref.title')}</h4>
        {view.loading ? <LoadingLine text={t('planning.wizard.cover.ref.loading')} />
          : view.error || !data ? <p className="text-nkz-sm text-nkz-warning">{t('planning.wizard.cover.ref.error')}</p>
          : data.status === 'no_data' || !data.aerial ? <p className="text-nkz-sm text-nkz-text-muted">{t('planning.wizard.cover.ref.noData')}</p>
          : (
            <>
              <p className="text-nkz-xs text-nkz-text-muted">
                {data.season_used === 'unspecified' ? t('planning.wizard.cover.ref.seasonUnspecified')
                  : data.season_used === 'spring' ? t('planning.wizard.cover.ref.seasonSpring') : t('planning.wizard.cover.ref.seasonAutumn')}
              </p>
              <dl className="text-nkz-sm text-nkz-text-primary">
                <dt className="text-nkz-text-muted">{t('planning.wizard.cover.ref.dm')}</dt><dd>{stat(data.aerial.dm_t_ha)}</dd>
                <dt className="text-nkz-text-muted">{t('planning.wizard.cover.ref.nPct')}</dt><dd>{stat(data.aerial.n_pct)}</dd>
                <dt className="text-nkz-text-muted">{t('planning.wizard.cover.ref.cn')}</dt><dd>{stat(data.aerial.c_n)}</dd>
              </dl>
              {data.aerial.n_accumulated_kg_ha != null && (
                <p className="text-nkz-sm text-nkz-text-primary">
                  {t('planning.wizard.cover.ref.nAcc', { value: num(data.aerial.n_accumulated_kg_ha) })}
                  <span className="block text-nkz-xs text-nkz-text-muted">{t('planning.wizard.cover.ref.nAccNote')}</span>
                </p>
              )}
              <p className="text-nkz-xs text-nkz-text-muted">{t('planning.wizard.cover.ref.coverage')}</p>
              <SourceAttribution attributions={data.attributions} />
            </>
          )}
      </Stack>
    </Card>
  );
}

/** Reference card of a listed cover species for the season of the chosen sowing. */
export default function CoverReference({ species, sowingStart }: { species: string; sowingStart: string }) {
  const season = coverSeason(sowingStart);
  const view = useLoad((signal) => fetchCoverReference(species, season, signal), [species, season]);
  return <CoverReferenceCard view={view} />;
}
