import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Card, EmptyState, Input, ProgressBar, Skeleton, Stack, Tabs } from '@nekazari/ui-kit';
import { useBioApi } from '../../services/api';
import type { Recommendation } from '../../types/recommend';
import { useCropName } from './RecommendationCard';
import { compareRows, grossIncome, levelKey, type LevelKind } from './viewModel';
import {
  barPct, fmtCell, loadPrice, monthSegments, parsePrice, rotationSummary, savePrice, type RotationSummary,
} from './compareModel';

const MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] as const;
const BAR_ROWS = new Set(['expectedYield']);
const LEVEL_ROWS: Record<string, LevelKind> = { water: 'water', soil: 'soil', frost: 'frost' };

function CropHeading({ rec }: { rec: Recommendation }) {
  const name = useCropName(rec);
  return <span className="text-nkz-sm font-semibold text-nkz-text-primary">{name}</span>;
}

function FitLens({ recs }: { recs: Recommendation[] }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const rows = useMemo(() => compareRows(recs), [recs]);
  const noData = t('whatToSow.noData');
  const fmt = (v: string) => fmtCell(v, i18n.language);

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-nkz-sm">
        <thead>
          <tr className="border-b border-nkz-border text-left">
            <th scope="col" className="py-2 pr-3 text-nkz-text-muted font-medium" />
            {recs.map((rec) => (
              <th key={rec.recommendation_id} scope="col" className="py-2 px-2 min-w-[120px]">
                <CropHeading rec={rec} />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const bars = BAR_ROWS.has(row.id) ? barPct(row.cells) : null;
            return (
              <tr key={row.id} className="border-b border-nkz-border">
                <th scope="row" className="py-2 pr-3 text-left font-medium text-nkz-text-muted">
                  {t(row.labelKey)}
                </th>
                {row.cells.map((cell, i) => {
                  const best = row.best.includes(i);
                  const kind = LEVEL_ROWS[row.id];
                  let content: React.ReactNode;
                  if (cell == null && !kind && row.id !== 'trust') content = noData;
                  else if (kind) content = t(levelKey(kind, cell as never).key);
                  else if (row.id === 'trust') content = t(`whatToSow.compare.trustLevel.${cell ?? 'unknown'}`);
                  else content = fmt(cell as string);
                  return (
                    <td key={recs[i].recommendation_id} className="py-2 px-2">
                      <Stack gap="tight">
                        <span className={best ? 'font-semibold text-nkz-text-primary' : 'text-nkz-text-secondary'}>
                          {best && <span aria-label={t('whatToSow.compare.best')}>★ </span>}
                          {content}
                        </span>
                        {bars && bars[i] != null && (
                          <ProgressBar value={bars[i] as number} size="sm" intent={best ? 'positive' : 'default'} />
                        )}
                      </Stack>
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function CalendarRow({ rec }: { rec: Recommendation }) {
  const { t } = useTranslation('bioorchestrator');
  const name = useCropName(rec);
  const segments = monthSegments(rec.season.sowing_window);
  const cycle = rec.season.cycle_days;
  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-2 items-center py-2 border-b border-nkz-border">
      <span className="text-nkz-sm font-medium text-nkz-text-primary">{name}</span>
      <div className="md:col-span-3">
        {segments.length > 0 ? (
          <div className="relative h-3 w-full rounded-full bg-nkz-surface-sunken overflow-hidden" aria-hidden>
            {segments.map((s) => (
              <div key={s.leftPct} className="absolute top-0 h-full bg-nkz-accent-base"
                style={{ left: `${s.leftPct}%`, width: `${s.widthPct}%` }} />
            ))}
          </div>
        ) : (
          <span className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.compare.calendar.none')}</span>
        )}
        <p className="text-nkz-xs text-nkz-text-muted mt-1">
          {cycle != null ? t('whatToSow.compare.calendar.cycle', { days: cycle }) : t('whatToSow.compare.calendar.noCycle')}
        </p>
      </div>
    </div>
  );
}

function CalendarLens({ recs }: { recs: Recommendation[] }) {
  const { t } = useTranslation('bioorchestrator');
  const anyWindow = recs.some((r) => monthSegments(r.season.sowing_window).length > 0);
  return (
    <Stack gap="stack">
      {!anyWindow && (
        <EmptyState title={t('whatToSow.compare.calendar.emptyTitle')}
          description={t('whatToSow.compare.calendar.emptyHint')} />
      )}
      {anyWindow && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-2" aria-hidden>
          <span />
          <div className="md:col-span-3 flex text-center text-nkz-xs text-nkz-text-muted">
            {MONTHS.map((m) => <span key={m} className="flex-1">{t(`whatToSow.compare.calendar.month.${m}`)}</span>)}
          </div>
        </div>
      )}
      <div>{recs.map((rec) => <CalendarRow key={rec.recommendation_id} rec={rec} />)}</div>
    </Stack>
  );
}

type RotationState =
  | { kind: 'loading' }
  | { kind: 'error' }
  | { kind: 'ok'; summary: RotationSummary };

function RotationCard({ rec, parcelId }: { rec: Recommendation; parcelId: string }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const kgN = (n: number | null) => (n == null
    ? t('whatToSow.noData')
    : `${n.toLocaleString(i18n.language, { maximumFractionDigits: 1 })} kg N/ha`);
  const api = useBioApi();
  const name = useCropName(rec);
  const eppo = rec.crop.eppo;
  const [state, setState] = useState<RotationState>({ kind: 'loading' });

  useEffect(() => {
    let cancelled = false;
    setState({ kind: 'loading' });
    api.rotationPlan({ parcel_id: parcelId, years: 2, starting_crop: eppo })
      .then((data: unknown) => {
        if (cancelled) return;
        const summary = rotationSummary(data);
        setState(summary ? { kind: 'ok', summary } : { kind: 'error' });
      })
      .catch(() => {
        if (!cancelled) setState({ kind: 'error' });
      });
    return () => { cancelled = true; };
  }, [api, parcelId, eppo]);

  const next = state.kind === 'ok' ? state.summary.nextCrop : null;
  const nextName = useNextName(next);

  return (
    <Card padding="md">
      <Stack gap="tight">
        <h4 className="text-nkz-sm font-semibold text-nkz-text-primary">{name}</h4>
        {state.kind === 'loading' && <Skeleton variant="rect" height={64} />}
        {state.kind === 'error' && (
          <p className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.compare.rotation.unavailable')}</p>
        )}
        {state.kind === 'ok' && (
          <>
            <p className="text-nkz-sm text-nkz-text-secondary">
              {t('whatToSow.compare.rotation.next')}:{' '}
              <span className="font-semibold text-nkz-text-primary">
                {nextName ?? t('whatToSow.compare.rotation.noSuccessor')}
              </span>
            </p>
            {next != null && (
              <>
                <p className="text-nkz-sm text-nkz-text-secondary">
                  {t('whatToSow.compare.rotation.nFixation')}:{' '}
                  <span className="text-nkz-text-primary">{kgN(state.summary.nFixation)}</span>
                </p>
                <p className="text-nkz-sm text-nkz-text-secondary">
                  {t('whatToSow.compare.rotation.nBalance')}:{' '}
                  <span className="text-nkz-text-primary">{kgN(state.summary.nBalance)}</span>
                </p>
              </>
            )}
            {state.summary.warning && (
              <Badge intent="warning">{state.summary.warning}</Badge>
            )}
            <div>
              <p className="text-nkz-xs text-nkz-text-muted">{t('whatToSow.compare.rotation.pac')}</p>
              {state.summary.pacScore == null ? (
                <span className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.noData')}</span>
              ) : (
                <ProgressBar
                  value={state.summary.pacScore}
                  showLabel
                  intent={state.summary.pacScore >= 80 ? 'positive' : state.summary.pacScore >= 50 ? 'warning' : 'negative'}
                />
              )}
            </div>
          </>
        )}
      </Stack>
    </Card>
  );
}

function useNextName(eppo: string | null): string | null {
  const { t } = useTranslation('bioorchestrator');
  return eppo ? t(`crops.${eppo}`, { defaultValue: eppo }) : null;
}

function RotationLens({ recs, parcelId }: { recs: Recommendation[]; parcelId: string | null }) {
  const { t } = useTranslation('bioorchestrator');
  if (!parcelId) {
    return <EmptyState title={t('whatToSow.compare.rotation.needsParcel')} />;
  }
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      {recs.map((rec) => <RotationCard key={rec.recommendation_id} rec={rec} parcelId={parcelId} />)}
    </div>
  );
}

function EurosLens({ recs }: { recs: Recommendation[] }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const [prices, setPrices] = useState<Record<string, string>>({});
  const textOf = (eppo: string) => prices[eppo] ?? loadPrice(eppo);
  const fmt = (n: number | null) =>
    n == null ? t('whatToSow.noData') : n.toLocaleString(i18n.language, { maximumFractionDigits: 0 });

  const onChange = (eppo: string, text: string) => {
    setPrices((p) => ({ ...p, [eppo]: text }));
    savePrice(eppo, text);
  };

  return (
    <Stack gap="stack">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {recs.map((rec) => {
          const eppo = rec.crop.eppo;
          const text = textOf(eppo);
          const income = grossIncome(rec.yield.expected_kg_ha, rec.yield.interval, parsePrice(text));
          const inputId = `wts-price-${eppo}`;
          return (
            <Card key={rec.recommendation_id} padding="md">
              <Stack gap="tight">
                <CropHeading rec={rec} />
                <label htmlFor={inputId} className="text-nkz-xs text-nkz-text-muted">
                  {t('whatToSow.compare.euros.price')}
                </label>
                <div className="w-40">
                  <Input id={inputId} type="number" size="sm" min={0} step={1} value={text}
                    onChange={(e) => onChange(eppo, e.target.value)} />
                </div>
                {income == null ? (
                  <p className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.compare.euros.addPrice')}</p>
                ) : (
                  <>
                    <p className="text-nkz-base font-semibold text-nkz-text-primary">
                      {income.value == null ? t('whatToSow.noData') : `${fmt(income.value)} €/ha`}
                    </p>
                    <p className="text-nkz-sm text-nkz-text-muted">
                      {income.low != null && income.high != null
                        ? t('whatToSow.compare.euros.range', { low: fmt(income.low), high: fmt(income.high) })
                        : t('whatToSow.compare.euros.noRange')}
                    </p>
                  </>
                )}
              </Stack>
            </Card>
          );
        })}
      </div>
      <p className="text-nkz-xs text-nkz-text-muted">{t('whatToSow.compare.euros.note')}</p>
    </Stack>
  );
}

export interface CompareViewProps {
  recs: Recommendation[];
  parcelId: string | null;
}

export default function CompareView({ recs, parcelId }: CompareViewProps) {
  const { t } = useTranslation('bioorchestrator');
  const [tab, setTab] = useState('fit');
  return (
    <Card padding="md">
      <Tabs defaultValue="fit" value={tab} onValueChange={setTab}>
        <Tabs.List>
          <Tabs.Trigger value="fit">{t('whatToSow.compare.lens.fit')}</Tabs.Trigger>
          <Tabs.Trigger value="calendar">{t('whatToSow.compare.lens.calendar')}</Tabs.Trigger>
          <Tabs.Trigger value="rotation">{t('whatToSow.compare.lens.rotation')}</Tabs.Trigger>
          <Tabs.Trigger value="euros">{t('whatToSow.compare.lens.euros')}</Tabs.Trigger>
        </Tabs.List>
        <Tabs.Content value="fit"><FitLens recs={recs} /></Tabs.Content>
        <Tabs.Content value="calendar"><CalendarLens recs={recs} /></Tabs.Content>
        <Tabs.Content value="rotation"><RotationLens recs={recs} parcelId={parcelId} /></Tabs.Content>
        <Tabs.Content value="euros"><EurosLens recs={recs} /></Tabs.Content>
      </Tabs>
    </Card>
  );
}
