import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Card, Checkbox, Inline, Stack } from '@nekazari/ui-kit';
import type { Interval, Recommendation } from '../../types/recommend';
import { formatYield, levelKey, rangeBar, recYieldUnit, soilWarning, unitKey, yieldStatus } from './viewModel';
import { isFewTrials, seasonKey } from './pageModel';
import { typicalSowingMonth } from './compareModel';
import ExpertDetails from './ExpertDetails';
import ForageNotice from './ForageNotice';

/** Crop common name, scientific name as fallback. */
export function useCropName(rec: Recommendation): string {
  const { t } = useTranslation('bioorchestrator');
  return t(`crops.${rec.crop.eppo}`, { defaultValue: rec.crop.scientific_name });
}

/** Observed range with the expected value marked; geometry in % on a page-wide scale. */
export function RangeBarValues({ interval, expected, scaleMax }: {
  interval: Interval; expected: number | null; scaleMax: number | null;
}) {
  const { t } = useTranslation('bioorchestrator');
  const bar = rangeBar(interval, expected, scaleMax);
  if (!bar) return <span className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.noData')}</span>;
  return (
    <div className="relative h-2 w-full rounded-full bg-nkz-surface-sunken overflow-hidden" aria-hidden>
      <div
        className="absolute top-0 h-full rounded-full bg-nkz-accent-soft"
        style={{ left: `${bar.leftPct}%`, width: `${bar.widthPct}%` }}
      />
      <div className="absolute top-0 h-full w-1 bg-nkz-accent-base" style={{ left: `${bar.markerPct}%` }} />
    </div>
  );
}

export function RangeBarView({ rec, scaleMax }: { rec: Recommendation; scaleMax: number | null }) {
  return <RangeBarValues interval={rec.yield.interval} expected={rec.yield.expected_kg_ha} scaleMax={scaleMax} />;
}

const KINDS = ['water', 'soil', 'frost'] as const;

export function levelOf(rec: Recommendation, kind: (typeof KINDS)[number]) {
  return levelKey(kind, rec.suitability[kind].level);
}

interface RecommendationCardProps {
  rec: Recommendation;
  scaleMax: number | null;
  expert: boolean;
  compared: boolean;
  compareDisabled: boolean;
  onToggleCompare: () => void;
  onChooseVariety: () => void;
  onOpenEvidence: () => void;
  onReportValue: () => void;
  /** Absent when the backend has no forage mode (no forage notice then). */
  onViewForage?: () => void;
  policyVersion?: string | null;
}

export default function RecommendationCard({
  rec, scaleMax, expert, compared, compareDisabled, onToggleCompare, onChooseVariety,
  onOpenEvidence, onReportValue, onViewForage, policyVersion,
}: RecommendationCardProps) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const name = useCropName(rec);
  const noData = t('whatToSow.noData');
  const unit = recYieldUnit(rec);
  const unitLabel = t(unitKey(unit));
  const fmt = (n: number | null) => formatYield(n, unit, i18n.language) ?? noData;
  const [lo, hi] = rec.yield.interval;
  const status = yieldStatus(rec);
  // Without a number (basis unknown, presence only) there is no range to draw.
  const hasNumber = status === 'measured' || status === 'none';
  const warning = soilWarning(rec);
  const typicalMonth = typicalSowingMonth(rec.season, i18n.language);

  return (
    <Card padding="md">
      <Stack gap="stack">
        <div>
          <h3 className="text-nkz-base font-semibold text-nkz-text-primary">{name}</h3>
          <p className="text-nkz-sm text-nkz-text-muted">
            {t(seasonKey(rec.crop.sowing_type))}
            {typicalMonth && <> · {t('whatToSow.card.typicalSowing', { month: typicalMonth })}</>}
          </p>
        </div>

        <Stack gap="tight">
          {hasNumber ? (
            <>
              <p className="text-nkz-sm text-nkz-text-secondary">
                {t('whatToSow.card.expectedYield')}:{' '}
                <span className="font-semibold text-nkz-text-primary">
                  {rec.yield.expected_kg_ha == null ? noData : `${fmt(rec.yield.expected_kg_ha)} ${unitLabel}`}
                </span>
              </p>
              <RangeBarView rec={rec} scaleMax={scaleMax} />
              <p className="text-nkz-sm text-nkz-text-muted">
                {lo != null && hi != null
                  ? t('whatToSow.card.observedRange', { low: fmt(lo), high: fmt(hi), unit: unitLabel })
                  : `${t('whatToSow.card.observedRangeLabel')}: ${noData}`}
              </p>
            </>
          ) : (
            <p className="text-nkz-sm font-semibold text-nkz-text-primary">{t(`whatToSow.yieldStatus.${status}`)}</p>
          )}
        </Stack>

        <Inline gap="tight" wrap>
          {KINDS.map((kind) => {
            const level = levelOf(rec, kind);
            return (
              <Badge key={kind} intent={level.intent}>
                {t(`whatToSow.badge.${kind}`)}: {t(level.key)}
              </Badge>
            );
          })}
        </Inline>

        {warning && <p className="text-nkz-sm text-nkz-warning">{warning}</p>}

        {status === 'not_comparable' ? (
          <p className="text-nkz-sm text-nkz-text-muted">
            {t('whatToSow.card.unknownBasisTrials', { count: rec.yield.n_trials })}
          </p>
        ) : (
          <p className="text-nkz-sm text-nkz-text-muted">
            {t('whatToSow.card.trust', { n_trials: rec.yield.n_trials, n_sites: rec.yield.n_sites })}
            {isFewTrials(rec.yield.n_trials) && t('whatToSow.card.few')}
          </p>
        )}
        {rec.trust.similarity === 'vector_v2_fallback' && (
          <p className="text-nkz-sm text-nkz-info">{t('whatToSow.card.similarityV2')}</p>
        )}

        <ForageNotice rec={rec} onViewForage={onViewForage} />

        {expert && <ExpertDetails rec={rec} policyVersion={policyVersion} onOpenEvidence={onOpenEvidence} onReportValue={onReportValue} />}

        <div className="flex flex-wrap items-center justify-between gap-2">
          <Checkbox
            id={`compare-${rec.recommendation_id}`}
            checked={compared}
            disabled={compareDisabled && !compared}
            onChange={onToggleCompare}
            label={t('whatToSow.card.compare')}
          />
          <Button variant="primary" size="sm" onClick={onChooseVariety}>
            {t('whatToSow.card.chooseVariety')}
          </Button>
        </div>
      </Stack>
    </Card>
  );
}
