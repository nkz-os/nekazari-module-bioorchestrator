import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, DetailGrid, DetailItem, Inline, Stack } from '@nekazari/ui-kit';
import type { Recommendation } from '../../types/recommend';
import { describeReferenceScope, formatAssumptionValue } from './pageModel';
import { formatYield, hasListableTrials, recYieldUnit, unitKey } from './viewModel';

interface ExpertDetailsProps {
  rec: Recommendation;
  /** Evidence-policy version the answer was computed under. */
  policyVersion?: string | null;
  onOpenEvidence: () => void;
  onReportValue: () => void;
}

/** Technician view: every number of the card with its basis, so it can be audited. */
export default function ExpertDetails({ rec, policyVersion, onOpenEvidence, onReportValue }: ExpertDetailsProps) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const noData = t('whatToSow.noData');
  const num = (n: number | null | undefined, digits = 0) =>
    n == null ? noData : n.toLocaleString(i18n.language, { maximumFractionDigits: digits });
  const list = (items: string[]) => (items.length ? items.join(', ') : noData);
  const unit = recYieldUnit(rec);
  const unitLabel = t(unitKey(unit));
  const rel = rec.fit.relative_yield_pct;
  const ref = rec.fit.reference;
  const scope = describeReferenceScope(ref.scope, t);
  const years = rec.evidence.years;

  return (
    <Stack gap="tight" className="border-t border-nkz-border pt-3">
      <DetailGrid columns={2}>
        <DetailItem label={t('whatToSow.expert.eppo')} value={rec.crop.eppo} />
        <DetailItem label={t('whatToSow.expert.tier')} value={t(`whatToSow.tier.${rec.evidence.tier ?? 'field'}`)} />
        <DetailItem
          label={t('whatToSow.expert.regionalTrials')}
          value={rec.evidence.regional_trial_count == null ? noData : String(rec.evidence.regional_trial_count)}
        />
        <DetailItem
          label={t('whatToSow.expert.relativeYield')}
          value={rel == null ? noData : `${rel > 0 ? '+' : ''}${num(rel, 1)} %`}
        />
        <DetailItem
          label={t('whatToSow.expert.referenceMedian')}
          value={ref.median_kg_ha == null ? noData
            : t('whatToSow.expert.referenceValue', {
              value: formatYield(ref.median_kg_ha, unit, i18n.language), unit: unitLabel, n: ref.n_trials, scope,
            })}
        />
        {rec.evidence.purpose === 'forage' && (
          <>
            <DetailItem
              label={t('whatToSow.expert.yieldBasis')}
              value={rec.yield.basis ? t(`whatToSow.basis.${rec.yield.basis}`) : noData}
            />
            <DetailItem
              label={t('whatToSow.expert.unknownBasisTrials')}
              value={rec.evidence.unknown_basis_trials == null ? noData : String(rec.evidence.unknown_basis_trials)}
            />
          </>
        )}
        <DetailItem label={t('whatToSow.expert.referenceScope')} value={scope} />
        <DetailItem label={t('whatToSow.expert.cv')} value={num(rec.fit.stability_cv, 2)} />
        <DetailItem label={t('whatToSow.expert.sd')} value={rec.yield.sd == null ? noData : `${formatYield(rec.yield.sd, unit, i18n.language, 2)} ${unitLabel}`} />
        <DetailItem label={t('whatToSow.expert.intervalMethod')} value={rec.yield.interval_method || noData} />
        <DetailItem
          label={t('whatToSow.expert.dataGaps')}
          value={list(rec.trust.data_gaps.map((g) => t(`whatToSow.gap.${g}`, { defaultValue: g })))}
        />
        <DetailItem label={t('whatToSow.expert.sources')} value={list(rec.evidence.sources)} />
        <DetailItem label={t('whatToSow.expert.sites')} value={list(rec.evidence.sites)} />
        <DetailItem label={t('whatToSow.expert.years')} value={years ? `${years[0]}–${years[1]}` : noData} />
        <DetailItem label={t('whatToSow.expert.policy')} value={policyVersion || noData} />
      </DetailGrid>

      <div>
        <p className="text-nkz-sm font-medium text-nkz-text-secondary">{t('whatToSow.expert.assumptions')}</p>
        {rec.assumptions.length === 0 ? (
          <p className="text-nkz-sm text-nkz-text-muted">{noData}</p>
        ) : (
          <ul className="text-nkz-sm text-nkz-text-muted">
            {rec.assumptions.map((a) => (
              <li key={a.id}>
                {a.id} = {formatAssumptionValue(a.value) ?? noData}
                {a.citation ? ` — ${a.citation}` : ''}
              </li>
            ))}
          </ul>
        )}
      </div>

      <Inline gap="inline" wrap>
        {hasListableTrials(rec) && (
          <Button variant="secondary" size="sm" onClick={onOpenEvidence}>{t('whatToSow.expert.viewTrials')}</Button>
        )}
        <Button variant="ghost" size="sm" onClick={onReportValue}>{t('whatToSow.expert.reportValue')}</Button>
      </Inline>
    </Stack>
  );
}
