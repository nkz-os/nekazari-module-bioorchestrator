import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Card, Stack } from '@nekazari/ui-kit';
import type { Recommendation } from '../../types/recommend';
import { LevelDots } from './MoreList';
import { useCropName } from './RecommendationCard';
import ExpertDetails from './ExpertDetails';
import EvidenceNotes from './EvidenceNotes';
import { formatYield, recYieldUnit, unitKey, yieldStatus } from './viewModel';

interface RegionalListProps {
  recs: Recommendation[];
  expert: boolean;
  /** Evidence-policy version the answer was computed under. */
  policyVersion: string | null;
  /** No field recommendation exists: say so instead of leaving this section as the only content unexplained. */
  noFieldEvidence: boolean;
  onOpenEvidence: (rec: Recommendation) => void;
  onReportValue: (rec: Recommendation) => void;
}

function RegionalRow({ rec, expert, policyVersion, onOpenEvidence, onReportValue }: {
  rec: Recommendation; expert: boolean; policyVersion: string | null;
  onOpenEvidence: () => void; onReportValue: () => void;
}) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const name = useCropName(rec);
  const status = yieldStatus(rec);
  const unit = recYieldUnit(rec);
  const value = formatYield(rec.yield.expected_kg_ha, unit, i18n.language);
  const yieldText = status === 'measured' && value != null
    ? `${value} ${t(unitKey(unit))}`
    : status === 'none' ? t('whatToSow.noData') : t(`whatToSow.yieldStatus.${status}`);

  return (
    <li className="py-2 border-b border-nkz-border">
      <Stack gap="tight">
        <div className="flex flex-wrap items-center gap-3">
          <span className="text-nkz-sm font-medium text-nkz-text-primary">{name}</span>
          <Badge intent="info">{t('whatToSow.regional.badge')}</Badge>
          <span className="text-nkz-sm text-nkz-text-secondary">
            {yieldText} · {t('whatToSow.regional.trials', { count: rec.yield.n_trials })}
          </span>
          <LevelDots rec={rec} />
        </div>
        <EvidenceNotes rec={rec} />
        {expert && (
          <ExpertDetails rec={rec} policyVersion={policyVersion} onOpenEvidence={onOpenEvidence} onReportValue={onReportValue} />
        )}
      </Stack>
    </li>
  );
}

/**
 * Crops backed only by national or regional records (no field trial at similar-climate sites).
 * Their own section: those figures are not comparable with the local field average and never
 * enter the cards above.
 */
export default function RegionalList({ recs, expert, policyVersion, noFieldEvidence, onOpenEvidence, onReportValue }: RegionalListProps) {
  const { t } = useTranslation('bioorchestrator');
  if (recs.length === 0) return null;
  return (
    <Card padding="md">
      <Stack gap="tight">
        <div>
          <h3 className="text-nkz-base font-semibold text-nkz-text-primary">{t('whatToSow.regional.title')}</h3>
          <p className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.regional.explanation')}</p>
          {noFieldEvidence && <p className="text-nkz-sm text-nkz-warning">{t('whatToSow.regional.noFieldEvidence')}</p>}
        </div>
        <ul>
          {recs.map((rec) => (
            <RegionalRow
              key={rec.recommendation_id}
              rec={rec}
              expert={expert}
              policyVersion={policyVersion}
              onOpenEvidence={() => onOpenEvidence(rec)}
              onReportValue={() => onReportValue(rec)}
            />
          ))}
        </ul>
      </Stack>
    </Card>
  );
}
