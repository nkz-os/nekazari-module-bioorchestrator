import React from 'react';
import { useTranslation } from 'react-i18next';
import { Card, Button } from '@nekazari/ui-kit';
import type { CropPlan, PhenologyStatus } from '../../types/cropplan';
import { pickActiveSegment } from './cropplan.utils';
import AgronomicBadge from '../shared/AgronomicBadge';

const statusChip: Record<string, string> = {
  active: 'text-nkz-success',
  planned: 'text-nkz-text-secondary',
  harvested: 'text-nkz-text-muted',
  terminated: 'text-nkz-text-muted',
};

/** Rotation segments with status chip, planned vs actual dates, and the active stage. */
const PlanSegments: React.FC<{ plan: CropPlan; status: PhenologyStatus; onCreatePlan?: () => void }> = ({
  plan,
  status,
  onCreatePlan,
}) => {
  const { t } = useTranslation('bioorchestrator');
  if (!plan?.segments?.length) {
    return (
      <div className="text-nkz-sm text-nkz-text-muted">
        {t('cropPlan.segments.empty')}
        {onCreatePlan && (
          <Button variant="ghost" size="sm" onClick={onCreatePlan} className="ml-2">
            {t('cropPlan.segments.create')}
          </Button>
        )}
      </div>
    );
  }
  const active = pickActiveSegment(plan);
  return (
    <div className="flex flex-col gap-2">
      {plan.segments.map((s) => (
        <Card key={s.id} padding="sm">
          <div className="flex items-center justify-between">
            <span className="text-nkz-sm font-medium text-nkz-text-primary">
              {s.species || `#${s.seq}`}
              {s.variety ? ` · ${s.variety}` : ''}
            </span>
            <span className={`text-nkz-2xs font-semibold uppercase ${statusChip[s.status] ?? ''}`}>
              {t(`cropPlan.status.${s.status}`)}
            </span>
          </div>
          <div className="text-nkz-2xs text-nkz-text-muted mt-0.5">
            {t('cropPlan.segments.planned')}: {s.sowingWindowStart ?? '—'}
            {s.expectedTerminationDate ? ` → ${s.expectedTerminationDate}` : ''}
            {s.plantingDate && (
              <>
                {' · '}
                {t('cropPlan.segments.actual')}: {s.plantingDate}
              </>
            )}
          </div>
          {active?.id === s.id && status.currentStage && (
            <div className="text-nkz-xs text-nkz-text-secondary mt-1 flex items-center gap-1">
              {t('cropPlan.segments.stage')}: {t(`cropPlan.stage.${status.currentStage}`, status.currentStage)}
              <AgronomicBadge av={status.agronomic?.currentStage} />
            </div>
          )}
        </Card>
      ))}
    </div>
  );
};

export default PlanSegments;
