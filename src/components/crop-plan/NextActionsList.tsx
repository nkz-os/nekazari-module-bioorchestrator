import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card } from '@nekazari/ui-kit';
import { AlertTriangle, Clock } from 'lucide-react';
import type { IssuedOp } from '../../types/cropplan';
import type { AgronomicValue } from '../../types/agronomic';
import { sortByUrgency } from './cropplan.utils';
import AgronomicBadge from '../shared/AgronomicBadge';

const urgencyClass = (u?: string) =>
  u === 'high' ? 'text-nkz-error' : u === 'medium' ? 'text-nkz-warning' : 'text-nkz-text-muted';

/** Section HERO (P1 north-star): the concrete recommended actions, first and prominent. */
const NextActionsList: React.FC<{ ops: IssuedOp[]; agronomicByOp?: Record<string, AgronomicValue> }> = ({
  ops,
  agronomicByOp,
}) => {
  const { t } = useTranslation('bioorchestrator');
  if (!ops?.length) {
    return <p className="text-nkz-sm text-nkz-text-muted">{t('cropPlan.actions.empty')}</p>;
  }
  return (
    <div className="flex flex-col gap-2">
      {sortByUrgency(ops).map((op) => (
        <Card key={op.id} padding="sm">
          <div className="flex items-start justify-between gap-2">
            <div className="min-w-0">
              <div className="text-nkz-sm font-semibold text-nkz-text-primary truncate">
                {t(`cropPlan.opType.${op.operationType}`, op.operationType)}
              </div>
              {op.description && <div className="text-nkz-xs text-nkz-text-secondary">{op.description}</div>}
              {op.dueDate && (
                <div className="text-nkz-2xs text-nkz-text-muted flex items-center gap-1 mt-0.5">
                  <Clock size={11} /> {t('cropPlan.actions.dueBy', { date: op.dueDate })}
                </div>
              )}
              {agronomicByOp?.[op.id] && (
                <div className="mt-1">
                  <AgronomicBadge av={agronomicByOp[op.id]} />
                </div>
              )}
            </div>
            <span className={`text-nkz-2xs font-medium flex items-center gap-1 shrink-0 ${urgencyClass(op.urgency)}`}>
              <AlertTriangle size={11} /> {t(`cropPlan.urgency.${op.urgency ?? 'low'}`)}
            </span>
          </div>
        </Card>
      ))}
    </div>
  );
};

export default NextActionsList;
