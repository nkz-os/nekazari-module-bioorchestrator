import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Checkbox, Stack, Tooltip } from '@nekazari/ui-kit';
import type { Recommendation } from '../../types/recommend';
import type { Intent } from './viewModel';
import { MIN_TOP_TRIALS } from './viewModel';
import { RangeBarView, levelOf, useCropName } from './RecommendationCard';

const DOT: Record<Intent, string> = {
  positive: 'bg-nkz-success',
  warning: 'bg-nkz-warning',
  negative: 'bg-nkz-danger',
  default: 'bg-nkz-border-strong',
};

interface MoreListProps {
  recs: Recommendation[];
  scaleMax: number | null;
  isCompared: (id: string) => boolean;
  compareFull: boolean;
  onToggleCompare: (id: string) => void;
}

function MoreRow({ rec, scaleMax, compared, compareDisabled, onToggle }: {
  rec: Recommendation; scaleMax: number | null; compared: boolean; compareDisabled: boolean; onToggle: () => void;
}) {
  const { t } = useTranslation('bioorchestrator');
  const name = useCropName(rec);
  return (
    <li className="flex flex-wrap items-center gap-3 py-2 border-b border-nkz-border">
      <span className="w-40 truncate text-nkz-sm font-medium text-nkz-text-primary">
        {name}
        {rec.yield.n_trials < MIN_TOP_TRIALS && (
          <span className="ml-2"><Badge intent="warning">{t('whatToSow.more.fewTrials')}</Badge></span>
        )}
      </span>
      <div className="flex-1 min-w-0"><RangeBarView rec={rec} scaleMax={scaleMax} /></div>
      <span className="flex items-center gap-1">
        {(['water', 'soil', 'frost'] as const).map((kind) => {
          const level = levelOf(rec, kind);
          const word = `${t(`whatToSow.badge.${kind}`)}: ${t(level.key)}`;
          return (
            <Tooltip key={kind} content={word}>
              <span role="img" aria-label={word} className={`inline-block w-2 h-2 rounded-full ${DOT[level.intent]}`} />
            </Tooltip>
          );
        })}
      </span>
      <Checkbox
        id={`compare-more-${rec.recommendation_id}`}
        checked={compared}
        disabled={compareDisabled && !compared}
        onChange={onToggle}
        label={t('whatToSow.card.compare')}
      />
    </li>
  );
}

/** Compact rows for the recommendations outside the top cards; hidden when there are none. */
export default function MoreList({ recs, scaleMax, isCompared, compareFull, onToggleCompare }: MoreListProps) {
  const { t } = useTranslation('bioorchestrator');
  const [open, setOpen] = useState(false);
  if (recs.length === 0) return null;
  return (
    <Stack gap="tight">
      <div>
        <Button variant="ghost" size="sm" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
          {open ? t('whatToSow.more.hide') : t('whatToSow.more.show', { count: recs.length })}
        </Button>
      </div>
      {open && (
        <ul>
          {recs.map((rec) => (
            <MoreRow
              key={rec.recommendation_id}
              rec={rec}
              scaleMax={scaleMax}
              compared={isCompared(rec.recommendation_id)}
              compareDisabled={compareFull}
              onToggle={() => onToggleCompare(rec.recommendation_id)}
            />
          ))}
        </ul>
      )}
    </Stack>
  );
}
