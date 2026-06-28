import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import type { CropPlan, PhenologyStatus } from '../../types/cropplan';
import { buildTimeline } from './cropplan.utils';

const STATUS_FILL: Record<string, string> = {
  active: 'fill-nkz-success',
  planned: 'fill-nkz-border',
  harvested: 'fill-nkz-text-muted',
  terminated: 'fill-nkz-text-muted',
};

/** Horizontal campaign timeline: segment bands + projected stage marks + today. SVG, no custom CSS. */
const CampaignTimeline: React.FC<{ plan: CropPlan; status: PhenologyStatus }> = ({ plan, status }) => {
  const { t } = useTranslation('bioorchestrator');
  if (!plan?.segments?.length) {
    return <p className="text-nkz-sm text-nkz-text-muted">{t('cropPlan.timeline.empty')}</p>;
  }
  const tl = buildTimeline(plan, status, new Date());
  const W = 100;
  const ROW = 14;
  const H = tl.segments.length * (ROW + 4) + 24;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" role="img" aria-label={t('cropPlan.timeline.aria')}>
      {tl.segments.map((b, i) => (
        <g key={b.id}>
          <rect
            x={b.startPct}
            y={i * (ROW + 4)}
            width={Math.max(b.endPct - b.startPct, 1)}
            height={ROW}
            rx={2}
            className={STATUS_FILL[b.status] ?? 'fill-nkz-border'}
          />
          <text x={b.startPct + 1} y={i * (ROW + 4) + ROW - 4} className="fill-nkz-text-primary" fontSize="4">
            {b.label}
          </text>
        </g>
      ))}
      {tl.stages.map((m, i) => (
        <line
          key={i}
          x1={m.pct}
          x2={m.pct}
          y1={0}
          y2={H - 24}
          className={m.current ? 'stroke-nkz-primary' : 'stroke-nkz-border'}
          strokeWidth={m.current ? 0.6 : 0.3}
          strokeDasharray="1 1"
        />
      ))}
      <line x1={tl.todayPct} x2={tl.todayPct} y1={0} y2={H - 20} className="stroke-nkz-error" strokeWidth={0.6} />
      <text x={Math.min(tl.todayPct, 90)} y={H - 14} className="fill-nkz-error" fontSize="4">
        {t('cropPlan.timeline.today')}
      </text>
    </svg>
  );
};

export default CampaignTimeline;
