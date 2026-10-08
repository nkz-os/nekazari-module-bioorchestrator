import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import type { Recommendation } from '../../types/recommend';
import { evidenceNotes, matchedZoneLabels } from './viewModel';

/** What the figure is based on: matched GENVCE zone (+ caveat), country level, unstated regime, organic left out, few trials. */
export default function EvidenceNotes({ rec, skip = [] }: { rec: Recommendation; skip?: readonly string[] }) {
  const { t } = useTranslation('bioorchestrator');
  const notes = evidenceNotes(rec, skip);
  const zones = matchedZoneLabels(rec);
  if (notes.length === 0 && zones.length === 0) return null;
  return (
    <div className="flex flex-col gap-1">
      {zones.length > 0 && (
        <p className="text-nkz-sm text-nkz-info">{t('whatToSow.card.zoneMatched', { zone: zones.join(', ') })}</p>
      )}
      {notes.map((n) => (
        <p key={n.id} className={n.intent === 'info' ? 'text-nkz-sm text-nkz-info' : 'text-nkz-sm text-nkz-warning'}>
          {t(`whatToSow.gap.${n.id}`)}
        </p>
      ))}
    </div>
  );
}
