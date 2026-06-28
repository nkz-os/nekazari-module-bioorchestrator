import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import type { AgronomicValue } from '../../types/agronomic';
import { confidenceToken } from './agronomic.utils';

/** P1 reliability render: small, glanceable pill with a source+confidence tooltip.
 *  Secondary by design — it annotates a value/action, never competes with it. */
const AgronomicBadge: React.FC<{ av?: AgronomicValue | null }> = ({ av }) => {
  const { t } = useTranslation('bioorchestrator');
  if (!av) return null;
  const tok = confidenceToken(av.confidence);
  const tip = [
    `${t('badge.source')}: ${av.source?.short ?? '—'}`,
    av.source?.doi ? `DOI: ${av.source.doi}` : '',
    ...(av.notes ?? []),
  ]
    .filter(Boolean)
    .join('\n');
  return (
    <span
      className={`inline-flex items-center gap-1 text-nkz-2xs ${tok.text}`}
      title={tip}
      aria-label={`${t(tok.label)} — ${av.source?.short ?? ''}`}
    >
      <span className={`w-1.5 h-1.5 rounded-full ${tok.dot}`} />
      {av.source?.short ?? t(tok.label)}
    </span>
  );
};

export default AgronomicBadge;
