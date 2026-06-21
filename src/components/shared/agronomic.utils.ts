import type { Confidence } from '../../types/agronomic';

/** Token classes (no custom CSS) + i18n label key for a confidence tier.
 *  The P1 reliability render is intentionally small/glanceable. */
export function confidenceToken(c: Confidence): { text: string; dot: string; label: string } {
  switch (c) {
    case 'high':
      return { text: 'text-nkz-success', dot: 'bg-nkz-success', label: 'badge.confidence.high' };
    case 'medium':
      return { text: 'text-nkz-warning', dot: 'bg-nkz-warning', label: 'badge.confidence.medium' };
    default:
      return { text: 'text-nkz-text-muted', dot: 'bg-nkz-text-muted', label: 'badge.confidence.low' };
  }
}
