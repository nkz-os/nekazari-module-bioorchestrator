import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Card, Inline } from '@nekazari/ui-kit';
import type { Recommendation } from '../../types/recommend';
import type { SourceAttributionItem } from '../../types/attribution';
import CompareView from './CompareView';

export const MIN_COMPARE = 2;

export interface CompareTrayProps {
  selection: Recommendation[];
  parcelId: string | null;
  /** `attributions` of the recommend answer the selection comes from. */
  attributions?: SourceAttributionItem[];
  onClear: () => void;
}

/** Sticky bar with 2–4 checked crops; "Comparar" opens the four-lens view above it. */
export default function CompareTray({ selection, parcelId, attributions, onClear }: CompareTrayProps) {
  const { t } = useTranslation('bioorchestrator');
  const [open, setOpen] = useState(false);
  const enough = selection.length >= MIN_COMPARE;
  useEffect(() => {
    if (!enough) setOpen(false);
  }, [enough]);
  if (!enough) return null;

  return (
    <>
      {open && <CompareView recs={selection} parcelId={parcelId} attributions={attributions} />}
      <div className="sticky bottom-0 z-10">
        <Card padding="sm">
          <Inline gap="inline" align="center" wrap>
            <span className="text-nkz-sm font-medium text-nkz-text-primary">
              {t('whatToSow.compare.selected', { count: selection.length })}
            </span>
            <Button size="sm" variant="primary" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
              {open ? t('whatToSow.compare.hide') : t('whatToSow.compare.open')}
            </Button>
            <Button size="sm" variant="secondary" onClick={() => { setOpen(false); onClear(); }}>
              {t('whatToSow.compare.clear')}
            </Button>
          </Inline>
        </Card>
      </div>
    </>
  );
}
