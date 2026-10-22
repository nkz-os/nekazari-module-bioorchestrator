import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Input, Stack } from '@nekazari/ui-kit';
import { reasonText, reasonTone, speciesIndex } from '../fitModel';
import { cropName } from '../format';
import type { FitResponse, SpeciesFit } from '../types';

const VERDICT_TONE = { fits: 'positive', caveats: 'warning', no_fit: 'negative' } as const;

const fold = (text: string) => text.normalize('NFD').replace(/\p{M}/gu, '').toLowerCase();

/** Search ignores accents and case: "maiz" finds "maíz". */
export const matchesCrop = (name: string, query: string): boolean => fold(name).includes(fold(query.trim()));

/** A crop that does not fit says every reason; the others say their first one. */
export const cardReasons = (s: SpeciesFit) => (s.verdict === 'no_fit' ? s.reasons : s.reasons.slice(0, 1));

export const recommendedCrops = (species: SpeciesFit[], max = 6): SpeciesFit[] =>
  species.filter((s) => s.verdict !== 'no_fit').slice(0, max);

interface Props { fit: FitResponse; selected: string | null; onPick: (sp: SpeciesFit) => void }

export default function StepCrop({ fit, selected, onPick }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const [query, setQuery] = useState('');
  const names = speciesIndex(fit.species);
  const card = (s: SpeciesFit) => (
    <Button key={s.species} variant={selected === s.species ? 'primary' : 'secondary'} aria-pressed={selected === s.species}
      onClick={() => onPick(s)} className="w-full justify-start text-left">
      <span className="block">
        <span className="font-medium">{cropName(s.names, s.species, lang)}</span>{' '}
        <Badge intent={VERDICT_TONE[s.verdict]}>{t(`planning.fit.${s.verdict}`)}</Badge>
        {cardReasons(s).map((r, i) => <Badge key={`${r.code}-${i}`} intent={reasonTone(r)}>{reasonText(r, names, lang, t)}</Badge>)}
      </span>
    </Button>
  );
  const q = query.trim();
  const found = q ? fit.species.filter((s) => matchesCrop(cropName(s.names, s.species, lang), q)) : [];
  return (
    <Stack gap="stack">
      <h3 className="text-nkz-lg font-semibold text-nkz-text-primary">{t('planning.wizard.crop.title')}</h3>
      <p className="text-nkz-sm text-nkz-text-muted">{t('planning.wizard.crop.recommended')}</p>
      <Stack gap="tight">{recommendedCrops(fit.species).map(card)}</Stack>
      <Input value={query} placeholder={t('planning.wizard.crop.search')} aria-label={t('planning.wizard.crop.search')}
        onChange={(e) => setQuery(e.target.value)} />
      {q && (found.length ? <Stack gap="tight">{found.map(card)}</Stack>
        : <p className="text-nkz-sm text-nkz-text-muted">{t('planning.wizard.crop.noResults')}</p>)}
    </Stack>
  );
}
