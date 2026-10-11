import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Inline, Stack } from '@nekazari/ui-kit';
import { cropName } from './format';
import { groupFit, reasonText, reasonTone, sowingText, speciesIndex, splitSharedReasons } from './fitModel';
import type { FitResponse, SpeciesFit } from './types';

interface Props { fit: FitResponse; selected: string | null; onSelect: (species: string) => void }

export default function FitList({ fit, selected, onSelect }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const [showNoFit, setShowNoFit] = useState(false);
  const groups = groupFit(fit.species, lang);
  const names = speciesIndex(fit.species);
  // Parcel-wide reasons (no regime, no soil...) come on every crop: said once, not on each row.
  const { shared, perSpecies } = splitSharedReasons(fit.species);

  const item = (s: SpeciesFit) => {
    const sowing = sowingText(s.sowing, lang, t);
    return (
      <li key={s.species} className="py-2 border-t border-nkz-border">
        <Inline gap="inline" wrap align="center">
          <Button size="sm" variant={selected === s.species ? 'primary' : 'secondary'} aria-pressed={selected === s.species}
            onClick={() => onSelect(s.species)}>
            {cropName(s.names, s.species, lang)}
          </Button>
          {sowing && <span className="text-nkz-xs text-nkz-text-muted">{sowing}</span>}
          {s.has_variety_trials && <Badge intent="info">{t('planning.fit.trials')}</Badge>}
          {(perSpecies.get(s.species) ?? s.reasons).map((r, i) => (
            <Badge key={`${r.code}-${i}`} intent={reasonTone(r)}>{reasonText(r, names, lang, t)}</Badge>
          ))}
        </Inline>
      </li>
    );
  };

  return (
    <Stack gap="stack">
      {shared.length > 0 && (
        <Inline gap="inline" wrap align="center">
          <p className="text-nkz-sm text-nkz-text-muted">{t('planning.fit.shared')}</p>
          {shared.map((r, i) => <Badge key={`${r.code}-${i}`} intent={reasonTone(r)}>{reasonText(r, names, lang, t)}</Badge>)}
        </Inline>
      )}
      {(['fits', 'caveats'] as const).map((v) => (groups[v].length > 0 ? (
        <section key={v} aria-label={t(`planning.fit.${v}`)}>
          <h4 className="text-nkz-sm font-semibold text-nkz-text-primary">{`${t(`planning.fit.${v}`)} (${groups[v].length})`}</h4>
          <ul>{groups[v].map(item)}</ul>
        </section>
      ) : null))}
      {groups.no_fit.length > 0 && (
        <section aria-label={t('planning.fit.no_fit')}>
          <Button size="sm" variant="ghost" aria-expanded={showNoFit} onClick={() => setShowNoFit((x) => !x)}>
            {showNoFit ? t('planning.fit.hideNoFit') : t('planning.fit.showNoFit', { count: groups.no_fit.length })}
          </Button>
          {showNoFit && <ul>{groups.no_fit.map(item)}</ul>}
        </section>
      )}
    </Stack>
  );
}
