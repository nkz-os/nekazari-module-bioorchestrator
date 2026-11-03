import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Input, Stack } from '@nekazari/ui-kit';
import { Info } from 'lucide-react';
import SourceAttribution from '../../components/shared/SourceAttribution';
import { num, regionalLine, regionalName, regionalUnavailableKey } from '../regionalYield';
import { irrigatedParcel, irrigationLine, irrigationUnavailableKey, isPending } from '../irrigationNeed';
import { reasonText, reasonTone, speciesIndex } from '../fitModel';
import { cropName } from '../format';
import { askedByBar, completionNeeds, remainingNeeds } from '../completionModel';
import type { CompletionState, WriteKind } from '../completionModel';
import type { FitResponse, ParcelRow, Reason, RegionalYield, SpeciesFit, TFn } from '../types';
import CompletionBar from './CompletionBar';

const VERDICT_TONE = { fits: 'positive', caveats: 'warning', no_fit: 'negative' } as const;

const fold = (text: string) => text.normalize('NFD').replace(/\p{M}/gu, '').toLowerCase();

/** Search ignores accents and case: "maiz" finds "maíz". */
export const matchesCrop = (name: string, query: string): boolean => fold(name).includes(fold(query.trim()));

/** A crop that does not fit says every reason; the others say their first one. */
export const cardReasons = (s: SpeciesFit, hide: (r: Reason) => boolean = () => false) => {
  const shown = s.reasons.filter((r) => !hide(r));
  return s.verdict === 'no_fit' ? shown : shown.slice(0, 1);
};

/** A species sown only as a cover is offered in the cover step, never here. */
export const mainCrops = (species: SpeciesFit[]): SpeciesFit[] => species.filter((s) => !s.cover_only);

export const recommendedCrops = (species: SpeciesFit[], max = 6): SpeciesFit[] =>
  mainCrops(species).filter((s) => s.verdict !== 'no_fit').slice(0, max);

/** The full label of a regional figure: where, which years, what it mixes, and that it is not a forecast. */
export function RegionalDetail({ s, lang, t }: { s: SpeciesFit; lang: string; t: TFn }) {
  const r: RegionalYield | null | undefined = s.regional_yield;
  const base = 'planning.wizard.crop.regional';
  if (!r) return <p className="text-nkz-xs text-nkz-text-muted">{t(regionalUnavailableKey(s.regional_yield_unavailable))}</p>;
  return (
    <Stack gap="tight">
      <p className="text-nkz-xs text-nkz-text-muted">
        {t(`${base}.detailRegion`, { region: regionalName(r, lang), from: r.first_year, to: r.last_year })}{' '}
        {t(`${base}.detailMixed`)}{' '}
        {t(r.moisture.pct == null ? `${base}.detailMoistureNoPct` : `${base}.detailMoisture`,
          { pct: r.moisture.pct == null ? '' : num(r.moisture.pct, lang, 1, false) })}{' '}
        {t(`${base}.detailNotForecast`)}
      </p>
      <SourceAttribution attributions={r.attributions} />
    </Stack>
  );
}

/** The seasonal irrigation need in full: the years it covers, how it was computed, and its sources. */
export function IrrigationDetail({ s, lang, t }: { s: SpeciesFit; lang: string; t: TFn }) {
  const n = s.irrigation_need;
  const base = 'planning.wizard.crop.irrigation';
  if (!n) return <p className="text-nkz-xs text-nkz-text-muted">{t(irrigationUnavailableKey(s.irrigation_need_unavailable))}</p>;
  return (
    <Stack gap="tight">
      <p className="text-nkz-xs text-nkz-text-muted">
        {t(`${base}.detailYears`, { from: n.first_year, to: n.last_year, n: n.n_years })}{' '}
        {t(`${base}.detailNet`)}{' '}
        {t(`${base}.detailNotForecast`)}
      </p>
      <SourceAttribution attributions={n.attributions} />
    </Stack>
  );
}

interface Props {
  fit: FitResponse; selected: string | null; onPick: (sp: SpeciesFit) => void;
  /** With the parcel and a way to read the fit again, missing data is asked for above the crops. */
  row?: Pick<ParcelRow, 'parcel_id' | 'regime' | 'regime_source'>; onCompleted?: (kind: WriteKind) => void;
  /** Skipped and saved controls; kept by the wizard because this step unmounts while the fit reloads. */
  completion?: CompletionState;
}

export default function StepCrop({ fit, selected, onPick, row, onCompleted, completion }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const [query, setQuery] = useState('');
  const [infoOpen, setInfoOpen] = useState<string | null>(null);
  const names = speciesIndex(fit.species);
  const irrigated = irrigatedParcel(fit.facts?.regime);
  const asked = row && onCompleted && completion
    ? askedByBar(remainingNeeds(completionNeeds(fit, row), completion.skipped)) : undefined;
  const card = (s: SpeciesFit) => (
    <div key={s.species}>
      <Button variant={selected === s.species ? 'primary' : 'secondary'} aria-pressed={selected === s.species}
        onClick={() => onPick(s)} className="w-full justify-start text-left">
        <span className="block">
          <span className="font-medium">{cropName(s.names, s.species, lang)}</span>{' '}
          <Badge intent={VERDICT_TONE[s.verdict]}>{t(`planning.fit.${s.verdict}`)}</Badge>
          {cardReasons(s, asked).map((r, i) => <Badge key={`${r.code}-${i}`} intent={reasonTone(r)}>{reasonText(r, names, lang, t)}</Badge>)}
        </span>
      </Button>
      <div className="flex items-center gap-2 px-2">
        <span className="text-nkz-xs text-nkz-text-muted">
          {s.regional_yield ? regionalLine(s.regional_yield, lang, t) : t('planning.wizard.crop.regional.none')}
        </span>
        <Button size="sm" variant="ghost" aria-expanded={infoOpen === s.species} aria-label={t('planning.wizard.crop.regional.info')}
          onClick={() => setInfoOpen(infoOpen === s.species ? null : s.species)}>
          <Info size={14} aria-hidden="true" />
        </Button>
      </div>
      {irrigated && (s.irrigation_need || isPending(s)) && (
        <p className="px-2 text-nkz-xs text-nkz-text-muted">
          {s.irrigation_need ? irrigationLine(s.irrigation_need, lang, t) : t('planning.wizard.crop.irrigation.pending')}
        </p>
      )}
      {infoOpen === s.species && (
        <div className="px-2">
          <Stack gap="tight">
            <RegionalDetail s={s} lang={lang} t={t} />
            {irrigated && <IrrigationDetail s={s} lang={lang} t={t} />}
          </Stack>
        </div>
      )}
    </div>
  );
  const q = query.trim();
  const found = q ? mainCrops(fit.species).filter((s) => matchesCrop(cropName(s.names, s.species, lang), q)) : [];
  return (
    <Stack gap="stack">
      <h3 className="text-nkz-lg font-semibold text-nkz-text-primary">{t('planning.wizard.crop.title')}</h3>
      {row && onCompleted && completion && <CompletionBar fit={fit} row={row} onSaved={onCompleted} state={completion} />}
      <p className="text-nkz-sm text-nkz-text-muted">{t('planning.wizard.crop.recommended')}</p>
      <Stack gap="tight">{recommendedCrops(fit.species).map(card)}</Stack>
      <Input value={query} placeholder={t('planning.wizard.crop.search')} aria-label={t('planning.wizard.crop.search')}
        onChange={(e) => setQuery(e.target.value)} />
      {q && (found.length ? <Stack gap="tight">{found.map(card)}</Stack>
        : <p className="text-nkz-sm text-nkz-text-muted">{t('planning.wizard.crop.noResults')}</p>)}
    </Stack>
  );
}
