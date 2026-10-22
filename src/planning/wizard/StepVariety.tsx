import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Stack } from '@nekazari/ui-kit';
import VarietyTable from '../VarietyTable';
import type { VarietiesResponse, VarietyStat } from '../types';

export type Why = 'productive' | 'stable' | 'balanced';

/** Up to three, no repeats: the highest relative yield, the most stable, then the best stable-or-average left (W4). */
export function topVarieties(vs: VarietyStat[]): { variety: string; why: Why }[] {
  const byYield = [...vs].sort((a, b) => b.relative_yield_pct - a.relative_yield_pct);
  const out: { variety: string; why: Why }[] = [];
  const add = (x: VarietyStat | undefined, why: Why) => { if (x && !out.some((o) => o.variety === x.variety)) out.push({ variety: x.variety, why }); };
  add(byYield[0], 'productive');
  add(byYield.find((x) => x.stability === 'stable'), 'stable');
  add(byYield.find((x) => x.stability !== 'responsive' && !out.some((o) => o.variety === x.variety)), 'balanced');
  return out;
}

interface Props { data: VarietiesResponse; selected: string | null; onSelect: (variety: string | null) => void }

export default function StepVariety({ data, selected, onSelect }: Props) {
  const { t } = useTranslation('bioorchestrator');
  const [all, setAll] = useState(false);
  return (
    <Stack gap="stack">
      <h3 className="text-nkz-lg font-semibold text-nkz-text-primary">{t('planning.wizard.variety.title')}</h3>
      <Button variant={selected === null ? 'primary' : 'secondary'} aria-pressed={selected === null} onClick={() => onSelect(null)}>
        {t('planning.wizard.variety.none')}
      </Button>
      {topVarieties(data.varieties ?? []).map((x) => (
        <Button key={x.variety} variant={selected === x.variety ? 'primary' : 'secondary'} aria-pressed={selected === x.variety}
          onClick={() => onSelect(x.variety)} className="w-full justify-start text-left">
          <span className="font-medium">{x.variety}</span>{' · '}{t(`planning.wizard.variety.why.${x.why}`)}
        </Button>
      ))}
      <Button variant="ghost" onClick={() => setAll((a) => !a)}>{t('planning.wizard.variety.all')}</Button>
      {all && <VarietyTable data={data} selected={selected} onSelect={onSelect} />}
    </Stack>
  );
}
