import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { formatRange, monthColumns, monthLabel, sameFortnight, type Fortnight } from './fortnight';

interface Props {
  fortnights: Fortnight[]; recommended?: Fortnight[]; selected: Fortnight | null;
  bar?: { start: string; end: string } | null; onSelect?: (f: Fortnight) => void; label: string;
  /** When given, only these cells can be chosen; the others are shown read-only. */
  selectable?: Fortnight[];
}

/** Months × two halves (W8): the recommended window shaded, the choice marked, an optional bar. */
export default function FortnightStrip({ fortnights, recommended = [], selected, bar = null, onSelect, label, selectable }: Props) {
  const { i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const inBar = (f: Fortnight) => Boolean(bar && f.start >= bar.start && f.start <= bar.end);
  return (
    <div className="overflow-x-auto" role="group" aria-label={label}>
      <div className="flex gap-1">
        {monthColumns(fortnights).map((col) => (
          <div key={col.key} className="flex flex-col items-center gap-1">
            <span className="text-nkz-xs text-nkz-text-muted">{monthLabel(col.year, col.month, lang)}</span>
            <div className="flex gap-1">
              {col.halves.map((f) => {
                const rec = recommended.some((r) => sameFortnight(r, f));
                const sel = sameFortnight(selected, f);
                const cls = `h-8 w-8 rounded-nkz-sm border text-nkz-xs ${sel ? 'border-nkz-accent-base bg-nkz-accent-base text-nkz-text-on-accent'
                  : rec ? 'border-nkz-accent-base bg-nkz-accent-soft text-nkz-text-primary' : 'border-nkz-border text-nkz-text-muted'}${inBar(f) ? ' ring-2 ring-nkz-success' : ''}`;
                const day = f.start.slice(8, 10) === '01' ? '1' : '16';
                const props = { 'aria-label': formatRange(f, lang), 'data-recommended': String(rec), 'data-bar': String(inBar(f)), className: cls };
                return onSelect && (!selectable || selectable.some((x) => sameFortnight(x, f)))
                  ? <button key={f.start} type="button" aria-pressed={sel} onClick={() => onSelect(f)} {...props}>{day}</button>
                  : <span key={f.start} {...props}>{day}</span>;
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
