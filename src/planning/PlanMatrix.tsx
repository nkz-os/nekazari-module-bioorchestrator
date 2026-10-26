import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, EmptyState, Inline } from '@nekazari/ui-kit';
import { cropName, formatHa, shortId } from './format';
import { fortnightOf, formatRange } from './fortnight';
import { SYMBOL, lockMessage, lockReason, namesIndex, nextStep, rowsForFarm, simulationAvailable, warningText } from './matrixModel';
import type { CycleCell, FarmResponse } from './types';

export function CycleLine({ cell }: { cell: CycleCell }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const name = cropName(cell.names, cell.species_raw, i18n.language);
  return (
    <span className="block" title={t(`planning.provenance.${cell.provenance}`)}>
      {`${SYMBOL[cell.provenance]} ${name}${cell.variety ? ` · ${cell.variety}` : ''}`}
      {cell.role !== 'main_crop' && <span className="text-nkz-text-muted">{` (${t(`planning.role.${cell.role}`)})`}</span>}
    </span>
  );
}

interface Props {
  data: FarmResponse; farmId: string; onOpen: (parcelId: string) => void; editable?: boolean;
  /** Opens the campaign simulation; the link is offered only when the page wires it. */
  onSimulate?: (parcelId: string) => void;
  /** Opens the crop-health in-season forecast of a sown parcel. */
  onForecast?: (parcelId: string) => void;
}

/** `editable` false (a past campaign) keeps the history readable and offers no way into the form. */
export default function PlanMatrix({ data, farmId, onOpen, editable = true, onSimulate, onForecast }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const rows = rowsForFarm(data.parcels, farmId, lang);
  const names = namesIndex(data);
  if (rows.length === 0) {
    return <EmptyState title={t('planning.empty.noParcels')} description={t('planning.empty.noParcelsHint')} />;
  }
  const step = (row: (typeof rows)[number]) => nextStep(row, data.campaign);
  const renderStep = (row: (typeof rows)[number]) => {
    const s = step(row);
    if (s.kind === 'planned_sowing') {
      const crop = cropName(s.cell.names, s.cell.species_raw, lang);
      const window = s.cell.start ? ` · ${formatRange(fortnightOf(s.cell.start), lang)}` : '';
      return t('planning.next.sowing', { crop, window });
    }
    if (s.kind === 'in_field') {
      return onForecast
        ? <Button size="sm" variant="ghost" onClick={() => onForecast(row.parcel_id)}>{t('planning.next.inField')}</Button>
        : null;
    }
    if (s.kind === 'locked') {
      // A locked parcel says why, once, in this column; a past campaign is only history, and
      // unreadable history is already a row warning.
      if (!editable || s.reason === 'cycles_unavailable') return null;
      return <span className="text-nkz-xs text-nkz-text-muted">{lockMessage(s.reason, data.campaign, t)}</span>;
    }
    if (!editable) return null;
    return (
      <Button size="sm" variant="secondary" onClick={() => onOpen(row.parcel_id)}>
        {t((row.cells[String(data.campaign)] ?? []).length ? 'planning.matrix.change' : 'planning.matrix.choose')}
      </Button>
    );
  };
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-nkz-sm">
        <thead>
          <tr className="text-left text-nkz-text-muted">
            <th className="py-2 pr-4 font-medium">{t('planning.matrix.parcel')}</th>
            <th className="py-2 pr-4 font-medium">{t('planning.matrix.area')}</th>
            <th className="py-2 pr-4 font-medium">{t('planning.matrix.regime')}</th>
            {data.campaigns.map((c) => <th key={c} className="py-2 pr-4 font-medium">{c}</th>)}
            {editable && <th className="py-2 pr-4 font-medium">{t('planning.matrix.nextStep')}</th>}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.parcel_id} className="border-t border-nkz-border">
              <td className="py-2 pr-4">
                <span className="font-medium text-nkz-text-primary">{row.name ?? shortId(row.parcel_id)}</span>
                {row.warnings.map((w, i) => (
                  <span key={`${w.code}-${i}`} className="block text-nkz-xs text-nkz-warning">
                    {`⚠ ${warningText(w, names, lang, t)}`}
                  </span>
                ))}
              </td>
              <td className="py-2 pr-4">{formatHa(row.area_ha, lang) ?? '—'}</td>
              <td className="py-2 pr-4">{t(`planning.regime.${row.regime ?? 'unknown'}`)}</td>
              {data.campaigns.map((c) => {
                const cells = row.cells[String(c)] ?? [];
                const isTarget = c === data.campaign;
                const locked = editable && isTarget ? lockReason(row, data.campaign) : null;
                const plannable = editable && isTarget && locked === null;
                // An undecided parcel gets its choose button in the next-step column, not twice.
                const cellChoose = plannable && !(isTarget && step(row).kind === 'undecided');
                const simulable = Boolean(onSimulate) && isTarget && simulationAvailable(row, data.campaign);
                const forecastable = Boolean(onForecast) && isTarget && simulationAvailable(row, data.campaign);
                return (
                  <td key={c} className="py-2 pr-4">
                    {cells.map((cell, i) => <CycleLine key={cell.crop_id ?? `${c}-${i}`} cell={cell} />)}
                    {(cellChoose || simulable || forecastable) && (
                      <Inline gap="inline" wrap>
                        {cellChoose && (
                          <Button size="sm" variant="secondary" onClick={() => onOpen(row.parcel_id)}>
                            {t(cells.length ? 'planning.matrix.change' : 'planning.matrix.choose')}
                          </Button>
                        )}
                        {simulable && (
                          <Button size="sm" variant="ghost" onClick={() => onSimulate?.(row.parcel_id)}>
                            {t('planning.simulation.open')}
                          </Button>
                        )}
                        {forecastable && (
                          <Button size="sm" variant="ghost" onClick={() => onForecast?.(row.parcel_id)}>
                            {t('planning.simulation.forecast')}
                          </Button>
                        )}
                      </Inline>
                    )}
                  </td>
                );
              })}
              {editable && <td className="py-2 pr-4">{renderStep(row)}</td>}
            </tr>
          ))}
        </tbody>
      </table>
      <p className="text-nkz-xs text-nkz-text-muted mt-2">{t('planning.matrix.legend')}</p>
    </div>
  );
}
