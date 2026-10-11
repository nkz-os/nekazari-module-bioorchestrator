import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, EmptyState } from '@nekazari/ui-kit';
import { cropName, formatHa, shortId } from './format';
import { SYMBOL, canPlan, namesIndex, rowsForFarm, warningText } from './matrixModel';
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

interface Props { data: FarmResponse; farmId: string; onOpen: (parcelId: string) => void }

export default function PlanMatrix({ data, farmId, onOpen }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const rows = rowsForFarm(data.parcels, farmId, lang);
  const names = namesIndex(data);
  if (rows.length === 0) {
    return <EmptyState title={t('planning.empty.noParcels')} description={t('planning.empty.noParcelsHint')} />;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-nkz-sm">
        <thead>
          <tr className="text-left text-nkz-text-muted">
            <th className="py-2 pr-4 font-medium">{t('planning.matrix.parcel')}</th>
            <th className="py-2 pr-4 font-medium">{t('planning.matrix.area')}</th>
            <th className="py-2 pr-4 font-medium">{t('planning.matrix.regime')}</th>
            {data.campaigns.map((c) => <th key={c} className="py-2 pr-4 font-medium">{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.parcel_id} className="border-t border-nkz-border align-top">
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
                return (
                  <td key={c} className="py-2 pr-4">
                    {cells.map((cell, i) => <CycleLine key={cell.crop_id ?? `${c}-${i}`} cell={cell} />)}
                    {c === data.campaign && canPlan(row, data.campaign) && (
                      <Button size="sm" variant="secondary" onClick={() => onOpen(row.parcel_id)}>
                        {t(cells.length ? 'planning.matrix.change' : 'planning.matrix.choose')}
                      </Button>
                    )}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <p className="text-nkz-xs text-nkz-text-muted mt-2">{t('planning.matrix.legend')}</p>
    </div>
  );
}
