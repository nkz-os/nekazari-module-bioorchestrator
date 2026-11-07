import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Input, Inline, Select, Stack } from '@nekazari/ui-kit';
import { deleteDeclaredCrop, EPPO_PATTERN, saveDeclaredCrop } from './api';
import { attemptWrite, declarableCrops } from './completionModel';
import { cropName, formatT } from './format';
import type { CycleCell, ParcelRow, SpeciesFit } from './types';

export const HISTORY_CAMPAIGN_COUNT = 5;

export function pastCampaigns(targetCampaign: number): number[] {
  const currentYear = new Date().getFullYear();
  const list: number[] = [];
  for (let c = targetCampaign - HISTORY_CAMPAIGN_COUNT; c < targetCampaign; c += 1) {
    if (c < currentYear) list.push(c);
  }
  return list;
}

function mainCellOf(row: ParcelRow, campaign: number): CycleCell | null {
  const cells = row.cells[String(campaign)] ?? [];
  return cells.find((c) => c.role === 'main_crop') ?? null;
}

function isDatedCell(cell: CycleCell | null): boolean {
  if (!cell) return false;
  return cell.provenance === 'actual' || cell.start !== null || cell.end !== null;
}

function initialEppo(cell: CycleCell | null, speciesBySlug: Map<string, string>): string {
  if (!cell) return '';
  if (cell.species && speciesBySlug.has(cell.species)) return speciesBySlug.get(cell.species) ?? '';
  if (cell.species_raw && EPPO_PATTERN.test(cell.species_raw)) return cell.species_raw;
  return '';
}

interface RowDraft {
  eppo: string;
  variety: string;
  yieldText: string;
}

interface Props {
  row: ParcelRow;
  campaign: number;
  species: SpeciesFit[];
  onSaved: () => void;
}

export default function ParcelHistorySection({ row, campaign, species, onSaved }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const campaigns = pastCampaigns(campaign);
  const crops = declarableCrops(species);
  const speciesBySlug = new Map<string, string>();
  for (const s of crops) {
    if (s.eppo) speciesBySlug.set(s.species, s.eppo);
  }
  const cropOptions = crops.map((s) => ({
    value: s.eppo as string,
    label: cropName(s.names, s.species, lang),
  }));

  const [drafts, setDrafts] = useState<Record<number, RowDraft>>({});
  const [busyCampaign, setBusyCampaign] = useState<number | null>(null);
  const [savedCampaign, setSavedCampaign] = useState<number | null>(null);
  const [errorKey, setErrorKey] = useState<string | null>(null);

  if (!row.cycles_available || campaigns.length === 0) return null;

  const getDraft = (c: number, cell: CycleCell | null): RowDraft => {
    const existing = drafts[c];
    if (existing) return existing;
    return {
      eppo: initialEppo(cell, speciesBySlug),
      variety: cell?.variety ?? '',
      yieldText: cell?.yield_kg_ha != null ? String(Math.round(cell.yield_kg_ha)) : '',
    };
  };

  const updateDraft = (c: number, cell: CycleCell | null, patch: Partial<RowDraft>) => {
    setDrafts((prev) => ({
      ...prev,
      [c]: { ...getDraft(c, cell), ...patch },
    }));
    setSavedCampaign(null);
    setErrorKey(null);
  };

  const handleSave = async (c: number, cell: CycleCell | null) => {
    const draft = getDraft(c, cell);
    if (!draft.eppo) {
      setErrorKey('invalid_species');
      return;
    }
    const rawYield = draft.yieldText.trim();
    let parsedYield: number | null = null;
    if (rawYield !== '') {
      const n = Number(rawYield.replace(',', '.'));
      if (!Number.isFinite(n) || n <= 0 || n > 200000) {
        setErrorKey('invalid_yield');
        return;
      }
      parsedYield = n;
    }
    setBusyCampaign(c);
    setErrorKey(null);
    setSavedCampaign(null);
    const outcome = await attemptWrite('history', () =>
      saveDeclaredCrop(row.parcel_id, c, draft.eppo, {
        variety: draft.variety.trim() || null,
        yieldKgHa: parsedYield,
      }),
    );
    setBusyCampaign(null);
    if (outcome.ok) {
      setSavedCampaign(c);
      onSaved();
    } else {
      setErrorKey(outcome.errorKey);
    }
  };

  const handleDelete = async (c: number) => {
    setBusyCampaign(c);
    setErrorKey(null);
    setSavedCampaign(null);
    const outcome = await attemptWrite('history', () => deleteDeclaredCrop(row.parcel_id, c));
    setBusyCampaign(null);
    if (outcome.ok) {
      setDrafts((prev) => {
        const next = { ...prev };
        delete next[c];
        return next;
      });
      setSavedCampaign(c);
      onSaved();
    } else {
      setErrorKey(outcome.errorKey);
    }
  };

  return (
    <section aria-label={t('planning.historyEditor.title')} className="rounded-nkz-md border border-nkz-border p-3">
      <Stack gap="stack">
        <div>
          <h3 className="text-nkz-sm font-semibold text-nkz-text-primary">{t('planning.historyEditor.title')}</h3>
          <p className="text-nkz-xs text-nkz-text-muted mt-0.5">{t('planning.historyEditor.hint')}</p>
        </div>
        <div className="space-y-2">
          {campaigns.map((c) => {
            const cell = mainCellOf(row, c);
            const dated = isDatedCell(cell);
            const declared = cell !== null && !dated && cell.provenance === 'declared';
            const draft = getDraft(c, cell);
            const busy = busyCampaign === c;

            if (dated && cell) {
              const name = cropName(cell.names, cell.species_raw, lang);
              const yieldLabel = cell.yield_kg_ha != null
                ? `${formatT(cell.yield_kg_ha / 1000, lang)} t/ha (${Math.round(cell.yield_kg_ha)} kg/ha)`
                : t('planning.historyEditor.noYield');
              return (
                <div key={c} className="flex flex-wrap items-center gap-2 border-t border-nkz-border pt-2 text-nkz-sm">
                  <span className="w-24 font-medium text-nkz-text-primary">{t('planning.historyEditor.campaign', { campaign: c })}</span>
                  <Badge intent="default">{t('planning.provenance.actual')}</Badge>
                  <span className="text-nkz-text-primary">
                    {`${name}${cell.variety ? ` · ${cell.variety}` : ''} · ${yieldLabel}`}
                  </span>
                </div>
              );
            }

            return (
              <div key={c} className="flex flex-wrap items-center gap-2 border-t border-nkz-border pt-2">
                <span className="w-24 text-nkz-sm font-medium text-nkz-text-primary">
                  {t('planning.historyEditor.campaign', { campaign: c })}
                </span>
                <div className="w-44">
                  <Select
                    value={draft.eppo}
                    size="sm"
                    placeholder={t('planning.historyEditor.cropPlaceholder')}
                    options={cropOptions}
                    disabled={busy || cropOptions.length === 0}
                    onValueChange={(v) => updateDraft(c, cell, { eppo: v })}
                  />
                </div>
                <div className="w-40">
                  <Input
                    value={draft.variety}
                    aria-label={`${t('planning.historyEditor.varietyLabel')} ${c}`}
                    placeholder={t('planning.historyEditor.varietyPlaceholder')}
                    disabled={busy}
                    onChange={(e) => updateDraft(c, cell, { variety: e.target.value })}
                  />
                </div>
                <div className="w-36">
                  <Input
                    value={draft.yieldText}
                    inputMode="decimal"
                    aria-label={`${t('planning.historyEditor.yieldLabel')} ${c}`}
                    placeholder={t('planning.historyEditor.yieldPlaceholder')}
                    disabled={busy}
                    onChange={(e) => updateDraft(c, cell, { yieldText: e.target.value })}
                  />
                </div>
                <Inline gap="inline" align="center">
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={busy || !draft.eppo}
                    onClick={() => { void handleSave(c, cell); }}
                  >
                    {t('planning.historyEditor.save')}
                  </Button>
                  {declared && (
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={busy}
                      onClick={() => { void handleDelete(c); }}
                    >
                      {t('planning.historyEditor.clear')}
                    </Button>
                  )}
                </Inline>
              </div>
            );
          })}
        </div>
        {savedCampaign !== null && (
          <p role="status" className="text-nkz-xs text-nkz-success">
            {t('planning.historyEditor.saved', { campaign: savedCampaign })}
          </p>
        )}
        {errorKey && (
          <p role="alert" className="text-nkz-sm text-nkz-danger">
            {t(`planning.wizard.crop.complete.error.${errorKey}`)}
          </p>
        )}
      </Stack>
    </section>
  );
}
