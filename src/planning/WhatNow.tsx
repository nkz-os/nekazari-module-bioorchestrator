/** "What is due now": crop-health's next job and surprise per parcel, each linking to crop health. */
import React from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from '@nekazari/sdk';
import { Inline, Stack } from '@nekazari/ui-kit';
import type { WorklistRow } from './cropHealthClient';
import { cropName, shortId } from './format';
import { forecastHref } from './urlState';

const JOB_KINDS = new Set(['irrigate', 'sow', 'treat', 'harvest', 'pending_assessment', 'none_until']);
const SURPRISE_CODES = new Set(['vigor_below_expected', 'water_drop', 'phenology_deviation', 'new_high_alert']);

const str = (v: unknown): string | undefined => (typeof v === 'string' && v ? v : undefined);

export default function WhatNow({ rows }: { rows: WorklistRow[] }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const navigate = useNavigate();
  const lang = i18n.language;
  const jobOf = (r: WorklistRow) => (r.nextJob && JOB_KINDS.has(r.nextJob.kind) ? r.nextJob : null);
  const surpriseOf = (r: WorklistRow) => (r.surprise && SURPRISE_CODES.has(r.surprise.code) ? r.surprise : null);
  const due = rows.filter((r) => jobOf(r) || surpriseOf(r));
  const when = (raw: string | undefined): string | undefined => {
    if (!raw) return undefined;
    if (raw === 'today') return t('planning.whatNow.today');
    const d = new Date(raw);
    return Number.isNaN(d.getTime()) ? raw : d.toLocaleDateString(lang);
  };
  const jobText = (r: WorklistRow): string | null => {
    const job = jobOf(r);
    if (!job) return null;
    const detail = job.detail ?? {};
    const date = when(str(detail.date) ?? str(job.when ?? undefined));
    switch (job.kind) {
      case 'sow': {
        const crop = cropName(r.plannedCrop?.names, str(detail.crop) ?? str(detail.species) ?? r.plannedCrop?.species, lang);
        return date ? t('planning.whatNow.job.sowOn', { crop, date }) : t('planning.whatNow.job.sow', { crop });
      }
      case 'harvest':
        return date ? t('planning.whatNow.job.harvestOn', { date }) : t('planning.whatNow.job.harvest');
      case 'treat':
        return t('planning.whatNow.job.treat', { disease: str(detail.disease) ?? '' });
      case 'none_until':
        return date ? t('planning.whatNow.job.none_until', { date }) : t('planning.whatNow.job.none');
      default:
        return t(`planning.whatNow.job.${job.kind}`);
    }
  };
  const nameOf = (r: WorklistRow) => (r.parcelName && !r.parcelName.startsWith('urn:') ? r.parcelName : shortId(r.parcelId));
  return (
    <Stack gap="stack">
      <h2 className="text-nkz-lg font-semibold text-nkz-text-primary">{t('planning.whatNow.title')}</h2>
      {due.length === 0 && <p className="text-nkz-sm text-nkz-text-muted">{t('planning.whatNow.quiet')}</p>}
      {due.map((r) => {
        const job = jobText(r);
        const surprise = surpriseOf(r);
        return (
          <Inline key={r.parcelId} gap="inline" wrap align="center">
            <span className="text-nkz-sm font-medium text-nkz-text-primary">{nameOf(r)}</span>
            {job && <span className="text-nkz-sm text-nkz-text-secondary">{job}</span>}
            {surprise && <span className="text-nkz-sm text-nkz-warning">{t(`planning.whatNow.surprise.${surprise.code}`, { disease: str(surprise.detail?.disease) ?? '' })}</span>}
            <a href={forecastHref(r.parcelId)} className="text-nkz-sm text-nkz-accent-base underline"
              onClick={(e) => {
                if (e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;
                e.preventDefault();
                navigate(forecastHref(r.parcelId));
              }}>{t('planning.whatNow.open')}</a>
          </Inline>
        );
      })}
    </Stack>
  );
}
