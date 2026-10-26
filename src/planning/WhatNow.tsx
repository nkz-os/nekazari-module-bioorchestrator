/** "What is due now": crop-health's next job and surprise per parcel, each linking to crop health. */
import React from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from '@nekazari/sdk';
import { Inline, Stack } from '@nekazari/ui-kit';
import type { WorklistRow } from './cropHealthClient';
import { forecastHref } from './urlState';

export default function WhatNow({ rows }: { rows: WorklistRow[] }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const due = rows.filter((r) => r.nextJob || r.surprise);
  return (
    <Stack gap="stack">
      <h2 className="text-nkz-lg font-semibold text-nkz-text-primary">{t('planning.whatNow.title')}</h2>
      {due.length === 0 && <p className="text-nkz-sm text-nkz-text-muted">{t('planning.whatNow.quiet')}</p>}
      {due.map((r) => {
        const job = r.nextJob;
        const detail = job?.detail ?? {};
        const jobOpts = job?.kind === 'sow' ? { ...detail, crop: detail.crop ?? detail.species } : detail;
        return (
          <Inline key={r.parcelId} gap="inline" wrap align="center">
            <span className="text-nkz-sm font-medium text-nkz-text-primary">{r.parcelName ?? r.parcelId}</span>
            {job && <span className="text-nkz-sm text-nkz-text-secondary">{t(`planning.whatNow.job.${job.kind}`, jobOpts)}</span>}
            {r.surprise && <span className="text-nkz-sm text-nkz-warning">{t(`planning.whatNow.surprise.${r.surprise.code}`, r.surprise.detail ?? {})}</span>}
            <a href={forecastHref(r.parcelId)} className="text-nkz-sm text-nkz-accent-base underline"
              onClick={(e) => { e.preventDefault(); navigate(forecastHref(r.parcelId)); }}>{t('planning.whatNow.open')}</a>
          </Inline>
        );
      })}
    </Stack>
  );
}
