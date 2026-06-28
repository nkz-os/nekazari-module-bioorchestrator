import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Stack, Card, Button, Input, Select, FormField, FormGrid } from '@nekazari/ui-kit';
import { useCropApi } from '../services/api';

interface Props {
  cropId: string;
  cropName: string;
  onClose: () => void;
  onSuccess: () => void;
}

const STAGES = ['vegetative', 'flowering', 'fruit_development', 'maturity', 'harvest'];

export default function ContributeWizard({ cropId, cropName, onClose, onSuccess }: Props) {
  const { t } = useTranslation('bioorchestrator');
  const { contributeParameter } = useCropApi();
  const [submitting, setSubmitting] = useState(false);
  const [success, setSuccess] = useState(false);
  const [form, setForm] = useState({
    stage: '',
    kc: '',
    d1: '',
    d2: '',
    mds: '',
    doi: '',
    author: '',
    year: '',
    institution: '',
    method: '',
    conditions: '',
  });

  const update = (field: string, value: string) => setForm(f => ({ ...f, [field]: value }));

  const handleSubmit = async () => {
    if (!form.doi) return;
    setSubmitting(true);
    const params: Record<string, number> = {};
    if (form.kc) params.kc = parseFloat(form.kc);
    if (form.d1) params.d1 = parseFloat(form.d1);
    if (form.d2) params.d2 = parseFloat(form.d2);
    if (form.mds) params.mdsRef = parseFloat(form.mds);

    try {
      await contributeParameter({
        crop_id: cropId,
        params,
        provenance: {
          doi: form.doi,
          author: form.author,
          year: form.year ? parseInt(form.year) : undefined,
          institution: form.institution,
          method: form.method,
          conditions: form.conditions,
        },
      });
      setSuccess(true);
      setTimeout(onSuccess, 2000);
    } catch (e) {
      // Error state handled by SDK
    } finally {
      setSubmitting(false);
    }
  };

  if (success) {
    return <Card><p className="text-nkz-success">{t('catalog.contribute.success')}</p></Card>;
  }

  return (
    <Stack gap="section">
      <h3>{t('catalog.contribute.title')} — {cropName}</h3>

      <FormField label={t('catalog.contribute.stage')}>
        <Select
          value={form.stage}
          onValueChange={(v: string) => update('stage', v)}
          options={STAGES.map(s => ({ value: s, label: s }))}
        />
      </FormField>

      <FormGrid columns={2}>
        <FormField label={t('catalog.contribute.kc')}>
          <Input type="number" step="0.01" min={0} max={2} value={form.kc} onChange={e => update('kc', e.target.value)} size="sm" />
        </FormField>
        <FormField label={t('catalog.contribute.d1')}>
          <Input type="number" step="0.1" value={form.d1} onChange={e => update('d1', e.target.value)} size="sm" />
        </FormField>
        <FormField label={t('catalog.contribute.d2')}>
          <Input type="number" step="0.1" value={form.d2} onChange={e => update('d2', e.target.value)} size="sm" />
        </FormField>
        <FormField label={t('catalog.contribute.mds')}>
          <Input type="number" step="1" value={form.mds} onChange={e => update('mds', e.target.value)} size="sm" />
        </FormField>
      </FormGrid>

      <hr />
      <h4>Provenance</h4>
      <FormGrid columns={2}>
        <FormField label={t('catalog.contribute.doi')} required>
          <Input value={form.doi} onChange={e => update('doi', e.target.value)} size="sm" required />
        </FormField>
        <FormField label={t('catalog.contribute.author')}>
          <Input value={form.author} onChange={e => update('author', e.target.value)} size="sm" />
        </FormField>
        <FormField label={t('catalog.contribute.year')}>
          <Input type="number" value={form.year} onChange={e => update('year', e.target.value)} size="sm" />
        </FormField>
        <FormField label={t('catalog.contribute.institution')}>
          <Input value={form.institution} onChange={e => update('institution', e.target.value)} size="sm" />
        </FormField>
        <FormField label={t('catalog.contribute.method')}>
          <Input value={form.method} onChange={e => update('method', e.target.value)} size="sm" />
        </FormField>
      </FormGrid>
      <FormField label={t('catalog.contribute.conditions')}>
        <textarea className="h-24 w-full rounded-nkz-md border border-nkz-border bg-nkz-surface px-3 py-2 text-nkz-sm" value={form.conditions} onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => update('conditions', e.target.value)} />
      </FormField>

      <Stack gap="tight">
        <Button variant="primary" size="sm" onClick={handleSubmit} loading={submitting} disabled={!form.doi}>
          {t('catalog.contribute.submit')}
        </Button>
        <Button variant="ghost" size="sm" onClick={onClose}>{t('catalog.contribute.cancel')}</Button>
      </Stack>
    </Stack>
  );
}
