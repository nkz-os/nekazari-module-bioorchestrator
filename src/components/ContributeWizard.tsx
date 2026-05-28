import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Stack, Card, Button, Input, Textarea, Select } from '@nekazari/ui-kit';
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
    <Stack gap="md">
      <h3>{t('catalog.contribute.title')} — {cropName}</h3>

      <Select
        label={t('catalog.contribute.stage')}
        value={form.stage}
        onChange={e => update('stage', e.target.value)}
        options={STAGES.map(s => ({ value: s, label: s }))}
      />

      <Input label={t('catalog.contribute.kc')} type="number" step="0.01"
             value={form.kc} onChange={e => update('kc', e.target.value)} />
      <Input label={t('catalog.contribute.d1')} type="number" step="0.1"
             value={form.d1} onChange={e => update('d1', e.target.value)} />
      <Input label={t('catalog.contribute.d2')} type="number" step="0.1"
             value={form.d2} onChange={e => update('d2', e.target.value)} />
      <Input label={t('catalog.contribute.mds')} type="number" step="1"
             value={form.mds} onChange={e => update('mds', e.target.value)} />

      <hr />
      <h4>Provenance</h4>
      <Input label={t('catalog.contribute.doi')} required
             value={form.doi} onChange={e => update('doi', e.target.value)} />
      <Input label={t('catalog.contribute.author')}
             value={form.author} onChange={e => update('author', e.target.value)} />
      <Input label={t('catalog.contribute.year')} type="number"
             value={form.year} onChange={e => update('year', e.target.value)} />
      <Input label={t('catalog.contribute.institution')}
             value={form.institution} onChange={e => update('institution', e.target.value)} />
      <Input label={t('catalog.contribute.method')}
             value={form.method} onChange={e => update('method', e.target.value)} />
      <Textarea label={t('catalog.contribute.conditions')}
                value={form.conditions} onChange={e => update('conditions', e.target.value)} />

      <Stack direction="row" gap="sm">
        <Button onClick={handleSubmit} loading={submitting} disabled={!form.doi}>
          {t('catalog.contribute.submit')}
        </Button>
        <Button variant="ghost" onClick={onClose}>{t('catalog.contribute.cancel')}</Button>
      </Stack>
    </Stack>
  );
}
