import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Stack, Card, Button, Input } from '@nekazari/ui-kit';
import { useCropApi } from '../services/api';

interface Props {
  cropId: string;
  cropName: string;
  onClose: () => void;
  onSuccess: () => void;
}

const STAGES = ['vegetative', 'flowering', 'fruit_development', 'maturity', 'harvest'];

const selectCls = "w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base";
const labelCls = "text-nkz-xs font-medium text-nkz-text-muted block mb-1";

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

  const update = (field: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) =>
    setForm(f => ({ ...f, [field]: e.target.value }));

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
    } catch {
      // Error state handled by SDK
    } finally {
      setSubmitting(false);
    }
  };

  if (success) {
    return <Card padding="md"><p className="text-nkz-success">{t('catalog.contribute.success')}</p></Card>;
  }

  return (
    <Stack gap="stack">
      <h3 className="text-nkz-md font-semibold">{t('catalog.contribute.title')} — {cropName}</h3>

      <div>
        <label className={labelCls}>{t('catalog.contribute.stage')}</label>
        <select className={selectCls} value={form.stage} onChange={update('stage')}>
          <option value="">{t('phenology.anyStage')}</option>
          {STAGES.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      <div><label className={labelCls}>{t('catalog.contribute.kc')}</label><Input type="number" step="0.01" value={form.kc} onChange={update('kc')} /></div>
      <div><label className={labelCls}>{t('catalog.contribute.d1')}</label><Input type="number" step="0.1"  value={form.d1} onChange={update('d1')} /></div>
      <div><label className={labelCls}>{t('catalog.contribute.d2')}</label><Input type="number" step="0.1"  value={form.d2} onChange={update('d2')} /></div>
      <div><label className={labelCls}>{t('catalog.contribute.mds')}</label><Input type="number" step="1"    value={form.mds} onChange={update('mds')} /></div>

      <hr className="border-nkz-border" />
      <h4 className="text-nkz-sm font-semibold">Provenance</h4>
      <div><label className={labelCls}>{t('catalog.contribute.doi')}</label><Input required value={form.doi} onChange={update('doi')} /></div>
      <div><label className={labelCls}>{t('catalog.contribute.author')}</label><Input value={form.author} onChange={update('author')} /></div>
      <div><label className={labelCls}>{t('catalog.contribute.year')}</label><Input type="number" value={form.year} onChange={update('year')} /></div>
      <div><label className={labelCls}>{t('catalog.contribute.institution')}</label><Input value={form.institution} onChange={update('institution')} /></div>
      <div><label className={labelCls}>{t('catalog.contribute.method')}</label><Input value={form.method} onChange={update('method')} /></div>
      <div>
        <label className={labelCls}>{t('catalog.contribute.conditions')}</label>
        <textarea
          className="w-full h-24 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack py-nkz-inline text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base placeholder:text-nkz-text-muted resize-y"
          value={form.conditions}
          onChange={update('conditions')}
          rows={3}
        />
      </div>

      <div className="flex gap-2">
        <Button onClick={handleSubmit} loading={submitting} disabled={!form.doi}>
          {t('catalog.contribute.submit')}
        </Button>
        <Button variant="ghost" onClick={onClose}>{t('catalog.contribute.cancel')}</Button>
      </div>
    </Stack>
  );
}
