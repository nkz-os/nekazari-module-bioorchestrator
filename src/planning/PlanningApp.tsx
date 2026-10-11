// src/planning/PlanningApp.tsx
import React, { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useAuth, useTranslation } from '@nekazari/sdk';
import { Button, Card, Inline, Select, Spinner, Stack } from '@nekazari/ui-kit';
import { Sprout } from 'lucide-react';
import DisclaimerFooter from '../components/DisclaimerFooter';
import AdminTools from './AdminTools';
import { fetchFarm } from './api';
import { cropName, formatHa } from './format';
import ParcelDetail from './ParcelDetail';
import PlanMatrix from './PlanMatrix';
import SourcesFooter from './SourcesFooter';
import type { FarmResponse } from './types';
import { useLoad } from './useLoad';

function SummaryLine({ data }: { data: FarmResponse }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const parts = data.summary.area_by_crop.map((a) => t('planning.summary.area', {
    crop: cropName(a.names, a.species, lang), ha: formatHa(a.area_ha, lang),
  }));
  parts.push(t('planning.summary.undecided', { count: data.summary.undecided }));
  if (data.summary.rotation_warnings > 0) parts.push(t('planning.summary.rotation', { count: data.summary.rotation_warnings }));
  return <p className="text-nkz-sm text-nkz-text-muted">{`${data.campaign}: ${parts.join(' · ')}`}</p>;
}

export default function PlanningApp() {
  const { t } = useTranslation('bioorchestrator');
  const { hasRole } = useAuth();
  const [params] = useSearchParams();
  const linkedCampaign = Number(params.get('campaign')) || undefined;
  const [campaign, setCampaign] = useState<number | undefined>(linkedCampaign);
  const [farmId, setFarmId] = useState('all');
  const [openParcel, setOpenParcel] = useState<string | null>(params.get('parcel'));
  const [admin, setAdmin] = useState(false);
  const farm = useLoad((s) => fetchFarm(campaign, undefined, s), [campaign]);
  const data = farm.data;
  const row = data && openParcel ? data.parcels.find((p) => p.parcel_id === openParcel || p.parcel_id.endsWith(`:${openParcel}`)) : undefined;
  const linkedFarms = (data?.farms ?? []).filter((f) => f.farm_id);

  return (
    <Card padding="lg">
      <Stack gap="section">
        <Inline gap="inline" align="center">
          <Sprout className="w-7 h-7 text-nkz-accent-base" aria-hidden />
          <div>
            <h1 className="text-nkz-2xl font-bold text-nkz-text-primary">{t('planning.title')}</h1>
            <p className="text-nkz-base text-nkz-text-muted mt-1">{t('planning.subtitle')}</p>
          </div>
        </Inline>

        {admin ? <AdminTools /> : (
          <>
            {farm.loading && <div className="flex justify-center py-8" aria-busy="true"><Spinner size="lg" /></div>}
            {farm.error != null && (
              <Inline gap="inline" align="center">
                <p className="text-nkz-sm text-nkz-danger">{t('planning.errorLoad')}</p>
                <Button size="sm" variant="secondary" onClick={farm.retry}>{t('planning.retry')}</Button>
              </Inline>
            )}
            {data && row && (
              <ParcelDetail row={row} campaign={data.campaign} onClose={() => setOpenParcel(null)}
                onSaved={() => { setOpenParcel(null); farm.retry(); }} />
            )}
            {data && !row && (
              <Stack gap="stack">
                <Inline gap="inline" wrap align="end">
                  <div className="w-40">
                    <Select value={String(data.campaign)} onValueChange={(v) => setCampaign(Number(v))}
                      options={[data.campaign - 1, data.campaign, data.campaign + 1].map((c) => ({ value: String(c), label: `${t('planning.campaign')} ${c}` }))} />
                  </div>
                  {linkedFarms.length > 0 && (
                    <div className="w-56">
                      <Select value={farmId} onValueChange={setFarmId}
                        options={[{ value: 'all', label: t('planning.farmAll') }, ...linkedFarms.map((f) => ({ value: f.farm_id as string, label: f.name ?? (f.farm_id as string) }))]} />
                    </div>
                  )}
                </Inline>
                <SummaryLine data={data} />
                <PlanMatrix data={data} farmId={farmId} onOpen={setOpenParcel} />
              </Stack>
            )}
          </>
        )}

        <Inline gap="inline" wrap align="center">
          <SourcesFooter />
          {hasRole('PlatformAdmin') && (
            <Button size="sm" variant="ghost" onClick={() => setAdmin((a) => !a)}>{t(admin ? 'planning.admin.close' : 'planning.admin.open')}</Button>
          )}
        </Inline>
        <DisclaimerFooter />
      </Stack>
    </Card>
  );
}
