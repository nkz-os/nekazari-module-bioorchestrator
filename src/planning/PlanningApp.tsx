// src/planning/PlanningApp.tsx
import React, { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useAuth, useTranslation } from '@nekazari/sdk';
import { Button, Card, Inline, Select, Spinner, Stack } from '@nekazari/ui-kit';
import { Sprout } from 'lucide-react';
import DisclaimerFooter from '../components/DisclaimerFooter';
import AdminTools from './AdminTools';
import { fetchFarm } from './api';
import { loadErrorKey } from './errors';
import { cropName, formatHa } from './format';
import LazyBoundary from './LazyBoundary';
import { parseCampaign } from './matrixModel';
import ParcelDetail from './ParcelDetail';
import PlanMatrix from './PlanMatrix';
import SimulationBridge from './SimulationBridge';
import SourcesFooter from './SourcesFooter';
import type { FarmResponse } from './types';
import { closeParams, openParcelParams, openSimulationParams, readView } from './urlState';
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
  // The opened parcel and the simulation view live in the URL (a new history entry each), so Back returns
  // to the matrix and a reload reopens what was open. The campaign is read once; the URL carries it along.
  const [params, setParams] = useSearchParams();
  const [campaign, setCampaign] = useState<number | undefined>(() => parseCampaign(params.get('campaign')));
  const [farmId, setFarmId] = useState('all');
  const { parcel: openParcel, simulationFor } = readView(params);
  const [admin, setAdmin] = useState(false);
  const farm = useLoad((s) => fetchFarm(campaign, undefined, s), [campaign]);
  const data = farm.data;
  const row = data && openParcel ? data.parcels.find((p) => p.parcel_id === openParcel || p.parcel_id.endsWith(`:${openParcel}`)) : undefined;
  const linkedFarms = (data?.farms ?? []).filter((f) => f.farm_id);
  const openDetail = (parcelId: string) => setParams((p) => openParcelParams(p, parcelId, data?.campaign ?? new Date().getFullYear()));
  const openSimulation = (parcelId: string) => setParams((p) => openSimulationParams(p, parcelId));
  const closeView = () => setParams((p) => closeParams(p));
  // A new campaign drops a link that did not resolve (and its "parcel not found" notice) instead of keeping it.
  const changeCampaign = (c: number) => {
    setCampaign(c);
    if (['parcel', 'campaign', 'view'].some((k) => params.has(k))) setParams((p) => closeParams(p), { replace: true });
  };

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

        {admin ? <LazyBoundary><AdminTools /></LazyBoundary> : simulationFor ? (
          <SimulationBridge parcelId={simulationFor} onClose={closeView} />
        ) : (
          <>
            {farm.loading && <div className="flex justify-center py-8" aria-busy="true"><Spinner size="lg" /></div>}
            {farm.error != null && (
              <Inline gap="inline" align="center">
                <p className="text-nkz-sm text-nkz-danger">{t(loadErrorKey(farm.error))}</p>
                <Button size="sm" variant="secondary" onClick={farm.retry}>{t('planning.retry')}</Button>
              </Inline>
            )}
            {data && row && (
              <ParcelDetail key={`${row.parcel_id}:${data.campaign}`} row={row} campaign={data.campaign} onClose={closeView}
                onSaved={() => { closeView(); farm.retry(); }} onSimulate={() => openSimulation(row.parcel_id)} onReload={farm.retry} />
            )}
            {data && !row && (
              <Stack gap="stack">
                <Inline gap="inline" wrap align="end">
                  <div className="w-40">
                    <Select value={String(data.campaign)} onValueChange={(v) => changeCampaign(Number(v))}
                      options={[data.campaign - 1, data.campaign, data.campaign + 1].map((c) => ({ value: String(c), label: `${t('planning.campaign')} ${c}` }))} />
                  </div>
                  {linkedFarms.length > 0 && (
                    <div className="w-64">
                      <Select value={farmId} onValueChange={setFarmId}
                        options={[{ value: 'all', label: t('planning.farmAll') }, ...linkedFarms.map((f) => ({ value: f.farm_id as string, label: f.name ?? (f.farm_id as string) }))]} />
                    </div>
                  )}
                </Inline>
                {openParcel && <p className="text-nkz-sm text-nkz-text-muted">{t('planning.plan.error.parcel_not_found')}</p>}
                <SummaryLine data={data} />
                <PlanMatrix data={data} farmId={farmId} onOpen={openDetail} onSimulate={openSimulation}
                  editable={data.campaign >= new Date().getFullYear()} />
              </Stack>
            )}
          </>
        )}

        <SourcesFooter />
        {hasRole('PlatformAdmin') && (
          <Inline gap="inline" wrap align="center">
            <Button size="sm" variant="ghost" onClick={() => setAdmin((a) => !a)}>{t(admin ? 'planning.admin.close' : 'planning.admin.open')}</Button>
          </Inline>
        )}
        <DisclaimerFooter />
      </Stack>
    </Card>
  );
}
