/**
 * TEMPORARY (owner decision 2026-10-11): entry point to the agronomic-models campaign simulation
 * until it moves to crop-health (F5). Remove this file and its two links when that happens.
 */
import React, { Suspense, lazy, useEffect } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Spinner, Stack } from '@nekazari/ui-kit';
import { ArrowLeft } from 'lucide-react';
import { ParcelProvider, useParcelContext } from '../context/ParcelContext';
import { PlanningScenarioProvider } from '../context/PlanningScenarioContext';

const CropSimulation = lazy(() => import('../components/CropSimulation'));

// ParcelContext selects by the entity id (the AgriParcel URN, as fetchParcels returns it), which is the
// same id the planning rows carry as parcel_id, so it is passed through unchanged.
function SelectParcel({ parcelId }: { parcelId: string }) {
  const { setSelectedParcel } = useParcelContext();
  useEffect(() => { setSelectedParcel(parcelId); }, [parcelId, setSelectedParcel]);
  return null;
}

export default function SimulationBridge({ parcelId, onClose }: { parcelId: string; onClose: () => void }) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <ParcelProvider>
      <PlanningScenarioProvider>
        <SelectParcel parcelId={parcelId} />
        <Stack gap="section">
          <div>
            <Button variant="ghost" onClick={onClose} leadingIcon={<ArrowLeft className="w-4 h-4" />}>{t('planning.detail.back')}</Button>
          </div>
          <p className="text-nkz-xs text-nkz-text-muted">{t('planning.simulation.temporary')}</p>
          <Suspense fallback={<Spinner size="lg" />}>
            <CropSimulation />
          </Suspense>
        </Stack>
      </PlanningScenarioProvider>
    </ParcelProvider>
  );
}
