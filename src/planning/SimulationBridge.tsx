/**
 * Entry point to the campaign simulation (switchable engines, "what if" runs). It stays in planning:
 * crop-health shows the in-season forecast and links here for the detail, pre-filled from the URL.
 */
import React, { Suspense, lazy, useEffect } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Spinner, Stack } from '@nekazari/ui-kit';
import { ArrowLeft } from 'lucide-react';
import { ParcelProvider, useParcelContext } from '../context/ParcelContext';
import { PlanningScenarioProvider } from '../context/PlanningScenarioContext';
import LazyBoundary from './LazyBoundary';
import type { SimulationPrefill } from '../components/cropSimulationModel';

const CropSimulation = lazy(() => import('../components/CropSimulation'));

// ParcelContext selects by the entity id (the AgriParcel URN, as fetchParcels returns it), which is the
// same id the planning rows carry as parcel_id, so it is passed through unchanged.
// ParcelProvider also follows the viewer's selected parcel once its list loads; re-assert the parcel the
// user opened whenever the selection drifts, or the simulation would show another parcel under this one.
function SelectParcel({ parcelId }: { parcelId: string }) {
  const { selectedParcel, setSelectedParcel } = useParcelContext();
  useEffect(() => {
    if (selectedParcel !== parcelId) setSelectedParcel(parcelId);
  }, [parcelId, selectedParcel, setSelectedParcel]);
  return null;
}

export default function SimulationBridge({ parcelId, prefill, onClose }: {
  parcelId: string; prefill?: SimulationPrefill; onClose: () => void;
}) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <ParcelProvider>
      <PlanningScenarioProvider>
        <SelectParcel parcelId={parcelId} />
        <Stack gap="section">
          <div>
            <Button variant="ghost" onClick={onClose} leadingIcon={<ArrowLeft className="w-4 h-4" />}>{t('planning.detail.back')}</Button>
          </div>
          <p className="text-nkz-xs text-nkz-text-muted">{t('planning.simulation.forecastNote')}</p>
          <LazyBoundary>
            <Suspense fallback={<Spinner size="lg" />}>
              <CropSimulation link={{ parcelId, prefill: prefill ?? {} }} />
            </Suspense>
          </LazyBoundary>
        </Stack>
      </PlanningScenarioProvider>
    </ParcelProvider>
  );
}
