/**
 * CropManagement — standalone page tool for the bioorchestrator module.
 *
 * This is the ISOLATED DEVELOPMENT SURFACE for crop management. It uses the
 * module's own ParcelContext (GlobalParcelSelector) instead of the viewer's
 * slot system, so features can be developed, tested, and debugged without the
 * noise of other modules converging in the unified viewer.
 *
 * Once everything works here, the same logic (CropManagementView) is already
 * consumed by RecommendationsPanel in the viewer — no duplication of logic.
 */
import React from 'react';
import { Card, Stack } from '@nekazari/ui-kit';
import { useTranslation } from '@nekazari/sdk';
import { Sprout } from 'lucide-react';
import { useParcelContext } from '../context/ParcelContext';
import CropManagementView from './CropManagementView';

const CropManagement: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel, parcels, loading } = useParcelContext();
  const parcel = parcels.find((p) => p.id === selectedParcel);

  return (
    <Stack gap="section">
      <div className="flex items-center gap-3">
        <Sprout className="w-6 h-6 text-nkz-accent-base" />
        <div>
          <h2 className="text-nkz-xl font-bold text-nkz-text-primary">
            {t('cropManagement.title', { defaultValue: 'Crop Management' })}
          </h2>
          <p className="text-nkz-sm text-nkz-text-muted">
            {t('cropManagement.subtitle', { defaultValue: 'Assign and manage the crop for the selected parcel.' })}
          </p>
        </div>
      </div>

      {loading ? (
        <Card padding="lg">
          <p className="text-nkz-text-muted">{t('cropManagement.loadingParcels', { defaultValue: 'Loading parcels…' })}</p>
        </Card>
      ) : !selectedParcel ? (
        <Card padding="lg">
          <p className="text-nkz-text-muted">
            {t('cropManagement.selectParcelFirst', { defaultValue: 'Select a parcel above to manage its crop.' })}
          </p>
        </Card>
      ) : (
        <CropManagementView parcelId={selectedParcel} parcelName={parcel?.name} />
      )}
    </Stack>
  );
};

export default CropManagement;
