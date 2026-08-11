/**
 * RecommendationsPanel — thin slot wrapper for the unified viewer.
 *
 * Delegates all logic to CropManagementView (single source of truth) and only
 * adds the SlotShell container that the viewer's context-panel slot requires.
 */
import React from 'react';
import { SlotShell } from '@nekazari/viewer-kit';
import { useTranslation } from '@nekazari/sdk';
import { RefreshCw } from 'lucide-react';
import { resolveParcelContext, type ParcelEntityData } from '../utils/entityData';
import CropManagementView from './CropManagementView';

const bioAccent = { base: '#14B8A6', soft: '#CCFBF1', strong: '#0D9488' };

interface Props { entityData?: ParcelEntityData; }

const RecommendationsPanel: React.FC<Props> = ({ entityData }) => {
  const { t } = useTranslation('bioorchestrator');
  const { parcelId, parcelName, lat, lon } = resolveParcelContext(entityData);

  return (
    <SlotShell moduleId="bioorchestrator" title={t('panel.title')} icon={<RefreshCw className="w-4 h-4" />} accent={bioAccent}>
      {parcelId ? (
        <CropManagementView parcelId={parcelId} parcelName={parcelName} lat={lat} lon={lon} />
      ) : (
        <p className="text-nkz-sm text-nkz-text-muted">{t('panel.noParcel', { defaultValue: 'Select a parcel to see recommendations.' })}</p>
      )}
    </SlotShell>
  );
};

export default RecommendationsPanel;
