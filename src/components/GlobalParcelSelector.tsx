import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Surface, Badge, Spinner } from '@nekazari/ui-kit';
import { MapPin, AlertTriangle } from 'lucide-react';
import { useParcelContext } from '../context/ParcelContext';

export default function GlobalParcelSelector() {
  const { t } = useTranslation('bioorchestrator');
  const { parcels, selectedParcel, setSelectedParcel, loading, error } = useParcelContext();
  const selected = parcels.find(p => p.id === selectedParcel);

  return (
    <Surface variant="raised" padding="stack" radius="md">
      <div className="flex items-center gap-3 flex-wrap">
        <MapPin className="w-5 h-5 text-nkz-accent-base shrink-0" />
        <label htmlFor="global-parcel-select" className="text-nkz-sm font-medium text-nkz-text-secondary shrink-0">
          {t('globalParcel.workingOn')}:
        </label>
        {loading ? (
          <span className="flex items-center gap-2 text-nkz-sm text-nkz-text-muted">
            <Spinner size="sm" />
            {t('common.loading')}
          </span>
        ) : error ? (
          <span className="flex items-center gap-2 text-nkz-sm text-nkz-danger">
            <AlertTriangle className="w-4 h-4" />
            {t('globalParcel.error', { defaultValue: 'Failed to load parcels' })}
          </span>
        ) : parcels.length === 0 ? (
          <span className="text-nkz-sm text-nkz-text-muted">
            {t('globalParcel.noParcels', { defaultValue: 'No parcels available' })}
          </span>
        ) : (
          <>
            <select
              id="global-parcel-select"
              className="h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-3 text-nkz-sm text-nkz-text-primary min-w-[240px]"
              value={selectedParcel}
              onChange={e => setSelectedParcel(e.target.value)}
            >
              <option value="">{t('globalParcel.selectParcel')}</option>
              {parcels.map(p => (
                <option key={p.id} value={p.id}>{p.name}</option>
              ))}
            </select>
            {selected && (
              <div className="flex items-center gap-2 text-nkz-sm text-nkz-text-muted">
                <Badge intent="info">{selected.name}</Badge>
              </div>
            )}
          </>
        )}
      </div>
    </Surface>
  );
}
