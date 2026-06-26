import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Surface, Spinner } from '@nekazari/ui-kit';
import { MapPin, AlertTriangle, FlaskConical } from 'lucide-react';
import { useParcelContext } from '../context/ParcelContext';
import { usePlanningScenario } from '../context/PlanningScenarioContext';
import { useBioApi } from '../services/api';

interface CropOption {
  eppo_code: string;
  scientific_name: string;
}

export default function GlobalParcelSelector() {
  const { t } = useTranslation('bioorchestrator');
  const { parcels, selectedParcel, setSelectedParcel, loading, error } = useParcelContext();
  const { enabled: scenarioEnabled, crop: scenarioCrop, setEnabled, setCrop } = usePlanningScenario();
  const api = useBioApi();
  const selected = parcels.find(p => p.id === selectedParcel);

  const [crops, setCrops] = useState<CropOption[]>([]);
  const [cropsLoading, setCropsLoading] = useState(false);

  useEffect(() => {
    if (!selectedParcel) return;
    setCropsLoading(true);
    api.getAgricultureCrops()
      .then((d: { crops?: CropOption[] }) => setCrops(d?.crops || []))
      .catch(() => setCrops([]))
      .finally(() => setCropsLoading(false));
    // eslint-disable-next-line react-hooks/exhaustive-deps -- reload crops when parcel changes
  }, [selectedParcel]);

  return (
    <Surface variant="raised" padding="stack" radius="md">
      <div className="flex flex-col gap-3">
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
                <Badge intent="info">{selected.name}</Badge>
              )}
            </>
          )}
        </div>

        {selectedParcel && (
          <div
            className={`flex flex-wrap items-center gap-3 rounded-nkz-md border px-3 py-2 transition-colors duration-200 ${
              scenarioEnabled
                ? 'border-nkz-warning bg-nkz-warning-soft'
                : 'border-nkz-border bg-nkz-surface'
            }`}
          >
            <FlaskConical
              className={`w-4 h-4 shrink-0 ${scenarioEnabled ? 'text-nkz-warning' : 'text-nkz-text-muted'}`}
              aria-hidden
            />
            <label className="inline-flex items-center gap-2 cursor-pointer text-nkz-sm font-medium text-nkz-text-primary">
              <input
                type="checkbox"
                checked={scenarioEnabled}
                onChange={e => setEnabled(e.target.checked)}
                className="h-4 w-4 rounded border-nkz-border accent-nkz-warning"
              />
              {t('scenarioMode.enabled')}
            </label>
            <select
              id="scenario-crop-select"
              disabled={!scenarioEnabled || cropsLoading}
              className="h-8 min-w-[200px] rounded-nkz-md border border-nkz-border bg-nkz-surface px-2 text-nkz-sm text-nkz-text-primary disabled:opacity-50"
              value={scenarioCrop?.eppo ?? ''}
              onChange={e => {
                const opt = crops.find(c => c.eppo_code === e.target.value);
                if (opt) {
                  setCrop({ eppo: opt.eppo_code, scientificName: opt.scientific_name });
                } else {
                  setCrop(null);
                }
              }}
            >
              <option value="">{t('scenarioMode.cropPlaceholder')}</option>
              {crops.map(c => (
                <option key={c.eppo_code} value={c.eppo_code}>
                  {c.eppo_code} — {c.scientific_name?.slice(0, 28) || ''}
                </option>
              ))}
            </select>
            {scenarioEnabled && !scenarioCrop?.eppo && (
              <span className="text-nkz-xs text-nkz-warning font-medium">
                {t('scenarioMode.pickCrop')}
              </span>
            )}
          </div>
        )}
      </div>
    </Surface>
  );
}
