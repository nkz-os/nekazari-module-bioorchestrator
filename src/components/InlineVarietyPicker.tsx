import React, { useEffect, useState } from 'react';
import { Stack, Card, Badge, Button, Skeleton } from '@nekazari/ui-kit';
import { useTranslation } from '@nekazari/sdk';
import { X, Search, AlertTriangle } from 'lucide-react';
import { useBioApi } from '../services/api';

interface VarietyInfo {
  name: string;
  scientificName?: string;
  cropUri: string;
  varietyUri: string;
  expectedYield: number;
  confidenceInterval: [number, number];
  trialCount: number;
}

interface CropOption {
  eppo_code: string;
  scientific_name: string;
  trial_count: number;
}

interface VarietyResult {
  variety: string;
  crop_uri?: string;
  variety_uri?: string;
  mean_yield_kg_ha: number;
  min_yield_kg_ha: number;
  max_yield_kg_ha: number;
  trial_count: number;
}

interface Props {
  parcelId: string;
  species?: string;
  onSelect: (variety: VarietyInfo) => void;
  onClose: () => void;
}

export default function InlineVarietyPicker({ parcelId, species, onSelect, onClose }: Props) {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();

  const [cropOptions, setCropOptions] = useState<CropOption[]>([]);
  const [selectedEppo, setSelectedEppo] = useState<string>('');
  const [results, setResults] = useState<VarietyResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [searching, setSearching] = useState(false);
  const [error, setError] = useState('');

  // Load crop list
  useEffect(() => {
    api.getAgricultureCrops?.()
      .then((d: any) => {
        if (Array.isArray(d?.crops)) {
          setCropOptions(d.crops);
        }
      })
      .catch((err: Error) => {
        console.warn('[InlineVarietyPicker] Failed to load crop list:', err.message);
      });
  }, []);

  // Auto-resolve EPPO code from species name
  useEffect(() => {
    if (!species || cropOptions.length === 0) return;
    const match = cropOptions.find((c) => {
      const s = species.toLowerCase();
      return (
        c.eppo_code.toLowerCase() === s ||
        c.scientific_name.toLowerCase() === s ||
        c.scientific_name.toLowerCase().includes(s)
      );
    });
    if (match) {
      setSelectedEppo(match.eppo_code);
      setLoading(false);
    } else {
      setLoading(false);
    }
  }, [species, cropOptions]);

  // No species provided → allow manual selection
  useEffect(() => {
    if (!species && cropOptions.length > 0) {
      setLoading(false);
    }
  }, [species, cropOptions]);

  const handleSearch = async () => {
    if (!selectedEppo) return;
    setSearching(true);
    setError('');
    try {
      const data = await api.extrapolateVarieties({
        crop: selectedEppo,
        parcel_id: parcelId,
        top_n: '15',
      });
      setResults(data.ranked_varieties || []);
    } catch (e: any) {
      setError(e.message || 'Unknown error');
    } finally {
      setSearching(false);
    }
  };

  // Auto-search when EPPO is resolved
  useEffect(() => {
    if (!selectedEppo || loading) return;
    handleSearch();
  }, [selectedEppo, loading]);

  const mapVariety = (v: VarietyResult): VarietyInfo => ({
    name: v.variety,
    cropUri: v.crop_uri || '',
    varietyUri: v.variety_uri || '',
    expectedYield: v.mean_yield_kg_ha,
    confidenceInterval: [v.min_yield_kg_ha, v.max_yield_kg_ha],
    trialCount: v.trial_count,
  });

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      onClick={onClose}
    >
      <div
        className="bg-nkz-surface rounded-nkz-lg shadow-lg w-full max-w-md max-h-[80vh] flex flex-col mx-4"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-nkz-border">
          <h3 className="text-nkz-sm font-semibold text-nkz-text-primary">
            {t('inlinePicker.title', { defaultValue: 'Select variety' })}
          </h3>
          <button
            onClick={onClose}
            className="p-1 rounded-nkz-sm hover:bg-nkz-surface-sunken text-nkz-text-muted"
            aria-label="Close"
          >
            <X size={18} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-4">
          {loading ? (
            <Stack gap="stack">
              <Skeleton variant="rect" height="40px" />
              <Skeleton variant="rect" height="60px" />
              <Skeleton variant="rect" height="60px" />
            </Stack>
          ) : !selectedEppo ? (
            /* Species selector (when no species or no match) */
            <Stack gap="stack">
              {species && (
                <p className="text-nkz-sm text-nkz-text-muted">
                  {t('inlinePicker.couldNotDetect', {
                    defaultValue: `Could not auto-detect species for "${species}". Please select one below.`,
                    species,
                  })}
                </p>
              )}
              <label className="text-nkz-xs font-medium text-nkz-text-secondary">
                {t('inlinePicker.selectSpecies', { defaultValue: 'Select crop species' })}
              </label>
              <select
                className="h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-3 text-nkz-sm w-full"
                value={selectedEppo}
                onChange={(e) => setSelectedEppo(e.target.value)}
              >
                <option value="">{t('inlinePicker.pickSpecies', { defaultValue: '— Select —' })}</option>
                {cropOptions.map((c) => (
                  <option key={c.eppo_code} value={c.eppo_code}>
                    {c.scientific_name || c.eppo_code} ({c.trial_count} trials)
                  </option>
                ))}
              </select>
              <Button
                variant="secondary"
                size="sm"
                disabled={!selectedEppo}
                onClick={handleSearch}
              >
                <Search size={14} className="mr-1" />
                {t('inlinePicker.searchAction', { defaultValue: 'Search varieties' })}
              </Button>
            </Stack>
          ) : searching ? (
            <Stack gap="stack">
              <Skeleton variant="rect" height="40px" />
              <Skeleton variant="rect" height="60px" />
              <Skeleton variant="rect" height="60px" />
            </Stack>
          ) : error ? (
            <div className="flex items-center gap-2 text-nkz-error text-sm">
              <AlertTriangle size={16} />
              <span className="flex-1">{error}</span>
              <button
                onClick={handleSearch}
                className="text-nkz-accent-base text-xs hover:underline"
              >
                {t('panel.retry', { defaultValue: 'Retry' })}
              </button>
            </div>
          ) : results.length === 0 ? (
            <p className="text-nkz-sm text-nkz-text-muted text-center py-4">
              {t('inlinePicker.noVarieties', { defaultValue: 'No varieties found for this species in similar environments.' })}
            </p>
          ) : (
            <Stack gap="tight">
              {/* Species change option */}
              <div className="flex items-center gap-2 mb-1">
                <span className="text-nkz-xs text-nkz-text-muted">
                  {t('inlinePicker.showingFor', {
                    defaultValue: 'Showing varieties for {{crop}}',
                    crop: cropOptions.find(c => c.eppo_code === selectedEppo)?.scientific_name || selectedEppo,
                  })}
                </span>
                <button
                  onClick={() => { setSelectedEppo(''); setResults([]); }}
                  className="text-nkz-accent-base text-xs hover:underline"
                >
                  {t('inlinePicker.change', { defaultValue: 'Change' })}
                </button>
              </div>

              {results.map((v, i) => (
                <Card key={v.variety_uri || v.variety || i} padding="md">
                  <div
                    className="cursor-pointer hover:bg-nkz-surface-sunken -m-3 p-3 rounded-nkz-md transition-colors"
                    onClick={() => onSelect(mapVariety(v))}
                  >
                    <div className="flex justify-between items-start">
                      <div className="flex-1 min-w-0">
                        <div className="font-medium text-nkz-sm text-nkz-text-primary truncate">
                          {v.variety}
                        </div>
                        <div className="text-nkz-xs text-nkz-text-muted mt-0.5">
                          {t('varietyFinder.meanYield', { defaultValue: 'Mean yield' })}:{' '}
                          <strong className="text-nkz-accent-base">
                            {v.mean_yield_kg_ha?.toLocaleString()} kg/ha
                          </strong>
                          {' ['}
                          {v.min_yield_kg_ha?.toLocaleString()} – {v.max_yield_kg_ha?.toLocaleString()}
                          {']'}
                        </div>
                      </div>
                      <Badge intent={v.trial_count >= 5 ? 'positive' : 'warning'}>
                        {v.trial_count}
                      </Badge>
                    </div>
                  </div>
                </Card>
              ))}
            </Stack>
          )}
        </div>
      </div>
    </div>
  );
}
