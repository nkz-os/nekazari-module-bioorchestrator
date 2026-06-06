import React, { useEffect, useState } from 'react';
import { Card, Badge, Skeleton } from '@nekazari/ui-kit';
import { useTranslation } from '@nekazari/sdk';
import { MapPin, Thermometer, Droplets, Globe } from 'lucide-react';
import { useBioApi } from '../services/api';

interface TrialSite {
  name: string;
  municipality: string;
  climate_class: string;
  soil_type: string;
  soil_texture: string;
  soil_ph: number;
  annual_rainfall_mm: number;
  elevation_m: number;
  frost_days: number;
  latitude: number;
  longitude: number;
  variety_trial_count: number;
  mgmt_trial_count: number;
}

const CLIMATE_COLORS: Record<string, string> = {
  'Csa': '#E74C3C',  // red — hot Med
  'CSa': '#E74C3C',
  'BSk': '#F39C12',  // orange — cold semi-arid
  'BSh': '#E67E22',  // dark orange — hot semi-arid
  'Cfb': '#2ECC71',  // green — oceanic
  'Dfa': '#3498DB',  // blue — hot continental
  'Dfb': '#2980B9',  // dark blue — warm continental
};

const CLIMATE_LABELS: Record<string, string> = {
  'Csa': 'Mediterranean (Csa)',
  'BSk': 'Cold semi-arid (BSk)',
  'BSh': 'Hot semi-arid (BSh)',
  'Cfb': 'Oceanic (Cfb)',
  'Dfa': 'Hot continental (Dfa)',
  'Dfb': 'Warm continental (Dfb)',
};

const ClimateExplorer: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [sites, setSites] = useState<TrialSite[]>([]);
  const [filtered, setFiltered] = useState<TrialSite[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedClimate, setSelectedClimate] = useState<string>('all');
  const [selectedSite, setSelectedSite] = useState<TrialSite | null>(null);
  const [climates, setClimates] = useState<string[]>([]);

  useEffect(() => {
    api.getTrialSites()
      .then((d: any) => {
        const all = (d?.sites || []) as TrialSite[];
        setSites(all);
        setFiltered(all);
        const uniqueClimates = [...new Set(all.map(s => s.climate_class).filter(Boolean))].sort();
        setClimates(uniqueClimates);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (selectedClimate === 'all') {
      setFiltered(sites);
    } else {
      setFiltered(sites.filter(s => s.climate_class === selectedClimate));
    }
  }, [selectedClimate, sites]);

  if (loading) {
    return (
      <Card padding="md">
        <Skeleton variant="rect" height="400px" />
      </Card>
    );
  }

  const bounds = filtered.length > 0
    ? {
        minLat: Math.min(...filtered.map(s => s.latitude || 0)),
        maxLat: Math.max(...filtered.map(s => s.latitude || 0)),
        minLon: Math.min(...filtered.map(s => s.longitude || 0)),
        maxLon: Math.max(...filtered.map(s => s.longitude || 0)),
      }
    : null;

  const viewBox = bounds
    ? `${bounds.minLon - 1} ${-(bounds.maxLat + 1)} ${bounds.maxLon - bounds.minLon + 2} ${bounds.maxLat - bounds.minLat + 2}`
    : '-10 -45 30 20';

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-2 mb-2">
        <Globe className="w-5 h-5 text-nkz-accent-base" />
        <h2 className="text-nkz-lg font-bold text-nkz-text-primary">{t('climateExplorer.title')}</h2>
      </div>

      {/* Climate filter */}
      <Card padding="md">
        <div className="flex flex-wrap gap-2 items-center">
          <span className="text-nkz-sm text-nkz-text-secondary mr-2">{t('climateExplorer.filter')}:</span>
          <Badge
            intent={selectedClimate === 'all' ? 'positive' : 'default'}
          >
            <button className="cursor-pointer bg-transparent border-0 p-0 text-inherit" onClick={() => setSelectedClimate('all')}>
              {t('climateExplorer.all')} ({sites.length})
            </button>
          </Badge>
          {climates.map(c => {
            const count = sites.filter(s => s.climate_class === c).length;
            return (
              <Badge
                key={c}
                intent={selectedClimate === c ? 'positive' : 'default'}
              >
                <button className="cursor-pointer bg-transparent border-0 p-0 text-inherit" onClick={() => setSelectedClimate(c)}>
                  {CLIMATE_LABELS[c] || c} ({count})
                </button>
              </Badge>
            );
          })}
        </div>
        <div className="mt-3 pt-3 border-t border-nkz-border text-nkz-xs text-nkz-text-muted">
          <strong>{t('climateExplorer.legend')}:</strong>{' '}
          {t('climateExplorer.legendGeneral')}{' '}
          {t('climateExplorer.legendCsa')}.{' '}
          {t('climateExplorer.legendBSk')}.{' '}
          {t('climateExplorer.legendCfb')}.
        </div>
      </Card>

      {/* Map */}
      <Card padding="md">
        <div className="relative bg-nkz-surface-sunken rounded-nkz-md overflow-hidden" style={{ height: '400px' }}>
          <svg viewBox={viewBox} className="w-full h-full" preserveAspectRatio="xMidYMid meet">
            {/* Grid lines */}
            {bounds && Array.from({ length: 5 }).map((_, i) => (
              <line
                key={`h${i}`}
                x1={bounds.minLon - 1}
                y1={-(bounds.minLat + (bounds.maxLat - bounds.minLat) * i / 4)}
                x2={bounds.maxLon + 1}
                y2={-(bounds.minLat + (bounds.maxLat - bounds.minLat) * i / 4)}
                stroke="#e5e7eb"
                strokeWidth="0.05"
              />
            ))}

            {/* Site markers */}
            {filtered.map(site => (
              <g
                key={site.name}
                className="cursor-pointer"
                onClick={() => setSelectedSite(site === selectedSite ? null : site)}
              >
                <circle
                  cx={site.longitude}
                  cy={-site.latitude}
                  r={site === selectedSite ? 0.35 : 0.2}
                  fill={CLIMATE_COLORS[site.climate_class] || '#999'}
                  stroke={site === selectedSite ? '#000' : 'none'}
                  strokeWidth="0.05"
                  opacity={site === selectedSite ? 1 : 0.7}
                />
                {site.variety_trial_count > 50 && (
                  <circle
                    cx={site.longitude}
                    cy={-site.latitude}
                    r={site === selectedSite ? 0.5 : 0.3}
                    fill="none"
                    stroke={CLIMATE_COLORS[site.climate_class] || '#999'}
                    strokeWidth="0.05"
                    opacity={0.3}
                  />
                )}
              </g>
            ))}
          </svg>

          {/* Legend */}
          <div className="absolute bottom-3 left-3 bg-nkz-surface/90 p-2 rounded-nkz-md text-nkz-xs">
            <div className="font-medium mb-1 text-nkz-text-secondary">{t('climateExplorer.legend')}</div>
            {climates.map(c => (
              <div key={c} className="flex items-center gap-1.5">
                <span className="w-3 h-3 rounded-full inline-block" style={{ backgroundColor: CLIMATE_COLORS[c] || '#999' }} />
                <span>{c}</span>
              </div>
            ))}
            <div className="mt-1 text-nkz-text-muted">● large = {t('climateExplorer.majorSite')} (&gt;50 trials)</div>
          </div>

          {/* Site count */}
          <div className="absolute top-3 right-3 bg-nkz-surface/90 px-2 py-1 rounded-nkz-md text-nkz-xs text-nkz-text-secondary">
            {filtered.length} {t('climateExplorer.sites')}
          </div>
        </div>
      </Card>

      {/* Selected site detail */}
      {selectedSite && (
        <Card padding="md">
          <h3 className="text-nkz-sm font-semibold text-nkz-text-primary mb-2 flex items-center gap-1.5">
            <MapPin className="w-4 h-4" style={{ color: CLIMATE_COLORS[selectedSite.climate_class] || '#999' }} />
            {selectedSite.name} ({selectedSite.municipality || '—'})
          </h3>
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 text-nkz-sm">
            <div>
              <div className="text-nkz-text-muted text-nkz-xs">{t('climateExplorer.climate')}</div>
              <div className="font-medium">{CLIMATE_LABELS[selectedSite.climate_class] || selectedSite.climate_class}</div>
            </div>
            <div>
              <div className="text-nkz-text-muted text-nkz-xs">{t('climateExplorer.soil')}</div>
              <div className="font-medium">{selectedSite.soil_type || '—'} ({selectedSite.soil_texture || '—'})</div>
            </div>
            <div>
              <div className="text-nkz-text-muted text-nkz-xs">{t('climateExplorer.rainfall')}</div>
              <div className="font-medium">{selectedSite.annual_rainfall_mm} mm</div>
            </div>
            <div>
              <div className="text-nkz-text-muted text-nkz-xs">{t('climateExplorer.elevation')}</div>
              <div className="font-medium">{selectedSite.elevation_m} m</div>
            </div>
            <div>
              <div className="text-nkz-text-muted text-nkz-xs">{t('climateExplorer.frostDays')}</div>
              <div className="font-medium">{selectedSite.frost_days} days</div>
            </div>
            <div>
              <div className="text-nkz-text-muted text-nkz-xs">{t('climateExplorer.trials')}</div>
              <div className="font-medium">{selectedSite.variety_trial_count}</div>
            </div>
          </div>
        </Card>
      )}
    </div>
  );
};

export default ClimateExplorer;
