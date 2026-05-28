import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { Stack, Card, Badge, Button, DetailGrid, DetailItem, Skeleton, EmptyState } from '@nekazari/ui-kit';
import { Leaf, AlertTriangle, PlusCircle, Link, Droplets } from 'lucide-react';
import { useCropApi, CropDetail as CropDetailType } from '../services/api';

interface Props {
  cropId: string;
  onContribute: () => void;
  onViewInParcel: () => void;
}

export default function CropDetail({ cropId, onContribute, onViewInParcel }: Props) {
  const { t } = useTranslation('bioorchestrator');
  const { getCropDetail } = useCropApi();
  const [detail, setDetail] = useState<CropDetailType | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('kc');

  useEffect(() => {
    let cancelled = false;
    getCropDetail(cropId).then(d => { if (!cancelled) setDetail(d); }).finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [cropId]);

  if (loading) return (
    <Stack gap="tight">
      {Array.from({ length: 6 }).map((_, i) => (
        <Skeleton key={i} width="100%" height="48px" />
      ))}
    </Stack>
  );
  if (!detail) return null;

  const { data_available: da } = detail;

  const tabItems = [
    { id: 'kc', label: 'Kc', available: da.kc },
    { id: 'thermal', label: t('catalog.detail.thermal'), available: da.thermal },
    { id: 'soil', label: t('catalog.detail.soil'), available: da.soil_suitability },
    { id: 'npk', label: t('catalog.detail.npk'), available: da.npk },
  ];

  return (
    <Stack gap="stack">
      <div className="flex items-center gap-2">
        <Leaf size={20} className="text-nkz-success" />
        <h2 className="text-nkz-lg font-bold">{detail.name} <small className="text-nkz-text-muted font-normal">{detail.scientificName}</small></h2>
        <Badge>{detail.dataProvider}</Badge>
      </div>

      <div className="flex flex-wrap gap-2">
        {tabItems.map(tab => (
          <Badge key={tab.id} intent={tab.available ? 'positive' : 'default'}>
            {tab.label} {tab.available ? '✓' : '✗'}
          </Badge>
        ))}
      </div>

      <div className="flex border-b border-nkz-border gap-1">
        {tabItems.map(tab => (
          <button
            key={tab.id}
            disabled={!tab.available}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab.id ? 'text-nkz-accent-base border-nkz-accent-base' : 'border-transparent text-nkz-text-muted hover:text-nkz-text-primary'} disabled:opacity-50 disabled:cursor-not-allowed`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === 'kc' && (
          <Card padding="md">
            <DetailGrid>
              {detail.phenology.map((p, i) => (
                <React.Fragment key={i}>
                  <DetailItem label={t('catalog.detail.stage')} value={p.stage || p.name || '—'} />
                  <DetailItem label="Kc" value={p.kc != null ? String(p.kc) : '—'} />
                  <DetailItem label={t('catalog.detail.d1')} value={p.d1 != null ? `${p.d1}°C` : '—'} />
                  <DetailItem label={t('catalog.detail.d2')} value={p.d2 != null ? `${p.d2}°C` : '—'} />
                  <DetailItem label={t('catalog.detail.mds')} value={p.mdsRef != null ? `${p.mdsRef}μm` : '—'} />
                  <DetailItem label={t('catalog.detail.source')} value={p.sourceShort || '—'} />
                </React.Fragment>
              ))}
            </DetailGrid>
            {!da.d1_d2 && (
              <span className="text-nkz-warning text-sm">
                <AlertTriangle size={14} className="inline mr-1" />
                {t('catalog.detail.pending')}
              </span>
            )}
          </Card>
      )}

      {activeTab === 'thermal' && detail.heat_tolerance.map((ht, i) => (
        <Card key={i} padding="md">
          <DetailGrid>
            <DetailItem label={t('catalog.detail.heatDamage')} value={ht.heatDamageThresholdC != null ? `${ht.heatDamageThresholdC}°C` : '—'} />
            <DetailItem label={t('catalog.detail.frostDamage')} value={ht.frostDamageThresholdC != null ? `${ht.frostDamageThresholdC}°C` : '—'} />
            <DetailItem label={t('catalog.detail.heatAccumHours')} value={ht.heatAccumHours || '—'} />
            <DetailItem label={t('catalog.detail.source')} value={ht.sourceType || ht.sourceShort || '—'} />
          </DetailGrid>
        </Card>
      ))}

      {activeTab === 'soil' && detail.soil_suitability.map((ss, i) => (
        <Card key={i} padding="md">
          <DetailGrid>
            <DetailItem label="pH" value={ss.phMin != null && ss.phMax != null ? `${ss.phMin} – ${ss.phMax}` : '—'} />
            <DetailItem label={t('catalog.detail.textures')} value={Array.isArray(ss.textures) ? ss.textures.join(', ') : (ss.textures || '—')} />
            <DetailItem label={t('catalog.detail.drainage')} value={ss.drainage || '—'} />
            <DetailItem label={t('catalog.detail.depthMin')} value={ss.depthMinCm != null ? `${ss.depthMinCm} cm` : '—'} />
            <DetailItem label={t('catalog.detail.salinityMax')} value={ss.salinityMaxDsM != null ? `${ss.salinityMaxDsM} dS/m` : '—'} />
          </DetailGrid>
        </Card>
      ))}

      {activeTab === 'npk' && (
        !da.npk ? (
          <EmptyState icon={<Droplets />} title={t('catalog.detail.missing')} />
        ) : (
          <Card padding="md">
            <DetailGrid>
              {detail.nutrient_profile.map((np, i) => (
                <React.Fragment key={i}>
                  <DetailItem label={np.element || 'N'} value={`${np.uptakeKgHaDay} kg/ha/day`} />
                  <DetailItem label={t('catalog.detail.stage')} value={np.stage || '—'} />
                  <DetailItem label={t('catalog.detail.source')} value={np.sourceShort || '—'} />
                </React.Fragment>
              ))}
            </DetailGrid>
          </Card>
        )
      )}

      <div className="flex gap-2">
        <Button variant="ghost" onClick={onContribute}>
          <span className="flex items-center gap-1"><PlusCircle size={14} />{t('catalog.detail.contributeData')}</span>
        </Button>
        <Button variant="ghost" onClick={onViewInParcel}>
          <span className="flex items-center gap-1"><Link size={14} />{t('catalog.detail.viewInParcel')}</span>
        </Button>
      </div>
    </Stack>
  );
}
