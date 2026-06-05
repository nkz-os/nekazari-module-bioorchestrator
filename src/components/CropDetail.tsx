import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { Stack, Card, Badge, Button, DetailGrid, DetailItem, Skeleton, Tabs, EmptyState } from '@nekazari/ui-kit';
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
    getCropDetail(cropId).then(d => { if (!cancelled) setDetail(d); }).finally(() => setLoading(false));
    return () => { cancelled = true; };
  }, [cropId]);

  if (loading) return <Skeleton />;
  if (!detail) return null;

  const { data_available: da } = detail;

  const tabs = [
    { id: 'kc', label: 'Kc', available: da.kc },
    { id: 'thermal', label: t('catalog.detail.thermal'), available: da.thermal },
    { id: 'soil', label: t('catalog.detail.soil'), available: da.soil_suitability },
    { id: 'npk', label: t('catalog.detail.npk'), available: da.npk },
  ];

  return (
    <Stack gap="section">
      <Stack gap="tight" className="items-center">
        <Leaf size={20} className="text-nkz-success" />
        <h2>{detail.name} <small className="text-nkz-text-muted">{detail.scientificName}</small></h2>
        <Badge>{detail.dataProvider}</Badge>
      </Stack>

      <Stack gap="tight">
        {tabs.map(tab => (
          <Badge key={tab.id} intent={tab.available ? 'positive' : 'default'}>
            {tab.label} {tab.available ? '✓' : '✗'}
          </Badge>
        ))}
      </Stack>

      <Tabs defaultValue="kc" value={activeTab} onValueChange={setActiveTab}>
        <Tabs.List>
          {tabs.map(t => (
            <Tabs.Trigger key={t.id} value={t.id}>{t.label}</Tabs.Trigger>
          ))}
        </Tabs.List>

        <Tabs.Content value="kc">
          <Card>
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
        </Tabs.Content>

        <Tabs.Content value="thermal">
          {detail.heat_tolerance.map((ht, i) => (
            <Card key={i}>
              <DetailGrid>
                <DetailItem label={t('catalog.detail.heatDamage')} value={ht.heatDamageThresholdC != null ? `${ht.heatDamageThresholdC}°C` : '—'} />
                <DetailItem label={t('catalog.detail.frostDamage')} value={ht.frostDamageThresholdC != null ? `${ht.frostDamageThresholdC}°C` : '—'} />
                <DetailItem label={t('catalog.detail.heatAccumHours')} value={ht.heatAccumHours || '—'} />
                <DetailItem label={t('catalog.detail.source')} value={ht.sourceType || ht.sourceShort || '—'} />
              </DetailGrid>
            </Card>
          ))}
        </Tabs.Content>

        <Tabs.Content value="soil">
          {detail.soil_suitability.length > 0 ? (
            <Card>
              <DetailGrid>
                {detail.soil_suitability.map((ss, i) => (
                  <React.Fragment key={i}>
                    <DetailItem label="pH" value={ss.phMin != null ? `${ss.phMin} - ${ss.phMax}` : '—'} />
                    <DetailItem label={t('catalog.detail.source')} value={ss.sourceShort || '—'} />
                  </React.Fragment>
                ))}
              </DetailGrid>
            </Card>
          ) : (
            <EmptyState icon={<Droplets />} title={t('catalog.detail.missing')} />
          )}
        </Tabs.Content>

        <Tabs.Content value="npk">
          {!da.npk ? (
            <EmptyState icon={<Droplets />} title={t('catalog.detail.missing')} />
          ) : (
            <Card>
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
          )}
        </Tabs.Content>
      </Tabs>

      <Stack gap="tight">
        <Button variant="secondary" size="sm" onClick={onContribute} leadingIcon={<PlusCircle size={14} />}>
          {t('catalog.detail.contributeData')}
        </Button>
        <Button variant="secondary" size="sm" onClick={onViewInParcel} leadingIcon={<Link size={14} />}>
          {t('catalog.detail.viewInParcel')}
        </Button>
      </Stack>
    </Stack>
  );
}
