import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { Stack, Card, Input, Select, Badge, Skeleton, EmptyState } from '@nekazari/ui-kit';
import { Search, ChevronRight, ChevronDown, Leaf } from 'lucide-react';
import { useCropApi, CropItem } from '../services/api';

interface CropCatalogProps {
  onSelectCrop: (crop: CropItem) => void;
}

export default function CropCatalog({ onSelectCrop }: CropCatalogProps) {
  const { t } = useTranslation('bioorchestrator');
  const { getCatalog } = useCropApi();
  const [crops, setCrops] = useState<CropItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [source, setSource] = useState<string>('');
  const [search, setSearch] = useState('');
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const result = await getCatalog({ source: source || undefined, q: search || undefined });
        if (!cancelled) setCrops(result.crops);
      } catch (e: any) {
        if (!cancelled) setError(e.message || 'Failed to load catalog');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [source, search]);

  const toggleExpand = (uri: string) => {
    setExpanded(prev => {
      const next = new Set(prev);
      next.has(uri) ? next.delete(uri) : next.add(uri);
      return next;
    });
  };

  if (loading) return <Skeleton count={8} />;
  if (error) return <EmptyState icon={<Leaf />} title={error} variant="error" />;
  if (!crops.length) return <EmptyState icon={<Leaf />} title={t('catalog.tree.noData')} />;

  return (
    <Stack gap="md">
      <Stack direction="row" gap="sm">
        <Input
          placeholder={t('catalog.search')}
          value={search}
          onChange={e => setSearch(e.target.value)}
          icon={<Search size={16} />}
          className="flex-1"
        />
        <Select
          value={source}
          onChange={e => setSource(e.target.value)}
          options={[
            { value: '', label: t('catalog.sources.ecocrop') + ' + ' + t('catalog.sources.cpvo') },
            { value: 'ecocrop', label: t('catalog.sources.ecocrop') },
            { value: 'cpvo', label: t('catalog.sources.cpvo') },
          ]}
        />
      </Stack>

      <Card>
        {crops.map(crop => (
          <div key={crop.uri}>
            <div
              className="flex items-center gap-2 p-2 hover:bg-nkz-surface-hover cursor-pointer"
              onClick={() => onSelectCrop(crop)}
            >
              {crop.variety_count > 0 && (
                <span onClick={(e) => { e.stopPropagation(); toggleExpand(crop.uri); }}>
                  {expanded.has(crop.uri) ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                </span>
              )}
              <Leaf size={16} className="text-nkz-success" />
              <span className="flex-1">{crop.name} <small className="text-nkz-text-muted">({crop.scientificName})</small></span>
              {crop.variety_count > 0 && (
                <Badge variant="info">{t('catalog.tree.varieties', { count: crop.variety_count })}</Badge>
              )}
              <Badge variant={crop.has_kc ? 'success' : 'muted'} size="sm">Kc {crop.has_kc ? '✓' : '✗'}</Badge>
              <Badge variant={crop.has_thermal ? 'success' : 'muted'} size="sm">{t('catalog.detail.thermal')} {crop.has_thermal ? '✓' : '✗'}</Badge>
            </div>
            {expanded.has(crop.uri) && crop.variety_count > 0 && (
              <div className="ml-6 text-nkz-text-muted text-sm">
              </div>
            )}
          </div>
        ))}
      </Card>
    </Stack>
  );
}
