import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Stack, Badge } from '@nekazari/ui-kit';
import {
  Leaf, Globe, Sprout, Thermometer, Droplets, RefreshCw,
  FlaskRound, GitBranch, Activity, Database, Mountain,
} from 'lucide-react';
import { useBioApi } from '../services/api';

interface KBItem {
  id: string;
  icon: React.ElementType;
  countBadge?: string;
}

const KB_ITEMS: KBItem[] = [
  { id: 'catalog', icon: Leaf, countBadge: undefined },
  { id: 'climate', icon: Globe },
  { id: 'phenology', icon: Sprout },
  { id: 'thermal', icon: Thermometer },
  { id: 'npk', icon: Droplets },
  { id: 'soil', icon: Mountain },
  { id: 'rotation', icon: RefreshCw },
  { id: 'organic', icon: FlaskRound },
  { id: 'pipeline', icon: GitBranch },
  { id: 'sources', icon: Activity, countBadge: undefined },
  { id: 'dadis', icon: Database },
];

interface Props {
  onSelect: (toolId: string) => void;
}

export default function KnowledgeBaseGrid({ onSelect }: Props) {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();

  // Fetch real stats from backend
  const [catalogCount, setCatalogCount] = useState<number | null>(null);
  const [sourcesCount, setSourcesCount] = useState<number | null>(null);

  useEffect(() => {
    // Fetch species count from /api/graph/species
    api.getSpecies()
      .then((d: any) => {
        if (Array.isArray(d)) {
          setCatalogCount(d.length);
        } else if (d?.length) {
          setCatalogCount(d.length);
        }
      })
      .catch(() => {
        // Silently fall back — no badge shown
      });

    // Fetch sources count from /api/graph/agriculture/sources
    api.getSources()
      .then((d: any) => {
        if (d?.total !== undefined) {
          setSourcesCount(d.total);
        }
      })
      .catch(() => {});
  }, []);

  // Build items with dynamic badges
  const items = KB_ITEMS.map(item => {
    if (item.id === 'catalog' && catalogCount !== null) {
      return { ...item, countBadge: String(catalogCount) };
    }
    if (item.id === 'sources' && sourcesCount !== null) {
      return { ...item, countBadge: String(sourcesCount) };
    }
    return item;
  });

  return (
    <Stack gap="section">
      <h2 className="text-nkz-lg font-semibold text-nkz-text-primary">
        {t('app.sections.knowledgeBase')}
      </h2>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {items.map(item => {
          const Icon = item.icon;
          return (
            <Card
              key={item.id}
              padding="md"
              role="button"
              tabIndex={0}
              aria-label={t(`app.cards.${item.id}.title`)}
              className="cursor-pointer hover:border-nkz-accent-base transition-colors focus-visible:ring-2 focus-visible:ring-nkz-accent-base focus-visible:outline-none"
              onClick={() => onSelect(item.id)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  onSelect(item.id);
                }
              }}
            >
              <div className="flex items-start gap-3">
                <Icon className="w-5 h-5 text-nkz-accent-base shrink-0 mt-0.5" />
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-nkz-base font-semibold text-nkz-text-primary">
                      {t(`app.cards.${item.id}.title`)}
                    </span>
                    {item.countBadge && (
                      <Badge intent="info">{item.countBadge}</Badge>
                    )}
                  </div>
                  <p className="text-nkz-sm text-nkz-text-muted mt-1">
                    {t(`app.cards.${item.id}.subtitle`)}
                  </p>
                </div>
              </div>
            </Card>
          );
        })}
      </div>
    </Stack>
  );
}
