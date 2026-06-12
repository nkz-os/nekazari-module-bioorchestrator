import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Stack, Badge } from '@nekazari/ui-kit';
import {
  Leaf, Globe, Sprout, Thermometer, Droplets, RefreshCw,
  FlaskRound, GitBranch, Activity, Database,
} from 'lucide-react';

interface KBItem {
  id: string;
  icon: React.ElementType;
  countBadge?: string;
}

const KB_ITEMS: KBItem[] = [
  { id: 'catalog', icon: Leaf, countBadge: '48' },
  { id: 'climate', icon: Globe },
  { id: 'phenology', icon: Sprout },
  { id: 'thermal', icon: Thermometer },
  { id: 'npk', icon: Droplets },
  { id: 'soil', icon: Sprout },
  { id: 'rotation', icon: RefreshCw },
  { id: 'organic', icon: FlaskRound },
  { id: 'pipeline', icon: GitBranch },
  { id: 'sources', icon: Activity, countBadge: '29' },
  { id: 'dadis', icon: Database },
];

interface Props {
  onSelect: (toolId: string) => void;
}

export default function KnowledgeBaseGrid({ onSelect }: Props) {
  const { t } = useTranslation('bioorchestrator');

  return (
    <Stack gap="section">
      <h2 className="text-nkz-lg font-semibold text-nkz-text-primary">
        {t('app.sections.knowledgeBase')}
      </h2>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {KB_ITEMS.map(item => {
          const Icon = item.icon;
          return (
            <Card
              key={item.id}
              padding="md"
              className="cursor-pointer hover:border-nkz-accent-base transition-colors"
              onClick={() => onSelect(item.id)}
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
