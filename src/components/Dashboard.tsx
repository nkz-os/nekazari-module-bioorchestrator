import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Stack } from '@nekazari/ui-kit';
import {
  Search, Heart, Activity, RefreshCw, Droplets, Dna,
} from 'lucide-react';
import { useParcelContext } from '../context/ParcelContext';
import KnowledgeBaseGrid from './KnowledgeBaseGrid';

interface ToolCard {
  id: string;
  icon: React.ElementType;
  section: 'predict' | 'compare';
}

const PREDICT_TOOLS: ToolCard[] = [
  { id: 'varietyFinder', icon: Search, section: 'predict' },
  { id: 'parcelStatus', icon: Heart, section: 'predict' },
];

const COMPARE_TOOLS: ToolCard[] = [
  { id: 'comparator', icon: Activity, section: 'compare' },
  { id: 'rotationPlanner', icon: RefreshCw, section: 'compare' },
  { id: 'waterBudget', icon: Droplets, section: 'compare' },
  { id: 'regenerative', icon: Dna, section: 'compare' },
];

interface DashboardProps {
  onSelectTool: (toolId: string) => void;
}

export default function Dashboard({ onSelectTool }: DashboardProps) {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel } = useParcelContext();

  const needsParcel = (section: ToolCard['section']) => section === 'predict' || section === 'compare';

  const renderCard = (tool: ToolCard) => {
    const Icon = tool.icon;
    const disabled = needsParcel(tool.section) && !selectedParcel;

    return (
      <Card
        key={tool.id}
        padding="lg"
        role="button"
        tabIndex={disabled ? -1 : 0}
        aria-label={t(`app.cards.${tool.id}.title`)}
        aria-disabled={disabled}
        className={`transition-colors focus-visible:ring-2 focus-visible:ring-nkz-accent-base focus-visible:outline-none ${
          disabled
            ? 'opacity-50 cursor-default'
            : 'cursor-pointer hover:border-nkz-accent-base'
        }`}
        onClick={() => !disabled && onSelectTool(tool.id)}
        onKeyDown={(e) => {
          if (!disabled && (e.key === 'Enter' || e.key === ' ')) {
            e.preventDefault();
            onSelectTool(tool.id);
          }
        }}
      >
        <div className="flex items-start gap-4">
          <Icon className="w-6 h-6 text-nkz-accent-base shrink-0 mt-1" />
          <div>
            <h3 className="text-nkz-base font-semibold text-nkz-text-primary">
              {t(`app.cards.${tool.id}.title`)}
            </h3>
            <p className="text-nkz-sm text-nkz-text-muted mt-1">
              {t(`app.cards.${tool.id}.subtitle`)}
            </p>
          </div>
        </div>
      </Card>
    );
  };

  return (
    <Stack gap="section">
      {/* Predict & Evaluate */}
      <section>
        <h2 className="text-nkz-lg font-semibold text-nkz-text-primary mb-4">
          {t('app.sections.predictEvaluate')}
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {PREDICT_TOOLS.map(renderCard)}
        </div>
      </section>

      {/* Compare & Plan */}
      <section>
        <h2 className="text-nkz-lg font-semibold text-nkz-text-primary mb-4">
          {t('app.sections.comparePlan')}
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {COMPARE_TOOLS.map(renderCard)}
        </div>
      </section>

      {/* Knowledge Base */}
      <KnowledgeBaseGrid onSelect={onSelectTool} />
    </Stack>
  );
}
