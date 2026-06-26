import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Surface } from '@nekazari/ui-kit';
import { AlertTriangle, FlaskConical } from 'lucide-react';
import { usePlanningScenario } from '../context/PlanningScenarioContext';

export default function ExplorationModeBanner() {
  const { t } = useTranslation('bioorchestrator');
  const { enabled, crop } = usePlanningScenario();

  if (!enabled || !crop?.eppo) return null;

  const cropLabel = crop.scientificName
    ? `${crop.eppo} — ${crop.scientificName}`
    : crop.eppo;

  return (
    <Surface
      variant="raised"
      padding="stack"
      radius="md"
      role="alert"
      aria-live="assertive"
      className="border-2 border-nkz-warning bg-nkz-warning-soft"
    >
      <div className="flex items-start gap-3">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-nkz-md bg-nkz-warning text-nkz-text-on-accent">
          <AlertTriangle className="h-5 w-5" aria-hidden />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <FlaskConical className="h-4 w-4 text-nkz-warning shrink-0" aria-hidden />
            <p className="text-nkz-base font-bold text-nkz-text-primary">
              {t('scenarioMode.bannerTitle')}
            </p>
            <Badge intent="warning">{cropLabel}</Badge>
          </div>
          <p className="mt-1 text-nkz-sm text-nkz-text-primary">
            {t('scenarioMode.bannerBody')}
          </p>
          <p className="mt-1 text-nkz-xs font-medium text-nkz-warning">
            {t('scenarioMode.bannerCampaignHint')}
          </p>
        </div>
      </div>
    </Surface>
  );
}
