import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Card, Spinner, Stack, Switch } from '@nekazari/ui-kit';
import { Sprout, Heart, BookOpen, Bell } from 'lucide-react';
import { useParcelContext } from '../context/ParcelContext';
import { usePlanningScenario } from '../context/PlanningScenarioContext';
import { useExpertMode } from '../features/whatToSow/expertModeContext';
import WhatToSowPage from '../features/whatToSow/WhatToSowPage';
import { ALL_TOOLS, ADVANCED_TOOLS, type ToolCardDef } from './Dashboard';
import {
  CAMPAIGN_TOOL_IDS,
  LIBRARY_TOOL_IDS,
  type Door,
} from '../utils/navigation';

export interface ParcelInfo {
  campaignCrop: string | null;
  alertCount: number;
  loading: boolean;
}

interface HomeProps {
  parcelInfo: ParcelInfo;
  door: Door;
  onDoorChange: (door: Door) => void;
  onSelectTool: (toolId: string) => void;
  /** Called after a variety is assigned to the parcel, so parcel info can be refreshed. */
  onAssigned?: () => void;
  /** The default door is not yet known for the selected parcel. */
  doorPending?: boolean;
}

const DOOR_DEFS: { id: Door; icon: React.ElementType }[] = [
  { id: 'whatToSow', icon: Sprout },
  { id: 'campaign', icon: Heart },
  { id: 'library', icon: BookOpen },
];

const pickTools = (defs: ToolCardDef[], ids: readonly string[]): ToolCardDef[] =>
  ids.map((id) => defs.find((d) => d.id === id)).filter((d): d is ToolCardDef => Boolean(d));

const CAMPAIGN_TOOLS = pickTools(ALL_TOOLS, CAMPAIGN_TOOL_IDS);
const LIBRARY_TOOLS = pickTools(ADVANCED_TOOLS, LIBRARY_TOOL_IDS);

export default function Home({ parcelInfo, door, onDoorChange, onSelectTool, onAssigned, doorPending = false }: HomeProps) {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel } = useParcelContext();
  const { enabled: scenarioEnabled } = usePlanningScenario();
  const { expert, setExpert } = useExpertMode();

  const renderToolCard = (tool: ToolCardDef, disabled: boolean) => {
    const Icon = tool.icon;
    return (
      <Card
        key={tool.id}
        padding={tool.compact ? 'md' : 'lg'}
        role="button"
        tabIndex={disabled ? -1 : 0}
        aria-label={t(`app.cards.${tool.id}.title`)}
        aria-disabled={disabled}
        className={`transition-all duration-200 focus-visible:ring-2 focus-visible:ring-nkz-accent-base ${
          disabled
            ? 'opacity-50'
            : 'cursor-pointer hover:border-nkz-accent-base hover:shadow-sm'
        }`}
        onClick={() => !disabled && onSelectTool(tool.id)}
        onKeyDown={(e) => {
          if (!disabled && (e.key === 'Enter' || e.key === ' ')) {
            e.preventDefault();
            onSelectTool(tool.id);
          }
        }}
      >
        <div className={tool.compact ? 'flex items-start gap-3' : 'flex items-start gap-4'}>
          <Icon className={`${tool.compact ? 'w-5 h-5 mt-0.5' : 'w-6 h-6 mt-1'} text-nkz-accent-base shrink-0`} />
          <div className="min-w-0">
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
      <div className="flex items-center justify-end">
        <Switch
          checked={expert}
          onChange={setExpert}
          label={t('app.expertMode')}
          labelPosition="left"
        />
      </div>

      <nav aria-label={t('app.doors.navLabel')} className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {DOOR_DEFS.map(({ id, icon: Icon }) => {
          const selected = door === id;
          return (
            <Card
              key={id}
              padding="lg"
              role="button"
              tabIndex={0}
              aria-pressed={selected}
              aria-label={t(`app.doors.${id}.title`)}
              className={`cursor-pointer transition-all duration-200 focus-visible:ring-2 focus-visible:ring-nkz-accent-base ${
                selected ? 'border-nkz-accent-base shadow-sm' : 'hover:border-nkz-accent-base'
              }`}
              onClick={() => onDoorChange(id)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  onDoorChange(id);
                }
              }}
            >
              <div className="flex items-start gap-4">
                <Icon className="w-6 h-6 mt-1 text-nkz-accent-base shrink-0" />
                <div className="min-w-0">
                  <h2 className="text-nkz-base font-semibold text-nkz-text-primary">
                    {t(`app.doors.${id}.title`)}
                  </h2>
                  <p className="text-nkz-sm text-nkz-text-muted mt-1">
                    {t(`app.doors.${id}.description`)}
                  </p>
                </div>
              </div>
            </Card>
          );
        })}
      </nav>

      {door === 'whatToSow' && doorPending && (
        <div className="flex justify-center py-8" aria-busy="true"><Spinner size="lg" /></div>
      )}
      {door === 'whatToSow' && !doorPending && (
        <WhatToSowPage parcelId={selectedParcel || null} onSelectTool={onSelectTool} onAssigned={onAssigned} />
      )}

      {door === 'campaign' && (
        <Stack gap="section">
          {!selectedParcel && (
            <p className="text-nkz-sm text-nkz-text-muted">{t('app.selectParcelPrompt')}</p>
          )}
          {selectedParcel && (
            <div className="flex flex-wrap items-center gap-2 text-nkz-sm">
              {parcelInfo.loading ? (
                <Spinner size="sm" />
              ) : parcelInfo.campaignCrop ? (
                <Badge intent="positive">{t('app.hubs.activeCampaign', { crop: parcelInfo.campaignCrop })}</Badge>
              ) : (
                <Badge intent="default">{t('app.hubs.noCampaign')}</Badge>
              )}
              {parcelInfo.alertCount > 0 && (
                <span className="inline-flex items-center gap-1.5 text-nkz-warning font-medium">
                  <Bell className="w-4 h-4" aria-hidden />
                  {t('app.hubs.alertsAwareness', { count: parcelInfo.alertCount })}
                </span>
              )}
            </div>
          )}
          {scenarioEnabled && (
            <p className="text-nkz-sm font-medium text-nkz-warning">{t('scenarioMode.campaignCardsBlocked')}</p>
          )}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {CAMPAIGN_TOOLS.map((tool) => renderToolCard(tool, !selectedParcel || scenarioEnabled))}
          </div>
        </Stack>
      )}

      {door === 'library' && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {LIBRARY_TOOLS.map((tool) => renderToolCard(tool, false))}
        </div>
      )}
    </Stack>
  );
}
