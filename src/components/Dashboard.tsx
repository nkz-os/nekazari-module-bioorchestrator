import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Card, Stack, Spinner } from '@nekazari/ui-kit';
import {
  Search, Heart, Activity, RefreshCw, Droplets, Dna, Microscope,
  Leaf, Globe, Sprout, Thermometer, FlaskRound, GitBranch, Database, Mountain,
  Bell, TrendingUp,
} from 'lucide-react';
import { useParcelContext } from '../context/ParcelContext';
import { usePlanningScenario } from '../context/PlanningScenarioContext';
import { useBioApi, getCropContext, fetchAlerts } from '../services/api';

export type HubId = 'planning' | 'campaign' | 'codex';

interface ToolCardDef {
  id: string;
  icon: React.ElementType;
  hub: HubId;
  compact?: boolean;
}

const HUB_ORDER: HubId[] = ['planning', 'campaign', 'codex'];

const ALL_TOOLS: ToolCardDef[] = [
  { id: 'varietyFinder', icon: Search, hub: 'planning' },
  { id: 'comparator', icon: Activity, hub: 'planning' },
  { id: 'rotationPlanner', icon: RefreshCw, hub: 'planning' },
  { id: 'regenerative', icon: Dna, hub: 'planning' },
  { id: 'parcelStatus', icon: Heart, hub: 'campaign' },
  { id: 'yieldProjection', icon: TrendingUp, hub: 'campaign' },
  { id: 'wofostSimulation', icon: Microscope, hub: 'campaign' },
  { id: 'waterBudget', icon: Droplets, hub: 'campaign' },
  { id: 'catalog', icon: Leaf, hub: 'codex', compact: true },
  { id: 'climate', icon: Globe, hub: 'codex', compact: true },
  { id: 'phenology', icon: Sprout, hub: 'codex', compact: true },
  { id: 'thermal', icon: Thermometer, hub: 'codex', compact: true },
  { id: 'npk', icon: Droplets, hub: 'codex', compact: true },
  { id: 'soil', icon: Mountain, hub: 'codex', compact: true },
  { id: 'rotation', icon: RefreshCw, hub: 'codex', compact: true },
  { id: 'organic', icon: FlaskRound, hub: 'codex', compact: true },
  { id: 'pipeline', icon: GitBranch, hub: 'codex', compact: true },
  { id: 'sources', icon: Activity, hub: 'codex', compact: true },
  { id: 'dadis', icon: Database, hub: 'codex', compact: true },
  { id: 'speciesExplorer', icon: Leaf, hub: 'codex', compact: true },
];

interface DashboardProps {
  onSelectTool: (toolId: string) => void;
}

function defaultHubForParcel(hasCampaign: boolean, hasParcel: boolean): HubId {
  if (!hasParcel) return 'codex';
  return hasCampaign ? 'campaign' : 'planning';
}

export default function Dashboard({ onSelectTool }: DashboardProps) {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel } = useParcelContext();
  const { enabled: scenarioEnabled } = usePlanningScenario();
  const api = useBioApi();

  const [activeHub, setActiveHub] = useState<HubId>('codex');
  const [hubAnimating, setHubAnimating] = useState(false);
  const [campaignCrop, setCampaignCrop] = useState<string | null>(null);
  const [alertCount, setAlertCount] = useState(0);
  const [catalogCount, setCatalogCount] = useState<number | null>(null);
  const [sourcesCount, setSourcesCount] = useState<number | null>(null);
  const [contextLoading, setContextLoading] = useState(false);

  useEffect(() => {
    api.getSpecies()
      .then((d: unknown) => {
        if (Array.isArray(d)) setCatalogCount(d.length);
      })
      .catch(() => {});
    api.getSources()
      .then((d: { total?: number }) => {
        if (d?.total !== undefined) setSourcesCount(d.total);
      })
      .catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps -- mount-once stats fetch
  }, []);

  useEffect(() => {
    if (!selectedParcel) {
      setCampaignCrop(null);
      setAlertCount(0);
      setActiveHub('codex');
      return;
    }

    let cancelled = false;
    setContextLoading(true);

    Promise.all([
      getCropContext(selectedParcel),
      fetchAlerts(selectedParcel),
    ])
      .then(([ctx, alerts]) => {
        if (cancelled) return;
        const hasCampaign = Boolean(ctx.crop?.eppo && ctx.crop.eppo !== 'unknown');
        const cropLabel = hasCampaign
          ? `${ctx.crop.name || ctx.crop.eppo} (${ctx.crop.eppo})`
          : null;
        setCampaignCrop(cropLabel);
        setAlertCount(alerts.length);
        setActiveHub(defaultHubForParcel(hasCampaign, true));
      })
      .catch(() => {
        if (!cancelled) {
          setCampaignCrop(null);
          setAlertCount(0);
          setActiveHub('planning');
        }
      })
      .finally(() => {
        if (!cancelled) setContextLoading(false);
      });

    return () => { cancelled = true; };
  }, [selectedParcel]);

  const switchHub = useCallback((hub: HubId) => {
    if (hub === activeHub) return;
    setHubAnimating(true);
    setActiveHub(hub);
    window.setTimeout(() => setHubAnimating(false), 200);
  }, [activeHub]);

  const toolsForHub = ALL_TOOLS.filter(tool => tool.hub === activeHub);

  const isCardDisabled = (tool: ToolCardDef): boolean => {
    if (tool.hub === 'codex') return false;
    if (!selectedParcel) return true;
    if (tool.hub === 'campaign' && scenarioEnabled) return true;
    return false;
  };

  const countBadge = (toolId: string): string | undefined => {
    if (toolId === 'catalog' && catalogCount !== null) return String(catalogCount);
    if (toolId === 'sources' && sourcesCount !== null) return String(sourcesCount);
    return undefined;
  };

  const gridCols = activeHub === 'codex'
    ? 'grid-cols-1 sm:grid-cols-2 lg:grid-cols-3'
    : 'grid-cols-1 md:grid-cols-2';

  const renderCard = (tool: ToolCardDef) => {
    const Icon = tool.icon;
    const disabled = isCardDisabled(tool);
    const badge = countBadge(tool.id);
    const padding = tool.compact ? 'md' : 'lg';

    return (
      <Card
        key={tool.id}
        padding={padding}
        role="button"
        tabIndex={disabled ? -1 : 0}
        aria-label={t(`app.cards.${tool.id}.title`)}
        aria-disabled={disabled}
        className={`transition-all duration-200 focus-visible:ring-2 focus-visible:ring-nkz-accent-base focus-visible:outline-none ${
          disabled
            ? 'opacity-50 cursor-default'
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
            <div className="flex items-center gap-2 flex-wrap">
              <h3 className="text-nkz-base font-semibold text-nkz-text-primary">
                {t(`app.cards.${tool.id}.title`)}
              </h3>
              {badge && <Badge intent="info">{badge}</Badge>}
            </div>
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
      {/* Hub switcher */}
      <div
        className="flex flex-col gap-3"
        role="tablist"
        aria-label={t('app.hubs.navLabel')}
      >
        <div className="flex flex-wrap gap-1 p-1 rounded-nkz-lg border border-nkz-border bg-nkz-surface-raised">
          {HUB_ORDER.map((hub) => {
            const selected = activeHub === hub;
            return (
              <button
                key={hub}
                type="button"
                role="tab"
                aria-selected={selected}
                className={`flex-1 min-w-[7rem] px-4 py-2.5 rounded-nkz-md text-nkz-sm font-semibold transition-all duration-200 ease-out ${
                  selected
                    ? 'bg-nkz-accent-base text-nkz-text-on-accent shadow-sm'
                    : 'text-nkz-text-secondary hover:bg-nkz-surface hover:text-nkz-text-primary'
                }`}
                onClick={() => switchHub(hub)}
              >
                {t(`app.hubs.${hub}`)}
              </button>
            );
          })}
        </div>

        <p className="text-nkz-sm text-nkz-text-muted transition-opacity duration-200">
          {t(`app.hubs.${activeHub}Description`)}
        </p>
      </div>

      {/* Campaign hub context strip */}
      {activeHub === 'campaign' && selectedParcel && (
        <div className="flex flex-wrap items-center gap-2 text-nkz-sm">
          {contextLoading ? (
            <Spinner size="sm" />
          ) : campaignCrop ? (
            <Badge intent="positive">{t('app.hubs.activeCampaign', { crop: campaignCrop })}</Badge>
          ) : (
            <Badge intent="default">{t('app.hubs.noCampaign')}</Badge>
          )}
          {alertCount > 0 && (
            <span className="inline-flex items-center gap-1.5 text-nkz-warning font-medium">
              <Bell className="w-4 h-4" aria-hidden />
              {t('app.hubs.alertsAwareness', { count: alertCount })}
            </span>
          )}
        </div>
      )}

      {!selectedParcel && activeHub !== 'codex' && (
        <p className="text-nkz-sm text-nkz-text-muted">{t('app.selectParcelPrompt')}</p>
      )}

      {activeHub === 'campaign' && scenarioEnabled && (
        <p className="text-nkz-sm font-medium text-nkz-warning">{t('scenarioMode.campaignCardsBlocked')}</p>
      )}

      {/* Tool grid with cross-fade */}
      <section
        key={activeHub}
        role="tabpanel"
        className={`transition-opacity duration-200 ease-out ${
          hubAnimating ? 'opacity-0' : 'opacity-100'
        }`}
      >
        <div className={`grid ${gridCols} gap-3 md:gap-4`}>
          {toolsForHub.map(renderCard)}
        </div>
      </section>
    </Stack>
  );
}
