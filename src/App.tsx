import React, { useState, useEffect, useCallback, useRef, lazy, Suspense, Component, ErrorInfo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation, useAuth } from '@nekazari/sdk';
import { Card, Stack, Spinner, Button } from '@nekazari/ui-kit';
import { ArrowLeft, FlaskConical } from 'lucide-react';
import { ParcelProvider, useParcelContext } from './context/ParcelContext';
import { PlanningScenarioProvider } from './context/PlanningScenarioContext';
import GlobalParcelSelector from './components/GlobalParcelSelector';
import ExplorationModeBanner from './components/ExplorationModeBanner';
import Home, { type ParcelInfo } from './components/Home';
import { ExpertModeProvider } from './features/whatToSow/expertModeContext';
import { getCropContext, fetchAlerts } from './services/api';
import DisclaimerFooter from './components/DisclaimerFooter';
import { resolveDoor, pickDefaultDoor, hasAssignedCrop, type Door } from './utils/navigation';
import './i18n';

const CropManagement = lazy(() => import('./components/CropManagement'));
const CropPlanner = lazy(() => import('./components/CropPlanner'));
const VarietyFinder = lazy(() => import('./components/VarietyFinder'));
const ParcelHealth = lazy(() => import('./components/ParcelHealth'));
const CropComparator = lazy(() => import('./components/CropComparator'));
const RotationPlanner = lazy(() => import('./components/RotationPlanner'));
const WaterBudget = lazy(() => import('./components/WaterBudget'));
const RegenerativeSequence = lazy(() => import('./components/RegenerativeSequence'));
const CropCatalog = lazy(() => import('./components/CropCatalog'));
const ClimateExplorer = lazy(() => import('./components/ClimateExplorer'));
const PhenologyBrowser = lazy(() => import('./components/PhenologyBrowser'));
const ThermalTolerance = lazy(() => import('./components/ThermalTolerance'));
const NutrientProfile = lazy(() => import('./components/NutrientProfile'));
const SoilSuitability = lazy(() => import('./components/SoilSuitability'));
const RotationConstraints = lazy(() => import('./components/RotationConstraints'));
const OrganicInputs = lazy(() => import('./components/OrganicInputs'));
const PipelineRunner = lazy(() => import('./components/PipelineRunner'));
const SourcesDashboard = lazy(() => import('./components/SourcesDashboard'));
const YieldProjection = lazy(() => import('./components/YieldProjection'));
const WofostSimulation = lazy(() => import('./components/WofostSimulation'));
const SpeciesExplorer = lazy(() => import('./components/SpeciesExplorer'));
const SimulateAlternative = lazy(() => import('./components/SimulateAlternative'));
const BreedDiscovery = lazy(() => import('./components/DADIS/BreedDiscovery').then(m => ({ default: m.BreedDiscovery })));


const TOOL_MAP: Record<string, React.LazyExoticComponent<React.ComponentType<any>>> = {
  cropManagement: CropManagement,
  cropPlanner: CropPlanner,
  varietyFinder: VarietyFinder,
  parcelStatus: ParcelHealth,
  comparator: CropComparator,
  rotationPlanner: RotationPlanner,
  waterBudget: WaterBudget,
  regenerative: RegenerativeSequence,
  yieldProjection: YieldProjection,
  wofostSimulation: WofostSimulation,
  speciesExplorer: SpeciesExplorer,
  catalog: CropCatalog,
  climate: ClimateExplorer,
  phenology: PhenologyBrowser,
  thermal: ThermalTolerance,
  npk: NutrientProfile,
  soil: SoilSuitability,
  rotation: RotationConstraints,
  organic: OrganicInputs,
  pipeline: PipelineRunner,
  sources: SourcesDashboard,
  dadis: BreedDiscovery,
  simulateScenario: SimulateAlternative,
};

function ToolErrorFallback({ toolId, onBack }: { toolId: string; onBack: () => void }) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <Card padding="lg">
      <p className="text-nkz-text-muted mb-3">Failed to load tool: {toolId}</p>
      <Button variant="ghost" onClick={onBack}>{t('app.backToDashboard')}</Button>
    </Card>
  );
}

class ToolErrorBoundary extends Component<
  { children: React.ReactNode; fallback: React.ReactNode },
  { hasError: boolean }
> {
  constructor(props: { children: React.ReactNode; fallback: React.ReactNode }) {
    super(props);
    this.state = { hasError: false };
  }
  static getDerivedStateFromError(_: Error) {
    return { hasError: true };
  }
  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Tool render error:', error, info);
  }
  render() {
    if (this.state.hasError) return this.props.fallback;
    return this.props.children;
  }
}

function ToolView({ toolId, onBack, onNavigateTool }: { toolId: string; onBack: () => void; onNavigateTool: (id: string) => void }) {
  const { t } = useTranslation('bioorchestrator');
  const ToolComponent = TOOL_MAP[toolId];

  if (!ToolComponent) {
    return (
      <Card padding="lg">
        <p className="text-nkz-text-muted">Unknown tool: {toolId}</p>
        <Button variant="ghost" onClick={onBack}>{t('app.backToDashboard')}</Button>
      </Card>
    );
  }

  const extraProps = toolId === 'cropPlanner' ? { onNavigateTool } : {};

  return (
    <Stack gap="section">
      <Button variant="ghost" onClick={onBack} leadingIcon={<ArrowLeft className="w-4 h-4" />}>
        {t('app.backToDashboard')}
      </Button>
      <Suspense fallback={<Spinner size="lg" />}>
        <ToolErrorBoundary fallback={<ToolErrorFallback toolId={toolId} onBack={onBack} />}>
          <ToolComponent {...extraProps} />
        </ToolErrorBoundary>
      </Suspense>
    </Stack>
  );
}

function AppInner() {
  const { t } = useTranslation('bioorchestrator');
  const [searchParams] = useSearchParams();
  const { tenantId } = useAuth();
  const { selectedParcel, setSelectedParcel } = useParcelContext();
  const [initialTarget] = useState(() => resolveDoor(searchParams));
  const [door, setDoor] = useState<Door>(initialTarget?.door ?? 'whatToSow');
  const [toolId, setToolId] = useState<string | null>(initialTarget?.tool ?? null);
  // The URL or a user click decides the door; only otherwise does the parcel's crop.
  const doorDecided = useRef(initialTarget !== null);
  const [parcelInfo, setParcelInfo] = useState<ParcelInfo>({ campaignCrop: null, alertCount: 0, loading: false });
  // Bumped after an assignment so the campaign badge reflects the new crop.
  const [parcelInfoVersion, setParcelInfoVersion] = useState(0);
  const refreshParcelInfo = useCallback(() => setParcelInfoVersion((v) => v + 1), []);
  // Parcel whose default door is already resolved: a refresh must not move the user to another door.
  const doorParcel = useRef<string | null>(null);

  useEffect(() => {
    const parcelId = searchParams.get('parcel');
    if (parcelId) setSelectedParcel(parcelId);
    // Only meant to apply once, from the initial deep-link URL.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // One crop-context fetch per parcel: feeds the default-door choice and the campaign badge.
  useEffect(() => {
    if (!selectedParcel) {
      doorParcel.current = null;
      setParcelInfo({ campaignCrop: null, alertCount: 0, loading: false });
      return;
    }
    let cancelled = false;
    setParcelInfo((prev) => ({ ...prev, loading: true }));
    Promise.all([
      getCropContext(selectedParcel, undefined, tenantId).catch(() => null),
      fetchAlerts(selectedParcel).catch(() => []),
    ]).then(([ctx, alerts]) => {
      if (cancelled) return;
      const assigned = hasAssignedCrop(ctx);
      setParcelInfo({
        campaignCrop: assigned && ctx ? `${ctx.crop.name || ctx.crop.eppo} (${ctx.crop.eppo})` : null,
        alertCount: alerts.length,
        loading: false,
      });
      if (!doorDecided.current && doorParcel.current !== selectedParcel) {
        setDoor(pickDefaultDoor({ hasParcel: true, hasAssignedCrop: assigned }));
      }
      doorParcel.current = selectedParcel;
    });
    return () => { cancelled = true; };
  }, [selectedParcel, tenantId, parcelInfoVersion]);

  // Until the parcel's crop decides the default door, don't mount a door that may be replaced
  // (it would fire a heavy recommend request that is thrown away).
  const doorPending = Boolean(selectedParcel) && !doorDecided.current && doorParcel.current !== selectedParcel;

  const handleDoorChange = (next: Door) => {
    doorDecided.current = true;
    setDoor(next);
  };

  const handleSelectTool = (id: string) => setToolId(id);
  const handleBack = () => setToolId(null);

  return (
    <Card padding="lg">
      <Stack gap="section">
        {/* Header */}
        <div className="flex items-center gap-3">
          <FlaskConical className="w-7 h-7 text-nkz-accent-base" />
          <div>
            <h1 className="text-nkz-2xl font-bold text-nkz-text-primary">
              {t('app.title')}
            </h1>
            <p className="text-nkz-base text-nkz-text-muted mt-1">
              {t('app.subtitle')}
            </p>
          </div>
        </div>

        {/* Global parcel selector */}
        <GlobalParcelSelector />

        <ExplorationModeBanner />

        {/* Content: doors or Tool */}
        {toolId === null ? (
          <Home
            parcelInfo={parcelInfo}
            door={door}
            onDoorChange={handleDoorChange}
            onSelectTool={handleSelectTool}
            onAssigned={refreshParcelInfo}
            doorPending={doorPending}
          />
        ) : (
          <ToolView toolId={toolId} onBack={handleBack} onNavigateTool={handleSelectTool} />
        )}

        <DisclaimerFooter />
      </Stack>
    </Card>
  );
}

const App: React.FC = () => (
  <ParcelProvider>
    <PlanningScenarioProvider>
      <ExpertModeProvider>
        <AppInner />
      </ExpertModeProvider>
    </PlanningScenarioProvider>
  </ParcelProvider>
);

export default App;
