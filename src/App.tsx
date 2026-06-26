import React, { useState, lazy, Suspense, Component, ErrorInfo } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Stack, Spinner, Button } from '@nekazari/ui-kit';
import { ArrowLeft, FlaskConical } from 'lucide-react';
import { ParcelProvider } from './context/ParcelContext';
import { PlanningScenarioProvider } from './context/PlanningScenarioContext';
import GlobalParcelSelector from './components/GlobalParcelSelector';
import ExplorationModeBanner from './components/ExplorationModeBanner';
import Dashboard from './components/Dashboard';
import DisclaimerFooter from './components/DisclaimerFooter';
import './i18n';

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
const WofostSimulation = lazy(() => import('./components/WofostSimulation'));
const BreedDiscovery = lazy(() => import('./components/DADIS/BreedDiscovery').then(m => ({ default: m.BreedDiscovery })));

type ViewState = { mode: 'dashboard' } | { mode: 'tool'; toolId: string };

const TOOL_MAP: Record<string, React.LazyExoticComponent<React.ComponentType<any>>> = {
  varietyFinder: VarietyFinder,
  parcelStatus: ParcelHealth,
  comparator: CropComparator,
  rotationPlanner: RotationPlanner,
  waterBudget: WaterBudget,
  regenerative: RegenerativeSequence,
  wofostSimulation: WofostSimulation,
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

function ToolView({ toolId, onBack }: { toolId: string; onBack: () => void }) {
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

  return (
    <Stack gap="section">
      <Button variant="ghost" onClick={onBack} leadingIcon={<ArrowLeft className="w-4 h-4" />}>
        {t('app.backToDashboard')}
      </Button>
      <Suspense fallback={<Spinner size="lg" />}>
        <ToolErrorBoundary fallback={<ToolErrorFallback toolId={toolId} onBack={onBack} />}>
          <ToolComponent />
        </ToolErrorBoundary>
      </Suspense>
    </Stack>
  );
}

function AppInner() {
  const { t } = useTranslation('bioorchestrator');
  const [view, setView] = useState<ViewState>({ mode: 'dashboard' });

  const handleSelectTool = (toolId: string) => {
    setView({ mode: 'tool', toolId });
  };

  const handleBack = () => {
    setView({ mode: 'dashboard' });
  };

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

        {/* Content: Dashboard or Tool */}
        {view.mode === 'dashboard' ? (
          <Dashboard onSelectTool={handleSelectTool} />
        ) : (
          <ToolView toolId={view.toolId} onBack={handleBack} />
        )}

        <DisclaimerFooter />
      </Stack>
    </Card>
  );
}

const App: React.FC = () => (
  <ParcelProvider>
    <PlanningScenarioProvider>
      <AppInner />
    </PlanningScenarioProvider>
  </ParcelProvider>
);

export default App;
