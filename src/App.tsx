import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Button } from '@nekazari/ui-kit';
import { Activity, GitBranch, Sprout, Database, Leaf, Thermometer, Droplets, RefreshCw, Search, Globe } from 'lucide-react';
import SourcesDashboard from './components/SourcesDashboard';
import PipelineRunner from './components/PipelineRunner';
import PhenologyBrowser from './components/PhenologyBrowser';
import { BreedDiscovery } from './components/DADIS/BreedDiscovery';
import CropCatalog from './components/CropCatalog';
import CropDetail from './components/CropDetail';
import ContributeWizard from './components/ContributeWizard';
import ThermalTolerance from './components/ThermalTolerance';
import NutrientProfile from './components/NutrientProfile';
import SoilSuitability from './components/SoilSuitability';
import RotationConstraints from './components/RotationConstraints';
import VarietyFinder from './components/VarietyFinder';
import ClimateExplorer from './components/ClimateExplorer';
import type { CropItem } from './services/api';
import './i18n';

const TABS = [
  { id: 'catalog', icon: Leaf },
  { id: 'variety-finder', icon: Search },
  { id: 'climate', icon: Globe },
  { id: 'phenology', icon: Sprout },
  { id: 'thermal', icon: Thermometer },
  { id: 'npk', icon: Droplets },
  { id: 'soil', icon: Sprout },
  { id: 'rotation', icon: RefreshCw },
  { id: 'pipeline', icon: GitBranch },
  { id: 'dadis', icon: Database },
  { id: 'sources', icon: Activity },
] as const;
type TabId = (typeof TABS)[number]['id'];

const App: React.FC = () => {
  const [active, setActive] = useState<TabId>('catalog');
  const { t } = useTranslation('bioorchestrator');
  const [selectedCrop, setSelectedCrop] = useState<CropItem | null>(null);
  const [showContribute, setShowContribute] = useState(false);
  const [view, setView] = useState<'catalog' | 'detail'>('catalog');

  const handleTabChange = (tabId: TabId) => {
    setActive(tabId);
    if (tabId !== 'catalog') {
      setView('catalog');
    }
  };

  return (
    <Card padding="md">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-nkz-lg font-bold text-nkz-text-primary">{t('app.title')}</h1>
      </div>
      <div className="flex flex-wrap border-b border-nkz-border mb-4">
        {TABS.map((tab) => {
          const Icon = tab.icon;
          const isActive = active === tab.id;
          return (
            <button
              key={tab.id}
              className={`flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 transition-colors ${isActive ? 'text-nkz-accent-base border-nkz-accent-base' : 'border-transparent text-nkz-text-muted hover:text-nkz-text-primary'}`}
              onClick={() => handleTabChange(tab.id)}
            >
              <Icon className="w-4 h-4" />
              {t(`app.tabs.${tab.id}`)}
            </button>
          );
        })}
      </div>

      {active === 'catalog' && view === 'catalog' && (
        <CropCatalog onSelectCrop={(crop) => { setSelectedCrop(crop); setView('detail'); }} />
      )}
      {active === 'variety-finder' && <VarietyFinder />}
      {active === 'climate' && <ClimateExplorer />}
      {active === 'catalog' && view === 'detail' && selectedCrop && (
        <>
          <Button variant="ghost" onClick={() => setView('catalog')}>{'< Back'}</Button>
          <CropDetail
            cropId={selectedCrop.uri}
            onContribute={() => setShowContribute(true)}
            onViewInParcel={() => {}}
          />
        </>
      )}
      {showContribute && selectedCrop && (
        <ContributeWizard
          cropId={selectedCrop.uri}
          cropName={selectedCrop.name}
          onClose={() => setShowContribute(false)}
          onSuccess={() => { setShowContribute(false); setView('detail'); }}
        />
      )}

      {active === 'phenology' && <PhenologyBrowser />}
      {active === 'thermal' && <ThermalTolerance />}
      {active === 'npk' && <NutrientProfile />}
      {active === 'soil' && <SoilSuitability />}
      {active === 'rotation' && <RotationConstraints />}
      {active === 'pipeline' && <PipelineRunner />}
      {active === 'dadis' && <BreedDiscovery />}
      {active === 'sources' && <SourcesDashboard />}
    </Card>
  );
};

export default App;
