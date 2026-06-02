import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Button } from '@nekazari/ui-kit';
import { Activity, GitBranch, Sprout, Database, Leaf, Thermometer, Droplets, RefreshCw, Search, Globe, Heart } from 'lucide-react';
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
import ParcelHealth from './components/ParcelHealth';
import WaterBudget from './components/WaterBudget';
import type { CropItem } from './services/api';
import './i18n';

const TAB_GROUPS = [
  {
    group: 'parcel' as const,
    label: 'app.groups.parcel',
    tabs: [
      { id: 'variety-finder' as const, icon: Search, label: 'app.tabs.varietyFinder' },
      { id: 'parcel-health' as const, icon: Heart, label: 'app.tabs.parcelHealth' },
      { id: 'water-budget' as const, icon: Droplets, label: 'app.tabs.waterBudget' },
    ],
  },
  {
    group: 'reference' as const,
    label: 'app.groups.reference',
    tabs: [
      { id: 'catalog' as const, icon: Leaf, label: 'app.tabs.catalog' },
      { id: 'climate' as const, icon: Globe, label: 'app.tabs.climate' },
      { id: 'phenology' as const, icon: Sprout, label: 'app.tabs.phenology' },
      { id: 'thermal' as const, icon: Thermometer, label: 'app.tabs.thermal' },
      { id: 'npk' as const, icon: Droplets, label: 'app.tabs.npk' },
      { id: 'soil' as const, icon: Sprout, label: 'app.tabs.soil' },
      { id: 'rotation' as const, icon: RefreshCw, label: 'app.tabs.rotation' },
      { id: 'pipeline' as const, icon: GitBranch, label: 'app.tabs.pipeline' },
      { id: 'dadis' as const, icon: Database, label: 'app.tabs.dadis' },
      { id: 'sources' as const, icon: Activity, label: 'app.tabs.sources' },
    ],
  },
] as const;

type TabId = typeof TAB_GROUPS[number]['tabs'][number]['id'];

const App: React.FC = () => {
  const [active, setActive] = useState<TabId>('variety-finder');
  const { t } = useTranslation('bioorchestrator');
  const [selectedCrop, setSelectedCrop] = useState<CropItem | null>(null);
  const [showContribute, setShowContribute] = useState(false);
  const [view, setView] = useState<'catalog' | 'detail'>('catalog');

  const handleTabChange = (tabId: TabId) => {
    setActive(tabId);
    if (tabId !== 'catalog') setView('catalog');
  };

  return (
    <Card padding="md">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-nkz-lg font-bold text-nkz-text-primary">{t('app.title')}</h1>
      </div>

      {TAB_GROUPS.map((group) => (
        <div key={group.group}>
          <div className="text-xs text-nkz-text-muted uppercase tracking-wider px-4 py-1 mt-2 first:mt-0">
            {t(group.label)}
          </div>
          <div className="flex flex-wrap border-b border-nkz-border mb-4">
            {group.tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = active === tab.id;
              return (
                <button
                  key={tab.id}
                  className={`flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 transition-colors ${isActive ? 'text-nkz-accent-base border-nkz-accent-base' : 'border-transparent text-nkz-text-muted hover:text-nkz-text-primary'}`}
                  onClick={() => handleTabChange(tab.id)}
                >
                  <Icon className="w-4 h-4" />
                  {t(tab.label)}
                </button>
              );
            })}
          </div>
        </div>
      ))}

      {active === 'catalog' && view === 'catalog' && (
        <CropCatalog onSelectCrop={(crop) => { setSelectedCrop(crop); setView('detail'); }} />
      )}
      {active === 'variety-finder' && <VarietyFinder />}
      {active === 'parcel-health' && <ParcelHealth />}
      {active === 'water-budget' && <WaterBudget />}
      {active === 'climate' && <ClimateExplorer />}
      {active === 'catalog' && view === 'detail' && selectedCrop && (
        <>
          <Button variant="ghost" onClick={() => setView('catalog')}>{'< Back'}</Button>
          <CropDetail cropId={selectedCrop.uri} onContribute={() => setShowContribute(true)} onViewInParcel={() => {}} />
        </>
      )}
      {showContribute && selectedCrop && (
        <ContributeWizard cropId={selectedCrop.uri} cropName={selectedCrop.name} onClose={() => setShowContribute(false)} onSuccess={() => { setShowContribute(false); setView('detail'); }} />
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
