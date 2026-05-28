import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Button } from '@nekazari/ui-kit';
import { Activity, GitBranch, Sprout, Database, Leaf } from 'lucide-react';
import SourcesDashboard from './components/SourcesDashboard';
import PipelineRunner from './components/PipelineRunner';
import PhenologyBrowser from './components/PhenologyBrowser';
import { BreedDiscovery } from './components/DADIS/BreedDiscovery';
import CropCatalog from './components/CropCatalog';
import CropDetail from './components/CropDetail';
import ContributeWizard from './components/ContributeWizard';
import type { CropItem } from './services/api';
import './i18n';

const TABS = [
  { id: 'catalog', icon: Leaf },
  { id: 'sources', icon: Activity },
  { id: 'pipeline', icon: GitBranch },
  { id: 'phenology', icon: Sprout },
  { id: 'dadis', icon: Database },
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
      <div className="flex border-b border-nkz-border mb-4">
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
              {t(`app.tabs.${tab.id}`, tab.id)}
            </button>
          );
        })}
      </div>

      {active === 'catalog' && view === 'catalog' && (
        <CropCatalog onSelectCrop={(crop) => { setSelectedCrop(crop); setView('detail'); }} />
      )}
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

      {active === 'sources' && <SourcesDashboard />}
      {active === 'pipeline' && <PipelineRunner />}
      {active === 'phenology' && <PhenologyBrowser />}
      {active === 'dadis' && <BreedDiscovery />}
    </Card>
  );
};

export default App;
