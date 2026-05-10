import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { SlotShell } from '@nekazari/viewer-kit';
import { Activity, GitBranch, Sprout, Database } from 'lucide-react';
import SourcesDashboard from './components/SourcesDashboard';
import PipelineRunner from './components/PipelineRunner';
import PhenologyBrowser from './components/PhenologyBrowser';
import { BreedDiscovery } from './components/DADIS/BreedDiscovery';
import './i18n';

const bioAccent = { base: '#14B8A6', soft: '#CCFBF1', strong: '#0D9488' };

const TABS = [
  { id: 'sources', icon: Activity },
  { id: 'pipeline', icon: GitBranch },
  { id: 'phenology', icon: Sprout },
  { id: 'dadis', icon: Database },
] as const;

type TabId = (typeof TABS)[number]['id'];

const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState<TabId>('sources');
  const { t } = useTranslation('bioorchestrator');

  return (
    <SlotShell
      moduleId="bioorchestrator"
      accent={bioAccent}
      title={t('app.title')}
    >
      <div className="flex border-b border-nkz-border mb-4">
        {TABS.map((tab) => {
          const Icon = tab.icon;
          const isActive = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              className={`flex items-center gap-1.5 px-nkz-stack py-nkz-inline text-nkz-sm font-medium border-b-2 transition-colors duration-nkz-fast ${
                isActive
                  ? 'text-nkz-accent-base border-nkz-accent-base'
                  : 'border-transparent text-nkz-text-muted hover:text-nkz-text-primary'
              }`}
              onClick={() => setActiveTab(tab.id)}
            >
              <Icon className="w-4 h-4" />
              {t(`app.tabs.${tab.id}`)}
            </button>
          );
        })}
      </div>

      {activeTab === 'sources' && <SourcesDashboard />}
      {activeTab === 'pipeline' && <PipelineRunner />}
      {activeTab === 'phenology' && <PhenologyBrowser />}
      {activeTab === 'dadis' && <BreedDiscovery />}
    </SlotShell>
  );
};

export default App;
