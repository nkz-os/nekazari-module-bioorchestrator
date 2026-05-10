import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { SlotShell } from '@nekazari/viewer-kit';
import { Tabs } from '@nekazari/ui-kit';
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
      <Tabs.Root value={activeTab} onValueChange={(v) => setActiveTab(v as TabId)}>
        <Tabs.List>
          {TABS.map((tab) => {
            const Icon = tab.icon;
            return (
              <Tabs.Trigger key={tab.id} value={tab.id}>
                <span className="flex items-center gap-1.5">
                  <Icon className="w-4 h-4" />
                  {t(`app.tabs.${tab.id}`)}
                </span>
              </Tabs.Trigger>
            );
          })}
        </Tabs.List>

        <div className="mt-4">
          <Tabs.Content value="sources">
            <SourcesDashboard />
          </Tabs.Content>
          <Tabs.Content value="pipeline">
            <PipelineRunner />
          </Tabs.Content>
          <Tabs.Content value="phenology">
            <PhenologyBrowser />
          </Tabs.Content>
          <Tabs.Content value="dadis">
            <BreedDiscovery />
          </Tabs.Content>
        </div>
      </Tabs.Root>
    </SlotShell>
  );
};

export default App;
