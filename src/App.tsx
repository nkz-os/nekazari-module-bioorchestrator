import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import SourcesDashboard from './components/SourcesDashboard';
import PipelineRunner from './components/PipelineRunner';
import { BreedDiscovery } from './components/DADIS/BreedDiscovery';
import './styles.css';
import './i18n';

type Tab = 'sources' | 'pipeline' | 'dadis';

const App: React.FC = () => {
    const [activeTab, setActiveTab] = useState<Tab>('sources');
    const { t } = useTranslation('bioorchestrator');

    return (
        <div className="bio-container">
            <header className="bio-header">
                <div className="bio-header-content">
                    <span className="bio-icon">🌿</span>
                    <div>
                        <h1 className="bio-title">{t('app.title')}</h1>
                        <p className="bio-subtitle">
                            {t('app.subtitle')}
                        </p>
                    </div>
                </div>

                <nav className="bio-tabs mt-4 flex gap-2">
                    <button
                        className={`bio-tab px-4 py-2 font-medium border-b-2 ${activeTab === 'sources' ? 'border-green-600 text-green-700' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                        onClick={() => setActiveTab('sources')}
                    >
                        📡 {t('app.tabs.sources')}
                    </button>
                    <button
                        className={`bio-tab px-4 py-2 font-medium border-b-2 ${activeTab === 'pipeline' ? 'border-green-600 text-green-700' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                        onClick={() => setActiveTab('pipeline')}
                    >
                        ⚙️ {t('app.tabs.pipeline')}
                    </button>
                    <button
                        className={`bio-tab px-4 py-2 font-medium border-b-2 ${activeTab === 'dadis' ? 'border-green-600 text-green-700' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                        onClick={() => setActiveTab('dadis')}
                    >
                        🐄 {t('app.tabs.dadis')}
                    </button>
                </nav>
            </header>

            <main className="bio-main min-h-[500px]">
                {activeTab === 'sources' && <SourcesDashboard />}
                {activeTab === 'pipeline' && <PipelineRunner />}
                {activeTab === 'dadis' && <BreedDiscovery />}
            </main>
        </div>
    );
};

export default App;
