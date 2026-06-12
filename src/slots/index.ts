import SourceStatusWidget from './SourceStatusWidget';
import { PipelineRunner } from './PipelineRunnerWidget';
import RecommendationsPanel from '../components/RecommendationsPanel';

const MODULE_ID = 'bioorchestrator';

export const moduleSlots = {
  'context-panel': [
    {
      id: 'bioorchestrator-source-status',
      moduleId: MODULE_ID,
      component: 'SourceStatusWidget',
      localComponent: SourceStatusWidget,
      priority: 20,
    },
    {
      id: 'bioorchestrator-recommendations',
      moduleId: MODULE_ID,
      component: 'RecommendationsPanel',
      localComponent: RecommendationsPanel,
      priority: 25,
      showWhen: { entityType: ['AgriParcel', 'AgriCrop'] },
    },
  ],
  'bottom-panel': [
    {
      id: 'bioorchestrator-pipeline-runner',
      moduleId: MODULE_ID,
      component: 'PipelineRunner',
      localComponent: PipelineRunner,
      priority: 30,
    },
  ],
};
