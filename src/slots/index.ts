import SourceStatusWidget from './SourceStatusWidget';
import RecommendationsPanel from '../components/RecommendationsPanel';
import CropPlanPanel from '../components/crop-plan/CropPlanPanel';

const MODULE_ID = 'bioorchestrator';

export const moduleSlots = {
  'context-panel': [
    {
      id: 'bioorchestrator-crop-plan',
      moduleId: MODULE_ID,
      component: 'CropPlanPanel',
      localComponent: CropPlanPanel,
      priority: 15,
      showWhen: { entityType: ['AgriParcel'] },
    },
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
  // The viewer's bottom panel is the time axis of the selected entity. The
  // ingestion pipeline runner is an admin tool, so it lives on the module page only.
  'bottom-panel': [],
};
