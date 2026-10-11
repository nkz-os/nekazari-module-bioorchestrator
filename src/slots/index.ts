import ParcelPlanWidget from '../planning/ParcelPlanWidget';

const MODULE_ID = 'bioorchestrator';

export const moduleSlots = {
  'context-panel': [
    {
      id: 'bioorchestrator-parcel-plan',
      moduleId: MODULE_ID,
      component: 'ParcelPlanWidget',
      localComponent: ParcelPlanWidget,
      priority: 15,
      showWhen: { entityType: ['AgriParcel'] },
    },
  ],
  'bottom-panel': [],
};
