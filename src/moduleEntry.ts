/**
 * NKZ Module Entry Point — IIFE bundle export.
 *
 * This is the entry point for the IIFE bundle that the NKZ platform loads.
 * It exports the module component, routes, and slot components.
 */

import App from './App';
import SourceStatusWidget from './slots/SourceStatusWidget';
import { PipelineRunner } from './slots/PipelineRunnerWidget';
import RecommendationsPanel from './components/RecommendationsPanel';

const MODULE_ID = 'bioorchestrator';

export interface SlotWidgetDefinition {
  id: string;
  moduleId: string;
  component: string;
  priority: number;
  localComponent: React.ComponentType<any>;
  defaultProps?: Record<string, any>;
  showWhen?: {
    entityType?: string[];
    layerActive?: string[];
  };
}

export type SlotType = 'layer-toggle' | 'context-panel' | 'bottom-panel' | 'entity-tree' | 'map-layer';

export interface ModuleViewerSlots {
  'layer-toggle'?: SlotWidgetDefinition[];
  'context-panel'?: SlotWidgetDefinition[];
  'bottom-panel'?: SlotWidgetDefinition[];
  'entity-tree'?: SlotWidgetDefinition[];
  'map-layer'?: SlotWidgetDefinition[];
}

const viewerSlots: ModuleViewerSlots = {
  'context-panel': [
    {
      id: 'bioorchestrator-source-status',
      moduleId: MODULE_ID,
      component: 'SourceStatusWidget',
      priority: 20,
      localComponent: SourceStatusWidget,
    },
    {
      id: 'bioorchestrator-recommendations',
      moduleId: MODULE_ID,
      component: 'RecommendationsPanel',
      priority: 25,
      localComponent: RecommendationsPanel,
      showWhen: {
        entityType: ['AgriParcel'],
      },
    },
  ],
  'bottom-panel': [
    {
      id: 'bioorchestrator-pipeline-runner',
      moduleId: MODULE_ID,
      component: 'PipelineRunner',
      priority: 30,
      localComponent: PipelineRunner,
    },
  ],
  'layer-toggle': [],
  'entity-tree': [],
  'map-layer': [],
};

// Register with host via NKZ API
function tryRegister(): void {
  if (typeof window !== 'undefined' && (window as any).__NKZ__) {
    (window as any).__NKZ__.register({
      id: MODULE_ID,
      main: App,
      viewerSlots,
      version: '0.1.0',
    });
  } else {
    const attempts = (window as any).__nkzRegisterAttempts || 0;
    if (attempts < 50) {
      (window as any).__nkzRegisterAttempts = attempts + 1;
      setTimeout(tryRegister, 100);
    }
  }
}

tryRegister();

// Legacy export for backward compatibility
const moduleExport = {
    id: MODULE_ID,
    component: App,
    viewerSlots,
};

export default moduleExport;
