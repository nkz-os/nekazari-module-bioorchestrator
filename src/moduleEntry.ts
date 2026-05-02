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

const moduleExport = {
    id: 'bioorchestrator',
    component: App,
    routes: [
        { path: '/bioorchestrator', component: App },
    ],
    slots: {
        'context-panel': {
            SourcesStatus: SourceStatusWidget,
            Recommendations: RecommendationsPanel,
        },
        'bottom-panel': {
            PipelineRunner: PipelineRunner,
        },
    },
};

// Attach to window for IIFE consumption
(window as any).__NKZ_MODULE_BIOORCHESTRATOR__ = moduleExport;

export default moduleExport;
