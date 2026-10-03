import type React from 'react';
import {
  Heart, Activity, RefreshCw, Droplets, Dna, Microscope,
  Leaf, Globe, Sprout, Thermometer, FlaskRound, GitBranch, Database, Mountain,
  TrendingUp,
} from 'lucide-react';

export type HubId = 'planning' | 'campaign' | 'codex';

export interface ToolCardDef {
  id: string;
  icon: React.ElementType;
  hub: HubId;
  compact?: boolean;
}

export const ALL_TOOLS: ToolCardDef[] = [
  { id: 'cropManagement', icon: Sprout, hub: 'planning' },
  { id: 'parcelStatus', icon: Heart, hub: 'campaign' },
  { id: 'yieldProjection', icon: TrendingUp, hub: 'campaign' },
  { id: 'waterBudget', icon: Droplets, hub: 'campaign' },
  { id: 'wofostSimulation', icon: Microscope, hub: 'campaign' },
  { id: 'simulateScenario', icon: Activity, hub: 'campaign' },
];

export const ADVANCED_TOOLS: ToolCardDef[] = [
  { id: 'regenerative', icon: Dna, hub: 'codex', compact: true },
  { id: 'catalog', icon: Leaf, hub: 'codex', compact: true },
  { id: 'climate', icon: Globe, hub: 'codex', compact: true },
  { id: 'phenology', icon: Sprout, hub: 'codex', compact: true },
  { id: 'thermal', icon: Thermometer, hub: 'codex', compact: true },
  { id: 'npk', icon: Droplets, hub: 'codex', compact: true },
  { id: 'soil', icon: Mountain, hub: 'codex', compact: true },
  { id: 'rotation', icon: RefreshCw, hub: 'codex', compact: true },
  { id: 'organic', icon: FlaskRound, hub: 'codex', compact: true },
  { id: 'pipeline', icon: GitBranch, hub: 'codex', compact: true },
  { id: 'sources', icon: Activity, hub: 'codex', compact: true },
  { id: 'dadis', icon: Database, hub: 'codex', compact: true },
  { id: 'speciesExplorer', icon: Leaf, hub: 'codex', compact: true },
];

