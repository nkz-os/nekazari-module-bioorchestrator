import type { AgronomicValue } from './agronomic';

export interface PlanSegment {
  id: string;
  seq: number;
  status: 'planned' | 'active' | 'harvested' | 'terminated';
  cropSeason?: string;
  species?: string;
  variety?: string;
  sowingWindowStart?: string;
  sowingWindowEnd?: string;
  expectedTerminationDate?: string;
  plantingDate?: string;
  terminationDate?: string;
}

export interface CropPlan {
  parcel_id: string;
  season: string;
  active: string | null;
  segments: PlanSegment[];
}

export interface WaterBudget {
  parcel_id: string;
  irrigation_required_mm: number;
  etc_weekly_mm: number;
  kc: number;
  recommendation?: string;
  confidence?: string;
  agronomic?: Record<string, AgronomicValue>;
}

export interface IssuedOp {
  id: string;
  operationType: string;
  status: string;
  parcel_id?: string;
  priority?: string;
  urgency?: string;
  dueDate?: string;
  sowingWindowEnd?: string;
  sourceRule?: string;
  description?: string;
  workOrder?: string;
}

export interface PhenologyStage {
  stage: string;
  startDate?: string;
  endDate?: string;
  current?: boolean;
}

export interface PhenologyStatus {
  parcelId?: string;
  currentStage?: string;
  deviation?: string;
  seasonStart?: string;
  dataFidelity?: string;
  stages?: PhenologyStage[];
  status?: string;
  agronomic?: Record<string, AgronomicValue>;
}
