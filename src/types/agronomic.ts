export type Confidence = 'high' | 'medium' | 'low';

export interface AgronomicSource {
  short: string;
  doi?: string | null;
  institution?: string | null;
}

export interface AgronomicValue {
  value: number | string | null;
  source: AgronomicSource;
  confidence: Confidence;
  fidelity?: string | null;
  notes?: string[];
}
