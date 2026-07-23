import { describe, it, expect } from 'vitest';
import { resolveCropTypeFromContext } from './cropContext';

describe('resolveCropTypeFromContext', () => {
  it('prefers the crop display name', () => {
    expect(resolveCropTypeFromContext({
      crop: { eppo: 'TRZAX', name: 'Wheat', scientific_name: 'Triticum aestivum' },
    })).toBe('Wheat');
  });

  it('falls back to scientific_name when name is empty', () => {
    expect(resolveCropTypeFromContext({
      crop: { eppo: 'TRZAX', name: '', scientific_name: 'Triticum aestivum' },
    })).toBe('Triticum aestivum');
  });

  it('falls back to eppo code when name and scientific_name are both absent', () => {
    expect(resolveCropTypeFromContext({
      crop: { eppo: 'TRZAX', name: '', scientific_name: null },
    })).toBe('TRZAX');
  });

  it('returns null when there is no crop field at all', () => {
    expect(resolveCropTypeFromContext({})).toBeNull();
  });

  it('returns null when context itself is null/undefined', () => {
    expect(resolveCropTypeFromContext(null)).toBeNull();
    expect(resolveCropTypeFromContext(undefined)).toBeNull();
  });
});
