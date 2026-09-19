import { describe, it, expect } from 'vitest';
import { resolveCropTypeFromContext, resolveCropDisplayName } from './cropContext';

describe('resolveCropTypeFromContext', () => {
  it('prefers the EPPO code (canonical query identifier)', () => {
    expect(resolveCropTypeFromContext({
      crop: { eppo: 'TRZAX', name: 'Wheat', scientific_name: 'Triticum aestivum' },
    })).toBe('TRZAX');
  });

  it('falls back to name when eppo is absent', () => {
    expect(resolveCropTypeFromContext({
      crop: { eppo: '', name: 'Wheat', scientific_name: 'Triticum aestivum' },
    })).toBe('Wheat');
  });

  it('falls back to scientific_name when eppo and name are absent', () => {
    expect(resolveCropTypeFromContext({
      crop: { eppo: '', name: '', scientific_name: 'Triticum aestivum' },
    })).toBe('Triticum aestivum');
  });

  it('returns null when there is no crop field at all', () => {
    expect(resolveCropTypeFromContext({})).toBeNull();
  });

  it('returns null when context itself is null/undefined', () => {
    expect(resolveCropTypeFromContext(null)).toBeNull();
    expect(resolveCropTypeFromContext(undefined)).toBeNull();
  });
});

describe('resolveCropDisplayName', () => {
  it('prefers the human display name', () => {
    expect(resolveCropDisplayName({
      crop: { eppo: 'TRZAX', name: 'Wheat', scientific_name: 'Triticum aestivum' },
    })).toBe('Wheat');
  });

  it('falls back to scientific_name when name is empty', () => {
    expect(resolveCropDisplayName({
      crop: { eppo: 'TRZAX', name: '', scientific_name: 'Triticum aestivum' },
    })).toBe('Triticum aestivum');
  });

  it('falls back to eppo when name and scientific_name are absent', () => {
    expect(resolveCropDisplayName({
      crop: { eppo: 'TRZAX', name: '', scientific_name: null },
    })).toBe('TRZAX');
  });
});
