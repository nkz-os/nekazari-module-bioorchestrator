import { describe, it, expect } from 'vitest';
import { confidenceToken } from './agronomic.utils';

describe('confidenceToken', () => {
  it('maps each confidence to a distinct token set', () => {
    expect(confidenceToken('high').dot).toContain('success');
    expect(confidenceToken('medium').dot).toContain('warning');
    expect(confidenceToken('low').dot).toContain('muted');
    expect(confidenceToken('high').label).toBe('badge.confidence.high');
  });
});
