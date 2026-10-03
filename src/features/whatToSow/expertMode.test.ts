import { describe, it, expect } from 'vitest';
import { resolveExpertMode } from './expertMode';

describe('resolveExpertMode', () => {
  it('url param wins over stored and roles', () => {
    expect(resolveExpertMode({ urlParam: '1', stored: '0', roles: [] })).toBe(true);
    expect(resolveExpertMode({ urlParam: '0', stored: '1', roles: ['TechnicalConsultant'] })).toBe(false);
  });
  it('stored wins over roles when url param is absent or invalid', () => {
    expect(resolveExpertMode({ urlParam: null, stored: '1', roles: [] })).toBe(true);
    expect(resolveExpertMode({ urlParam: 'yes', stored: '0', roles: ['PlatformAdmin'] })).toBe(false);
  });
  it('defaults on for TechnicalConsultant and PlatformAdmin', () => {
    expect(resolveExpertMode({ roles: ['TechnicalConsultant'] })).toBe(true);
    expect(resolveExpertMode({ roles: ['Farmer', 'PlatformAdmin'] })).toBe(true);
  });
  it('defaults off for other roles, empty or undefined roles', () => {
    expect(resolveExpertMode({ roles: ['Farmer', 'TenantAdmin'] })).toBe(false);
    expect(resolveExpertMode({ roles: [] })).toBe(false);
    expect(resolveExpertMode({ roles: undefined })).toBe(false);
    expect(resolveExpertMode({})).toBe(false);
  });
});
