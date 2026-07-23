import { describe, it, expect } from 'vitest';
import { buildBioorchestratorToolUrl, resolveToolFromSearchParams } from './navigation';

describe('buildBioorchestratorToolUrl', () => {
  it('includes both parcel and tool when parcelId is provided', () => {
    expect(buildBioorchestratorToolUrl('urn:ngsi-ld:AgriParcel:t1:P1', 'varietyFinder'))
      .toBe('/bioorchestrator?tool=varietyFinder&parcel=urn%3Angsi-ld%3AAgriParcel%3At1%3AP1');
  });

  it('omits the parcel param when parcelId is undefined', () => {
    expect(buildBioorchestratorToolUrl(undefined, 'cropPlanner'))
      .toBe('/bioorchestrator?tool=cropPlanner');
  });

  it('omits the parcel param when parcelId is an empty string', () => {
    expect(buildBioorchestratorToolUrl('', 'varietyFinder'))
      .toBe('/bioorchestrator?tool=varietyFinder');
  });
});

describe('resolveToolFromSearchParams', () => {
  it('returns "cropPlanner" when ?tool=cropPlanner', () => {
    const params = new URLSearchParams('tool=cropPlanner');
    expect(resolveToolFromSearchParams(params)).toBe('cropPlanner');
  });

  it('returns "varietyFinder" when ?tool=varietyFinder', () => {
    const params = new URLSearchParams('tool=varietyFinder');
    expect(resolveToolFromSearchParams(params)).toBe('varietyFinder');
  });

  it('returns null when ?tool is missing', () => {
    const params = new URLSearchParams('parcel=urn:ngsi-ld:AgriParcel:t1:P1');
    expect(resolveToolFromSearchParams(params)).toBeNull();
  });

  it('returns null when ?tool is an unrecognized value', () => {
    const params = new URLSearchParams('tool=somethingElse');
    expect(resolveToolFromSearchParams(params)).toBeNull();
  });
});
