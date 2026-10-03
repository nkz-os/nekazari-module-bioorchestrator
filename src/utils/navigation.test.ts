import { describe, it, expect } from 'vitest';
import {
  buildBioorchestratorToolUrl,
  resolveDoor,
  pickDefaultDoor,
  hasAssignedCrop,
  CAMPAIGN_TOOL_IDS,
  LIBRARY_TOOL_IDS,
} from './navigation';

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

  it('accepts the whatToSow tool', () => {
    expect(buildBioorchestratorToolUrl('P1', 'whatToSow')).toBe('/bioorchestrator?tool=whatToSow&parcel=P1');
  });
});

const sp = (q: string) => new URLSearchParams(q);

describe('resolveDoor', () => {
  it.each(['cropPlanner', 'varietyFinder', 'comparator', 'rotationPlanner', 'whatToSow'])(
    'legacy tool %s lands on whatToSow without a tool',
    (id) => {
      expect(resolveDoor(sp(`tool=${id}&parcel=P1`))).toEqual({ door: 'whatToSow' });
    },
  );

  it.each([...CAMPAIGN_TOOL_IDS])('campaign tool %s keeps its tool', (id) => {
    expect(resolveDoor(sp(`tool=${id}`))).toEqual({ door: 'campaign', tool: id });
  });

  it.each([...LIBRARY_TOOL_IDS])('library tool %s keeps its tool', (id) => {
    expect(resolveDoor(sp(`tool=${id}`))).toEqual({ door: 'library', tool: id });
  });

  it('returns null when nothing is given', () => {
    expect(resolveDoor(sp('parcel=P1'))).toBeNull();
  });

  it('returns null for an unknown tool', () => {
    expect(resolveDoor(sp('tool=somethingElse'))).toBeNull();
  });

  it('honours an explicit door', () => {
    expect(resolveDoor(sp('door=library'))).toEqual({ door: 'library' });
    expect(resolveDoor(sp('door=campaign'))).toEqual({ door: 'campaign' });
    expect(resolveDoor(sp('door=whatToSow'))).toEqual({ door: 'whatToSow' });
  });

  it('ignores an invalid door and falls back to the tool', () => {
    expect(resolveDoor(sp('door=nope'))).toBeNull();
    expect(resolveDoor(sp('door=nope&tool=waterBudget'))).toEqual({ door: 'campaign', tool: 'waterBudget' });
  });

  it('tool wins over door', () => {
    expect(resolveDoor(sp('door=library&tool=cropPlanner'))).toEqual({ door: 'whatToSow' });
  });

  it('keeps the two tool lists disjoint', () => {
    expect(CAMPAIGN_TOOL_IDS.filter((id) => (LIBRARY_TOOL_IDS as readonly string[]).includes(id))).toEqual([]);
  });
});

describe('hasAssignedCrop', () => {
  it('is true for a real EPPO code', () => {
    expect(hasAssignedCrop({ crop: { eppo: 'TRZAX' } })).toBe(true);
  });
  it('is false for missing, empty or "unknown"', () => {
    expect(hasAssignedCrop(null)).toBe(false);
    expect(hasAssignedCrop({})).toBe(false);
    expect(hasAssignedCrop({ crop: { eppo: '' } })).toBe(false);
    expect(hasAssignedCrop({ crop: { eppo: 'unknown' } })).toBe(false);
  });
});

describe('pickDefaultDoor', () => {
  it('campaign only when a parcel has an assigned crop', () => {
    expect(pickDefaultDoor({ hasParcel: true, hasAssignedCrop: true })).toBe('campaign');
  });
  it('whatToSow when the parcel has no crop', () => {
    expect(pickDefaultDoor({ hasParcel: true, hasAssignedCrop: false })).toBe('whatToSow');
  });
  it('whatToSow without a parcel', () => {
    expect(pickDefaultDoor({ hasParcel: false, hasAssignedCrop: true })).toBe('whatToSow');
  });
});
