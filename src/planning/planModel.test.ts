import { describe, expect, it } from 'vitest';
import { COVER_METHODS, isOtherCover, RAW_SPECIES_NAME } from './planModel';

describe('isOtherCover', () => {
  const listed = ['vicia', 'avena'];
  it('a listed species is not "other"', () => {
    expect(isOtherCover('vicia', listed, false)).toBe(false);
  });
  it('a name typed in "other" mode stays in that mode even when it equals a listed species (the box must not vanish while typing)', () => {
    expect(isOtherCover('vicia', listed, true)).toBe(true);
  });
  it('a typed species that is not listed is "other", even after a remount lost the choice', () => {
    expect(isOtherCover('phacelia', listed, false)).toBe(true);
    expect(isOtherCover('phacelia', [], false)).toBe(true);
  });
  it('an empty species is "other" only when the user chose it', () => {
    expect(isOtherCover('', listed, true)).toBe(true);
    expect(isOtherCover('', listed, false)).toBe(false);
  });
});

describe('free-text cover names', () => {
  it('accepts a name made of letters, digits, spaces, dots, dashes and brackets (what the server accepts)', () => {
    for (const ok of ['Phacelia', 'ab', 'Avena sativa (L.)', 'veza-avena', 'Haba ñandú', 'phacelia', 'bitter_vetch', 'a'.repeat(64)]) {
      expect(RAW_SPECIES_NAME.test(ok)).toBe(true);
    }
  });
  it('refuses a name the server would not resolve: too short, wrong characters, digit first, too long', () => {
    for (const bad of ['x', '<b>', 'veza/avena', '1abc', 'a'.repeat(65), '   ', 'a;b']) {
      expect(RAW_SPECIES_NAME.test(bad)).toBe(false);
    }
  });
});

describe('cover termination', () => {
  it('offers the roller crimper first, then mowing, and nothing else', () => {
    expect(COVER_METHODS).toEqual(['roller_crimper', 'mowing']);
  });
});
