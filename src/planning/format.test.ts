import { describe, expect, it } from 'vitest';
import { cropName, formatHa, parcelUrn, safeLang, shortId } from './format';

describe('format', () => {
  it('picks the language, then en, then es, then the fallback', () => {
    expect(cropName({ es: 'trigo', en: 'wheat' }, 'TRZAX', 'es-ES')).toBe('trigo');
    expect(cropName({ en: 'wheat' }, 'TRZAX', 'eu')).toBe('wheat');
    expect(cropName({}, 'TRZAX', 'eu')).toBe('TRZAX');
    expect(cropName(undefined, null, 'eu')).toBe('—');
  });
  it('builds parcel URNs once', () => {
    expect(parcelUrn('p1')).toBe('urn:ngsi-ld:AgriParcel:p1');
    expect(parcelUrn('urn:ngsi-ld:AgriParcel:t:p1')).toBe('urn:ngsi-ld:AgriParcel:t:p1');
    expect(shortId('urn:ngsi-ld:AgriParcel:t:p1')).toBe('p1');
  });
  it('falls back to en for an empty or malformed language tag', () => {
    expect(safeLang('')).toBe('en');
    expect(safeLang('en_US')).toBe('en');
    expect(safeLang('es-ES')).toBe('es-ES');
  });
  it('number formatting never throws on a malformed language tag', () => {
    expect(formatHa(1.5, 'en_US')).toBe('1.5');
    expect(formatHa(1.5, '')).toBe('1.5');
  });
});
