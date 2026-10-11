import { describe, expect, it } from 'vitest';
import { cropName, parcelUrn, shortId } from './format';

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
});
