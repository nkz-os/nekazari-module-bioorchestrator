import type { SourceAttributionItem } from '../types/attribution';

const isText = (v: unknown): v is string => typeof v === 'string' && v.trim() !== '';

/** The text entries of a per-language note; null when there is none. */
function normalizeNote(raw: unknown): Record<string, string> | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const entries = Object.entries(raw as Record<string, unknown>).filter((e): e is [string, string] => isText(e[1]));
  return entries.length ? Object.fromEntries(entries) : null;
}

/**
 * The attributions of an API answer, defensively: an older backend sends none, and a malformed
 * item must never break the page. An item needs a source id and a credit text; one item per
 * source (the first wins), in the order received.
 */
export function normalizeAttributions(raw: unknown): SourceAttributionItem[] {
  if (!Array.isArray(raw)) return [];
  const seen = new Set<string>();
  const out: SourceAttributionItem[] = [];
  for (const item of raw) {
    if (!item || typeof item !== 'object') continue;
    const { source_id, text, url, licence_id, licence_url, processing_note } = item as Record<string, unknown>;
    if (!isText(source_id) || !isText(text) || seen.has(source_id)) continue;
    seen.add(source_id);
    const entry: SourceAttributionItem = {
      source_id,
      text,
      url: typeof url === 'string' ? url : '',
      licence_id: typeof licence_id === 'string' ? licence_id : '',
      licence_url: typeof licence_url === 'string' ? licence_url : '',
    };
    const note = normalizeNote(processing_note);
    if (note) entry.processing_note = note;
    out.push(entry);
  }
  return out;
}

/** The URL when it is a plain http(s) link, else null: links come from the API and never run script. */
export function safeHref(url: string | null | undefined): string | null {
  if (!url) return null;
  try {
    const { protocol } = new URL(url);
    return protocol === 'https:' || protocol === 'http:' ? url : null;
  } catch {
    return null;
  }
}

/**
 * The attributions of an answer limited to the given sources, for a view that shows only part of
 * it (the sources of the selected crops, of the sites on screen). Sources without an attribution
 * in the answer are left out.
 */
export function attributionsForSources(
  attributions: unknown,
  sourceIds: Iterable<string | null | undefined>,
): SourceAttributionItem[] {
  const wanted = new Set<string>();
  for (const id of sourceIds) if (id) wanted.add(id);
  return normalizeAttributions(attributions).filter((a) => wanted.has(a.source_id));
}

/** The attributions of an answer limited to the sources the given recommendations draw on. */
export function attributionsForRecs(
  attributions: unknown,
  recs: readonly { evidence: { sources: readonly string[] } }[],
): SourceAttributionItem[] {
  return attributionsForSources(attributions, recs.flatMap((r) => r.evidence?.sources ?? []));
}

/**
 * The credit text split around the first occurrence of its own URL, so the URL can be a link
 * without changing a character of the text; null when the text does not contain the URL.
 */
export function splitAtUrl(text: string, url: string): { before: string; url: string; after: string } | null {
  if (!url) return null;
  const at = text.indexOf(url);
  if (at < 0) return null;
  return { before: text.slice(0, at), url, after: text.slice(at + url.length) };
}
