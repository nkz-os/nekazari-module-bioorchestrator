/**
 * Mandatory source attribution returned by the API (`attributions` of the recommend, evidence,
 * variety-trials, yield-potential and compare-crops responses, and the public
 * `/api/graph/agriculture/sources/attributions` listing). Mirror of
 * backend/app/common/source_registry.py::get_attributions.
 */
export interface SourceAttributionItem {
  source_id: string;
  /** Credit line the source's licence prescribes; shown verbatim. */
  text: string;
  /** Page of the source the credit points to. */
  url: string;
  licence_id: string;
  licence_url: string;
  /** Fidelity line (`es`/`en`: figures shown are Nekazari calculations); the UI shows its own i18n copy of it. */
  processing_note?: Record<string, string>;
}
