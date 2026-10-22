import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Stack } from '@nekazari/ui-kit';
import type { SourceAttributionItem } from '../../types/attribution';
import { normalizeAttributions, safeHref, splitAtUrl } from '../../utils/attribution';

interface SourceAttributionProps {
  /** `attributions` of an API answer; nothing is rendered when it lists no source. */
  attributions?: SourceAttributionItem[] | null;
}

/**
 * The credit lines the licences of the data sources require, verbatim, with the note that the
 * estimates and averages shown are Nekazari calculations and not data published by the sources.
 */
export default function SourceAttribution({ attributions }: SourceAttributionProps) {
  const { t } = useTranslation('bioorchestrator');
  const items = normalizeAttributions(attributions);
  if (items.length === 0) return null;
  return (
    <div className="pt-4 mt-4 border-t border-nkz-border" role="note" aria-label={t('sourceAttribution.title')}>
      <Stack gap="tight">
        <p className="text-nkz-xs font-medium text-nkz-text-muted">{t('sourceAttribution.title')}</p>
        {items.map((item) => {
          const source = safeHref(item.url);
          const licence = safeHref(item.licence_url);
          // A credit line that already carries its URL links that URL in place (text unchanged);
          // otherwise the source gets its own link.
          const inText = source ? splitAtUrl(item.text, item.url) : null;
          return (
            <p key={item.source_id} className="text-nkz-xs text-nkz-text-muted">
              {inText ? (
                <>
                  {inText.before}
                  <a href={source ?? undefined} target="_blank" rel="noopener noreferrer" className="underline">
                    {inText.url}
                  </a>
                  {inText.after}
                </>
              ) : item.text}
              {source && !inText && (
                <>
                  {' '}
                  <a href={source} target="_blank" rel="noopener noreferrer" className="underline">
                    {t('sourceAttribution.source')}
                  </a>
                </>
              )}
              {licence && (
                <>
                  {' '}
                  <a href={licence} target="_blank" rel="noopener noreferrer" className="underline">
                    {t('sourceAttribution.licence')}
                  </a>
                </>
              )}
            </p>
          );
        })}
        <p className="text-nkz-xs text-nkz-text-muted">{t('sourceAttribution.calculations')}</p>
      </Stack>
    </div>
  );
}
