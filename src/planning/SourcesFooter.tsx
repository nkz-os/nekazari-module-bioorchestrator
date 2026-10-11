// src/planning/SourcesFooter.tsx
import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Inline, Spinner, Stack } from '@nekazari/ui-kit';
import SourceAttribution from '../components/shared/SourceAttribution';
import { fetchAttributions } from './api';
import { useLoad } from './useLoad';

/** "Data sources": the credit lines every source licence requires, available to everyone. */
export default function SourcesFooter() {
  const { t } = useTranslation('bioorchestrator');
  const [open, setOpen] = useState(false);
  const data = useLoad((s) => fetchAttributions(s), [], open);
  return (
    <Stack gap="tight">
      <div>
        <Button size="sm" variant="ghost" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
          {t(open ? 'planning.sources.close' : 'planning.sources.open')}
        </Button>
      </div>
      {open && data.loading && <Spinner size="sm" />}
      {open && !data.loading && data.error != null && (
        <Inline gap="inline" align="center" wrap>
          <p className="text-nkz-sm text-nkz-danger">{t('planning.errorLoad')}</p>
          <Button size="sm" variant="secondary" onClick={data.retry}>{t('planning.retry')}</Button>
        </Inline>
      )}
      {open && data.data && <SourceAttribution attributions={data.data.attributions} />}
    </Stack>
  );
}
