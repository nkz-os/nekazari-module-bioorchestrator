// src/planning/SourcesFooter.tsx
import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Stack } from '@nekazari/ui-kit';
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
      {open && data.data && <SourceAttribution attributions={data.data.attributions} />}
    </Stack>
  );
}
