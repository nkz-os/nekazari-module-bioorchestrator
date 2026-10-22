// src/planning/AdminTools.tsx
import React, { Suspense, lazy } from 'react';
import { Spinner, Stack } from '@nekazari/ui-kit';

const SourcesDashboard = lazy(() => import('../components/SourcesDashboard'));

/** Platform administration only: ingestion sources. */
export default function AdminTools() {
  return (
    <Suspense fallback={<Spinner size="lg" />}>
      <Stack gap="section">
        <SourcesDashboard />
      </Stack>
    </Suspense>
  );
}
