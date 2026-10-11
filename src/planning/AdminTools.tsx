// src/planning/AdminTools.tsx
import React, { Suspense, lazy } from 'react';
import { Spinner, Stack } from '@nekazari/ui-kit';

const SourcesDashboard = lazy(() => import('../components/SourcesDashboard'));
const PipelineRunner = lazy(() => import('../components/PipelineRunner'));

/** Platform administration only: ingestion sources and the pipeline runner. */
export default function AdminTools() {
  return (
    <Suspense fallback={<Spinner size="lg" />}>
      <Stack gap="section">
        <SourcesDashboard />
        <PipelineRunner />
      </Stack>
    </Suspense>
  );
}
