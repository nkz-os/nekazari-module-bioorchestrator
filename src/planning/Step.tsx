// src/planning/Step.tsx
import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Card, Inline, Stack } from '@nekazari/ui-kit';
import { loadErrorKey } from './errors';
import LoadingLine from './LoadingLine';

interface Props {
  title: string; loading?: boolean; error?: unknown; onRetry?: () => void; children?: React.ReactNode;
  /** What is being loaded, said while it loads (a generic text otherwise). */
  loadingText?: string;
}

/** One step of the parcel detail: its own loading and error, so one failure never blanks the others. */
export default function Step({ title, loading = false, error = null, onRetry, children, loadingText }: Props) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <Card padding="md">
      <Stack gap="stack">
        <h3 className="text-nkz-base font-semibold text-nkz-text-primary">{title}</h3>
        {loading && <LoadingLine text={loadingText ?? t('planning.loading.generic')} />}
        {!loading && error != null && (
          <Inline gap="inline" align="center" wrap>
            <p className="text-nkz-sm text-nkz-danger">{t(loadErrorKey(error))}</p>
            {onRetry && <Button size="sm" variant="secondary" onClick={onRetry}>{t('planning.retry')}</Button>}
          </Inline>
        )}
        {!loading && error == null && children}
      </Stack>
    </Card>
  );
}
