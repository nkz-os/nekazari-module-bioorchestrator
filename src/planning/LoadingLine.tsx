// src/planning/LoadingLine.tsx
import React from 'react';
import { Inline, Spinner } from '@nekazari/ui-kit';

/** A spinner with words: a bare grey placeholder read as an empty field or an error. */
export default function LoadingLine({ text }: { text: string }) {
  return (
    <div role="status" aria-busy="true">
      <Inline gap="inline" align="center">
        <Spinner size="sm" />
        <p className="text-nkz-sm text-nkz-text-muted">{text}</p>
      </Inline>
    </div>
  );
}
