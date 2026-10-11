// src/planning/LazyBoundary.tsx
import React, { Component, type ErrorInfo, type ReactNode } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Inline } from '@nekazari/ui-kit';
import { isChunkLoadError } from './errors';

/**
 * Fallback as a function component: a class cannot call the translation hook. A chunk that failed
 * to load stays failed (React.lazy caches the rejected import), so the only way out is a reload.
 */
function LoadFailed({ onRetry, chunk }: { onRetry: () => void; chunk: boolean }) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <Inline gap="inline" align="center">
      <p className="text-nkz-sm text-nkz-danger">{t('planning.errorLoad')}</p>
      {chunk ? (
        <Button size="sm" variant="secondary" onClick={() => window.location.reload()}>{t('planning.reload')}</Button>
      ) : (
        <Button size="sm" variant="secondary" onClick={onRetry}>{t('planning.retry')}</Button>
      )}
    </Inline>
  );
}

interface State { failed: boolean; attempt: number; chunk: boolean }

/**
 * Contains a failure in a lazily loaded part so it never takes the whole page down. Retry remounts
 * the children, except after a chunk load failure, where it reloads the page.
 */
export default class LazyBoundary extends Component<{ children: ReactNode }, State> {
  state: State = { failed: false, attempt: 0, chunk: false };

  static getDerivedStateFromError(error: unknown): Partial<State> {
    return { failed: true, chunk: isChunkLoadError(error) };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('planning: lazy part failed', { where: 'planning', error, componentStack: info.componentStack });
  }

  private reset = () => this.setState((s) => ({ failed: false, chunk: false, attempt: s.attempt + 1 }));

  render() {
    if (this.state.failed) return <LoadFailed onRetry={this.reset} chunk={this.state.chunk} />;
    return <React.Fragment key={this.state.attempt}>{this.props.children}</React.Fragment>;
  }
}
