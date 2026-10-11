// src/planning/LazyBoundary.tsx
import React, { Component, type ErrorInfo, type ReactNode } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Inline } from '@nekazari/ui-kit';

/** Fallback as a function component: a class cannot call the translation hook. */
function LoadFailed({ onRetry }: { onRetry: () => void }) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <Inline gap="inline" align="center">
      <p className="text-nkz-sm text-nkz-danger">{t('planning.errorLoad')}</p>
      <Button size="sm" variant="secondary" onClick={onRetry}>{t('planning.retry')}</Button>
    </Inline>
  );
}

interface State { failed: boolean; attempt: number }

/** Contains a failure in a lazily loaded part so it never takes the whole page down; retry remounts the children. */
export default class LazyBoundary extends Component<{ children: ReactNode }, State> {
  state: State = { failed: false, attempt: 0 };

  static getDerivedStateFromError(): Partial<State> {
    return { failed: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('planning: lazy part failed', { where: 'planning', error, componentStack: info.componentStack });
  }

  private reset = () => this.setState((s) => ({ failed: false, attempt: s.attempt + 1 }));

  render() {
    if (this.state.failed) return <LoadFailed onRetry={this.reset} />;
    return <React.Fragment key={this.state.attempt}>{this.props.children}</React.Fragment>;
  }
}
