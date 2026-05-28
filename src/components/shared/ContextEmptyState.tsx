import React from 'react';
import { Button, Stack } from '@nekazari/ui-kit';
import { AlertTriangle, Info } from 'lucide-react';

interface Props {
  message: string;
  actionLabel?: string;
  onAction?: () => void;
  variant?: 'info' | 'warning';
}

export default function ContextEmptyState({
  message,
  actionLabel,
  onAction,
  variant = 'info',
}: Props) {
  const Icon = variant === 'warning' ? AlertTriangle : Info;
  return (
    <Stack gap="stack" className="items-center py-8 text-center">
      <Icon size={32} className={variant === 'warning' ? 'text-nkz-warning' : 'text-nkz-text-muted'} />
      <p className="text-nkz-text-muted max-w-md">{message}</p>
      {actionLabel && onAction && (
        <Button variant="secondary" onClick={onAction} size="sm">
          {actionLabel}
        </Button>
      )}
    </Stack>
  );
}
