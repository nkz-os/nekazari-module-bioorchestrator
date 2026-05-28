import React from 'react';
import { Skeleton, Stack } from '@nekazari/ui-kit';

interface Props {
  columns: number;
  rows?: number;
}

export default function DataTableSkeleton({ columns, rows = 6 }: Props) {
  return (
    <Stack gap="tight">
      {/* Header row */}
      <div className="flex gap-2">
        {Array.from({ length: columns }).map((_, i) => (
          <Skeleton key={i} width={i === 0 ? '200px' : '100px'} height="20px" />
        ))}
      </div>
      {/* Data rows */}
      {Array.from({ length: rows }).map((_, row) => (
        <div key={row} className="flex gap-2">
          {Array.from({ length: columns }).map((_, col) => (
            <Skeleton key={col} width={col === 0 ? '180px' : '80px'} height="16px" />
          ))}
        </div>
      ))}
    </Stack>
  );
}
