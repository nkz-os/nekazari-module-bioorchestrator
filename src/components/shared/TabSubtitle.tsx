import React from 'react';

interface Props {
  children: React.ReactNode;
}

export default function TabSubtitle({ children }: Props) {
  return (
    <p className="text-nkz-text-muted text-sm mb-3 leading-relaxed max-w-3xl">
      {children}
    </p>
  );
}
