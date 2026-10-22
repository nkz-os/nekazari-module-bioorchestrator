import React, { createContext, useContext, useState, useEffect, useMemo, useCallback } from 'react';
import { useViewer } from '@nekazari/sdk';

export interface Parcel {
  id: string;
  name: string;
}

interface ParcelContextValue {
  parcels: Parcel[];
  selectedParcel: string;
  setSelectedParcel: (id: string) => void;
  loading: boolean;
  error: string | null;
}

const ParcelContext = createContext<ParcelContextValue>({
  parcels: [],
  selectedParcel: '',
  setSelectedParcel: () => {},
  loading: false,
  error: null,
});

export function ParcelProvider({ children }: { children: React.ReactNode }) {
  const [parcels, setParcels] = useState<Parcel[]>([]);
  const [selectedParcel, setSelectedParcel] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Watch viewer entity selection for auto-sync
  // Safe: module always runs inside viewer host where useViewer context is provided
  let viewerEntityId: string | undefined;
  let viewerEntityType: string | undefined;
  try {
    const viewerCtx = useViewer();
    viewerEntityId = (viewerCtx as any)?.selectedEntityId;
    viewerEntityType = (viewerCtx as any)?.selectedEntityType;
  } catch {
    // Viewer context not available (dev/test outside viewer host)
  }

  // Fetch parcels from Orion-LD
  useEffect(() => {
    import('../services/api').then(({ fetchParcels }) => {
      fetchParcels()
        .then((p: Parcel[]) => {
          setParcels(p);
          setError(null);
        })
        .catch((err: unknown) => setError(String(err)))
        .finally(() => setLoading(false));
    }).catch(() => {
      setLoading(false);
    });
  }, []);

  // Sync: when viewer selects an AgriParcel, auto-select in our dropdown
  useEffect(() => {
    if (!viewerEntityId || !viewerEntityType) return;
    if (viewerEntityType !== 'AgriParcel') return;
    if (!parcels.some(p => p.id === viewerEntityId)) return;
    setSelectedParcel(viewerEntityId);
  }, [viewerEntityId, viewerEntityType, parcels]);

  // Stable parcel setter
  const handleSetSelectedParcel = useCallback((id: string) => {
    setSelectedParcel(id);
  }, []);

  const value = useMemo<ParcelContextValue>(
    () => ({ parcels, selectedParcel, setSelectedParcel: handleSetSelectedParcel, loading, error }),
    [parcels, selectedParcel, handleSetSelectedParcel, loading, error],
  );
  return <ParcelContext.Provider value={value}>{children}</ParcelContext.Provider>;
}

export function useParcelContext() {
  return useContext(ParcelContext);
}
