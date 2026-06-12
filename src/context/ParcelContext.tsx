import React, { createContext, useContext, useState, useEffect, useMemo } from 'react';

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

  useEffect(() => {
    const sdk = (window as any).__NKZ_SDK__;
    if (sdk?.getParcels) {
      sdk.getParcels()
        .then((p: Parcel[]) => setParcels(p))
        .catch((err: unknown) => setError(String(err)))
        .finally(() => setLoading(false));
    } else {
      setLoading(false);
    }
  }, []);

  const value = useMemo<ParcelContextValue>(
    () => ({ parcels, selectedParcel, setSelectedParcel, loading, error }),
    [parcels, selectedParcel, loading, error],
  );
  return <ParcelContext.Provider value={value}>{children}</ParcelContext.Provider>;
}

export function useParcelContext() {
  return useContext(ParcelContext);
}
