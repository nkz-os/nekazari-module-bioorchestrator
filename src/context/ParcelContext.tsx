import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';

export interface Parcel {
  id: string;
  name: string;
}

interface ParcelContextValue {
  parcels: Parcel[];
  selectedParcel: string;
  setSelectedParcel: (id: string) => void;
}

const ParcelContext = createContext<ParcelContextValue>({
  parcels: [],
  selectedParcel: '',
  setSelectedParcel: () => {},
});

export function ParcelProvider({ children }: { children: React.ReactNode }) {
  const [parcels, setParcels] = useState<Parcel[]>([]);
  const [selectedParcel, setSelectedParcel] = useState('');

  useEffect(() => {
    const sdk = (window as any).__NKZ_SDK__;
    if (sdk?.getParcels) {
      sdk.getParcels().then((p: Parcel[]) => setParcels(p));
    }
  }, []);

  const value: ParcelContextValue = { parcels, selectedParcel, setSelectedParcel };
  return <ParcelContext.Provider value={value}>{children}</ParcelContext.Provider>;
}

export function useParcelContext() {
  return useContext(ParcelContext);
}
