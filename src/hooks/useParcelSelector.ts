import { useState, useEffect } from "react";

export interface Parcel {
  id: string;
  name: string;
}

export function useParcelSelector() {
  const [parcels, setParcels] = useState<Parcel[]>([]);
  const [selected, setSelected] = useState("");

  useEffect(() => {
    const sdk = (window as any).__NKZ_SDK__;
    if (sdk?.getParcels) {
      sdk.getParcels().then((p: Parcel[]) => setParcels(p));
    }
  }, []);

  return { parcels, selected, setSelected };
}
