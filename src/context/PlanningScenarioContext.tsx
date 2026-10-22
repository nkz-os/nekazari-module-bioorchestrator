import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react';
import { useParcelContext } from './ParcelContext';

export interface ScenarioCrop {
  eppo: string;
  scientificName?: string;
}

interface PlanningScenarioValue {
  enabled: boolean;
  crop: ScenarioCrop | null;
  setEnabled: (on: boolean) => void;
  setCrop: (crop: ScenarioCrop | null) => void;
}

const PlanningScenarioContext = createContext<PlanningScenarioValue>({
  enabled: false,
  crop: null,
  setEnabled: () => {},
  setCrop: () => {},
});

export function PlanningScenarioProvider({ children }: { children: React.ReactNode }) {
  const { selectedParcel } = useParcelContext();
  const [enabled, setEnabledState] = useState(false);
  const [crop, setCropState] = useState<ScenarioCrop | null>(null);

  useEffect(() => {
    setEnabledState(false);
    setCropState(null);
  }, [selectedParcel]);

  const setEnabled = useCallback((on: boolean) => {
    setEnabledState(on);
    if (!on) setCropState(null);
  }, []);

  const setCrop = useCallback((next: ScenarioCrop | null) => {
    setCropState(next);
    if (next) setEnabledState(true);
  }, []);

  const value = useMemo(
    () => ({ enabled, crop, setEnabled, setCrop }),
    [enabled, crop, setEnabled, setCrop],
  );

  return (
    <PlanningScenarioContext.Provider value={value}>
      {children}
    </PlanningScenarioContext.Provider>
  );
}

export function usePlanningScenario() {
  return useContext(PlanningScenarioContext);
}
