import React from 'react';
import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react';
import { useAuth } from '@nekazari/sdk';
import { EXPERT_ROLES, EXPERT_STORAGE_KEY, resolveExpertMode } from './expertMode';

interface ExpertModeValue {
  expert: boolean;
  setExpert: (value: boolean) => void;
}

const ExpertModeContext = createContext<ExpertModeValue>({ expert: false, setExpert: () => {} });

function readUrlParam(): string | null {
  try {
    return new URLSearchParams(window.location.search).get('expert');
  } catch {
    return null;
  }
}

function readStored(): string | null {
  try {
    return window.localStorage.getItem(EXPERT_STORAGE_KEY);
  } catch {
    return null;
  }
}

export function ExpertModeProvider({ children }: { children: ReactNode }) {
  const { hasAnyRole } = useAuth();
  // Explicit choice made in this session; overrides URL/stored/role defaults.
  const [explicit, setExplicit] = useState<boolean | null>(null);

  // SDK exposes no roles array: derive the expert-eligible flag via hasAnyRole.
  // Recomputed every render so late-arriving roles are picked up.
  const roleDefault = hasAnyRole([...EXPERT_ROLES]);

  const expert = useMemo(() => {
    if (explicit !== null) return explicit;
    return resolveExpertMode({
      urlParam: readUrlParam(),
      stored: readStored(),
      roles: roleDefault ? [...EXPERT_ROLES] : [],
    });
  }, [explicit, roleDefault]);

  const setExpert = useCallback((value: boolean) => {
    setExplicit(value);
    try {
      window.localStorage.setItem(EXPERT_STORAGE_KEY, value ? '1' : '0');
    } catch {
      /* storage unavailable: toggle still applies for this session */
    }
  }, []);

  const value = useMemo(() => ({ expert, setExpert }), [expert, setExpert]);
  return <ExpertModeContext.Provider value={value}>{children}</ExpertModeContext.Provider>;
}

export function useExpertMode(): ExpertModeValue {
  return useContext(ExpertModeContext);
}
