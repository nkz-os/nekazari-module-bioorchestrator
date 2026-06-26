import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Card, EmptyState, Skeleton, Stack } from '@nekazari/ui-kit';
import { Microscope, AlertTriangle } from 'lucide-react';
import { useParcelContext } from '../context/ParcelContext';
import { usePlanningScenario } from '../context/PlanningScenarioContext';
import { useBioApi } from '../services/api';

export default function WofostSimulation() {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel, loading: parcelLoading, error: parcelError } = useParcelContext();
  const { enabled: scenarioEnabled } = usePlanningScenario();
  const api = useBioApi();
  const [checking, setChecking] = useState(false);
  const [deployPending, setDeployPending] = useState<boolean | null>(null);

  if (parcelLoading) return <Skeleton variant="rect" height="240px" />;
  if (parcelError) {
    return (
      <EmptyState icon={<AlertTriangle className="w-8 h-8" />} title={parcelError} />
    );
  }
  if (!selectedParcel) {
    return (
      <EmptyState
        icon={<Microscope className="w-8 h-8" />}
        title={t('wofostSimulation.selectParcel')}
      />
    );
  }
  if (scenarioEnabled) {
    return (
      <EmptyState
        icon={<AlertTriangle className="w-8 h-8" />}
        title={t('scenarioMode.campaignBlocked')}
        description={t('scenarioMode.campaignBlockedDetail')}
      />
    );
  }

  const probeEndpoint = async () => {
    setChecking(true);
    try {
      await api.runWofostSimulation(selectedParcel);
      setDeployPending(false);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setDeployPending(msg.includes('404') || msg.includes('Not Found'));
    } finally {
      setChecking(false);
    }
  };

  if (deployPending === null) {
    return (
      <Stack gap="section">
        <div>
          <h2 className="text-nkz-xl font-bold text-nkz-text-primary flex items-center gap-2">
            <Microscope className="w-5 h-5 text-nkz-accent-base" />
            {t('wofostSimulation.title')}
          </h2>
          <p className="text-nkz-sm text-nkz-text-muted mt-1">{t('wofostSimulation.subtitle')}</p>
        </div>
        <Card padding="lg">
          <p className="text-nkz-sm text-nkz-text-muted mb-4">{t('wofostSimulation.intro')}</p>
          <Button variant="primary" onClick={probeEndpoint} loading={checking}>
            {t('wofostSimulation.checkReady')}
          </Button>
        </Card>
      </Stack>
    );
  }

  if (deployPending) {
    return (
      <Stack gap="section">
        <div>
          <h2 className="text-nkz-xl font-bold text-nkz-text-primary">{t('wofostSimulation.title')}</h2>
        </div>
        <EmptyState
          icon={<Microscope className="w-8 h-8" />}
          title={t('wofostSimulation.deployPendingTitle')}
          description={t('wofostSimulation.deployPendingBody')}
        />
      </Stack>
    );
  }

  return (
    <EmptyState
      icon={<Microscope className="w-8 h-8" />}
      title={t('wofostSimulation.title')}
      description={t('wofostSimulation.phase2Note')}
    />
  );
}
