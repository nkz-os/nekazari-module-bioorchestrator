import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { SlotShell } from '@nekazari/viewer-kit';
import { Card, Skeleton, Button } from '@nekazari/ui-kit';
import { CalendarRange, AlertTriangle } from 'lucide-react';
import { useBioApi, useCropApi } from '../../services/api';
import type { CropPlan, WaterBudget, IssuedOp, PhenologyStatus } from '../../types/cropplan';
import type { AgronomicValue } from '../../types/agronomic';
import NextActionsList from './NextActionsList';
import CampaignTimeline from './CampaignTimeline';
import PlanSegments from './PlanSegments';
import AgroParams from './AgroParams';

const bioAccent = { base: '#14B8A6', soft: '#CCFBF1', strong: '#0D9488' };
const URN_PREFIX = 'urn:ngsi-ld:AgriParcel:';
const toUrn = (id: string) => (id.startsWith('urn:') ? id : `${URN_PREFIX}${id}`);
const toShort = (id: string) => id.replace(URN_PREFIX, '');

interface Props {
  parcelId?: string;
  parcelName?: string;
}

/** Section wrapper: each loads best-effort; one failure never tears down the panel. */
function Section({
  title,
  loading,
  error,
  onRetry,
  children,
}: {
  title: string;
  loading?: boolean;
  error?: string | null;
  onRetry?: () => void;
  children: React.ReactNode;
}) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <Card padding="md">
      <h4 className="text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider mb-2">{title}</h4>
      {loading ? (
        <Skeleton variant="rect" height="48px" />
      ) : error ? (
        <div className="text-nkz-error text-nkz-sm">
          <AlertTriangle size={14} className="inline mr-1" />
          {error}
          {onRetry && (
            <Button variant="ghost" size="sm" onClick={onRetry} className="ml-2">
              {t('panel.retry')}
            </Button>
          )}
        </div>
      ) : (
        children
      )}
    </Card>
  );
}

/** SP3 — Plan & Acciones: composes SP1 plan, SP2 actions and P1 reliability badges
 *  for the selected parcel. The recommended action is the hero; badges are secondary. */
const CropPlanPanel: React.FC<Props> = ({ parcelId }) => {
  const { t } = useTranslation('bioorchestrator');
  const bio = useBioApi();
  const crop = useCropApi();

  const [plan, setPlan] = useState<CropPlan | null>(null);
  const [status, setStatus] = useState<PhenologyStatus | null>(null);
  const [ops, setOps] = useState<IssuedOp[] | null>(null);
  const [water, setWater] = useState<WaterBudget | null>(null);
  const [phenoParams, setPhenoParams] = useState<Record<string, AgronomicValue> | null>(null);
  const [err, setErr] = useState<{ plan?: string; ops?: string; params?: string }>({});
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    if (!parcelId) return;
    const urn = toUrn(parcelId);
    const short = toShort(parcelId);
    setLoading(true);
    setErr({});
    await Promise.allSettled([
      bio.getCropPlan(urn).then(setPlan).catch(() => setErr((e) => ({ ...e, plan: t('cropPlan.err.plan') }))),
      crop.getPhenologyStatus(short).then(setStatus).catch(() => setStatus(null)),
      bio
        .getIssuedOperations(urn)
        .then((r: { operations: IssuedOp[] }) => setOps(r?.operations || []))
        .catch(() => setErr((e) => ({ ...e, ops: t('cropPlan.err.ops') }))),
      bio.getWaterBudget(urn).then(setWater).catch(() => setErr((e) => ({ ...e, params: t('cropPlan.err.params') }))),
    ]);
    setLoading(false);
  }, [bio, crop, parcelId, t]);

  useEffect(() => {
    void load();
  }, [load]);

  // phenology-params depends on the active segment species (best-effort, after plan loads)
  useEffect(() => {
    const species = plan?.segments?.find((s) => s.id === plan.active)?.species;
    if (!species) return;
    const params = new URLSearchParams({ species });
    bio
      .getPhenologyParams(params)
      .then((p: { agronomic?: Record<string, AgronomicValue> }) => setPhenoParams(p?.agronomic ?? null))
      .catch(() => {});
  }, [plan, bio]);

  const handleCreatePlan = useCallback(() => {
    // Navigate to the module page with rotation planner tool
    window.location.href = '/modules/bioorchestrator';
  }, []);

  if (!parcelId) return null;

  return (
    <SlotShell moduleId="bioorchestrator" title={t('cropPlan.title')} icon={<CalendarRange className="w-4 h-4" />} accent={bioAccent}>
      <div className="flex flex-col gap-3">
        {/* HERO: recommended actions first (P1 north-star) */}
        <Section title={t('cropPlan.actions.title')} loading={loading} error={err.ops} onRetry={load}>
          <NextActionsList ops={ops ?? []} />
        </Section>
        <Section title={t('cropPlan.timeline.title')} loading={loading}>
          {plan && <CampaignTimeline plan={plan} status={status ?? {}} />}
        </Section>
        <Section title={t('cropPlan.segments.title')} loading={loading} error={err.plan} onRetry={load}>
          {plan && <PlanSegments plan={plan} status={status ?? {}} onCreatePlan={handleCreatePlan} />}
        </Section>
        <Section title={t('cropPlan.params.title')} loading={loading} error={err.params} onRetry={load}>
          <AgroParams water={water} phenoParams={phenoParams} />
        </Section>
      </div>
    </SlotShell>
  );
};

export default CropPlanPanel;
