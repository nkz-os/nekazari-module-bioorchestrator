import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Card, EmptyState, Inline, Input, Skeleton, Stack, Tooltip } from '@nekazari/ui-kit';
import { fetchRecommendConditions, fetchRecommendParcel, type QueryParams } from '../../services/recommendApi';
import type { ParcelEnvironment, Recommendation, RecommendResponse } from '../../types/recommend';
import ContributeWizard from '../../components/ContributeWizard';
import { useExpertMode } from './expertModeContext';
import { partitionRecommendations, resolvePageState } from './viewModel';
import {
  DEFAULT_FILTERS, FROST_DEBOUNCE_MS, MANAGEMENTS, MAX_COMPARE, PURPOSES, SEASONS, conditionsQuery,
  evidenceConditions, irrigationOptions, pickIrrigation, frostMarginStatus, parcelQuery, rangeScaleMax, toggleCompare, type Filters,
} from './pageModel';
import EnvironmentChips from './EnvironmentChips';
import KoppenPicker from './KoppenPicker';
import RecommendationCard, { useCropName } from './RecommendationCard';
import MoreList from './MoreList';
import EvidenceDialog from './EvidenceDialog';
import CompareTray from './CompareTray';
import VarietyPanel from './VarietyPanel';

interface WhatToSowPageProps {
  parcelId: string | null;
  onSelectTool?: (toolId: string) => void;
  onAssigned?: () => void;
}

interface EvidenceTarget {
  rec: Recommendation;
  conditions: QueryParams;
}

function FilterGroup<T extends string>({ name, values, value, onChange, hints }: {
  name: string; values: readonly T[]; value: T; onChange: (v: T) => void;
  /** Tooltip text per option, for options whose label alone is not enough. */
  hints?: Partial<Record<T, string>>;
}) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <Inline gap="tight" wrap align="center" role="group" aria-label={t(`whatToSow.filter.${name}.label`)}>
      <span className="text-nkz-sm text-nkz-text-muted">{t(`whatToSow.filter.${name}.label`)}</span>
      {values.map((v) => {
        const button = (
          <Button
            size="sm"
            variant={v === value ? 'primary' : 'secondary'}
            aria-pressed={v === value}
            onClick={() => onChange(v)}
          >
            {t(`whatToSow.filter.${name}.${v}`)}
          </Button>
        );
        const hint = hints?.[v];
        return hint ? <Tooltip key={v} content={hint}>{button}</Tooltip> : <React.Fragment key={v}>{button}</React.Fragment>;
      })}
    </Inline>
  );
}

/** Keeps the raw text locally; commits it after a pause, and only when empty or within 0–15. */
function FrostMarginInput({ committed, onCommit }: { committed: string; onCommit: (v: string) => void }) {
  const { t } = useTranslation('bioorchestrator');
  const [text, setText] = useState(committed);
  const status = frostMarginStatus(text);
  const commitRef = useRef(onCommit);
  commitRef.current = onCommit;

  useEffect(() => {
    const value = status === 'empty' ? '' : text;
    if (status === 'invalid' || value === committed) return;
    const timer = setTimeout(() => commitRef.current(value), FROST_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [text, status, committed]);

  return (
    <Inline gap="tight" align="center" wrap>
      <label htmlFor="wts-frost-margin" className="text-nkz-sm text-nkz-text-muted">
        {t('whatToSow.filter.frostMargin')}
      </label>
      <div className="w-32">
        <Input
          id="wts-frost-margin"
          type="number"
          size="sm"
          min={0}
          max={15}
          step={0.5}
          placeholder="5"
          value={text}
          error={status === 'invalid'}
          aria-invalid={status === 'invalid'}
          aria-describedby={status === 'invalid' ? 'wts-frost-margin-hint' : undefined}
          onChange={(e) => setText(e.target.value)}
        />
      </div>
      {status === 'invalid' && (
        <span id="wts-frost-margin-hint" className="text-nkz-sm text-nkz-danger">
          {t('whatToSow.filter.frostMarginInvalid')}
        </span>
      )}
    </Inline>
  );
}

function FilterChips({ filters, expert, hasParcel, onChange }: {
  filters: Filters; expert: boolean; hasParcel: boolean; onChange: (patch: Partial<Filters>) => void;
}) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <Stack gap="tight">
      <FilterGroup name="purpose" values={PURPOSES} value={filters.purpose}
        hints={{ main: t('whatToSow.filter.purpose.mainHint') }}
        onChange={(purpose) => onChange({ purpose })} />
      <FilterGroup name="season" values={SEASONS} value={filters.season} onChange={(season) => onChange({ season })} />
      <FilterGroup name="management" values={MANAGEMENTS} value={filters.management}
        onChange={(management) => onChange({ management })} />
      <FilterGroup name="irrigation" values={irrigationOptions(hasParcel)} value={filters.irrigation}
        onChange={(picked) => onChange({ irrigation: pickIrrigation(filters.irrigation, picked, hasParcel) })} />
      {expert && (
        <FrostMarginInput committed={filters.frostMargin} onCommit={(frostMargin) => onChange({ frostMargin })} />
      )}
    </Stack>
  );
}

function ReportValueDialog({ rec, onClose }: { rec: Recommendation; onClose: () => void }) {
  const name = useCropName(rec);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={onClose}>
      <div role="dialog" aria-modal="true" aria-label={name}
        className="w-full max-w-2xl max-h-[80vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
        <Card padding="md">
          {/* ASSUMPTION: ContributeWizard is the route for reporting a wrong value — confirm in review. */}
          <ContributeWizard cropId={rec.crop.eppo} cropName={name} onClose={onClose} onSuccess={onClose} />
        </Card>
      </div>
    </div>
  );
}

function EvidenceFor({ target, onClose }: { target: EvidenceTarget; onClose: () => void }) {
  const name = useCropName(target.rec);
  return (
    <EvidenceDialog
      cropName={name}
      eppo={target.rec.crop.eppo}
      conditions={target.conditions}
      similarity={target.rec.trust.similarity}
      onClose={onClose}
    />
  );
}

export default function WhatToSowPage({ parcelId, onSelectTool, onAssigned }: WhatToSowPageProps) {
  const { t } = useTranslation('bioorchestrator');
  const { expert } = useExpertMode();
  const [baseFilters, setBaseFilters] = useState<Omit<Filters, 'climateClass'>>(DEFAULT_FILTERS);
  // Climate override belongs to the parcel it was picked for (null = explore mode).
  const [climate, setClimate] = useState<{ parcel: string | null; code: string }>({ parcel: null, code: '' });
  // The response is kept with the parcel it answers, so a parcel switch never flashes the old one.
  const [stored, setStored] = useState<{ parcel: string | null; data: RecommendResponse } | null>(null);
  const response = stored && stored.parcel === parcelId ? stored.data : null;
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [reload, setReload] = useState(0);
  const [compareIds, setCompareIds] = useState<string[]>([]);
  // Recommendation whose variety panel is open.
  const [varietyFor, setVarietyFor] = useState<string | null>(null);
  const [evidenceFor, setEvidenceFor] = useState<EvidenceTarget | null>(null);
  const [reportFor, setReportFor] = useState<Recommendation | null>(null);

  const filters: Filters = {
    ...baseFilters,
    climateClass: climate.parcel === parcelId ? climate.code : '',
  };
  const query = parcelId ? parcelQuery(filters, expert) : conditionsQuery(filters, expert);
  const queryKey = query ? JSON.stringify(query) : null;

  useEffect(() => {
    setCompareIds([]);
    setVarietyFor(null);
    setEvidenceFor(null);
    setReportFor(null);
  }, [parcelId]);

  useEffect(() => {
    if (!queryKey) {
      setLoading(false);
      return;
    }
    const params = JSON.parse(queryKey) as QueryParams;
    const ctrl = new AbortController();
    setLoading(true);
    setError(null);
    const request = parcelId
      ? fetchRecommendParcel(parcelId, params, ctrl.signal)
      : fetchRecommendConditions(params, ctrl.signal);
    request
      .then((res) => {
        setStored({ parcel: parcelId, data: res });
        if (res.status === 'ok') {
          const ids = new Set(res.recommendations.map((r) => r.recommendation_id));
          setCompareIds((sel) => sel.filter((id) => ids.has(id)));
        }
      })
      .catch((e: unknown) => {
        if ((e as { name?: string })?.name !== 'AbortError') setError(e);
      })
      .finally(() => {
        if (!ctrl.signal.aborted) setLoading(false);
      });
    return () => ctrl.abort();
  }, [parcelId, queryKey, reload]);

  const recs = useMemo(() => (response?.status === 'ok' ? response.recommendations : []), [response]);
  const environment: ParcelEnvironment | undefined = response?.parcel_environment;
  const { top, more } = useMemo(() => partitionRecommendations(recs), [recs]);
  const scaleMax = useMemo(() => rangeScaleMax(recs), [recs]);
  const usedClimate =
    (response?.status === 'ok' && typeof response.conditions.climate_class === 'string'
      ? response.conditions.climate_class : null) ?? environment?.climate_class ?? null;
  const usedIrrigation =
    (response?.status === 'ok' && typeof response.conditions.irrigation_regime === 'string'
      ? response.conditions.irrigation_regime : null) ?? environment?.irrigation?.inferred ?? null;

  const pickClimate = useCallback((code: string) => {
    if (code) setClimate({ parcel: parcelId, code });
  }, [parcelId]);
  const viewForage = useCallback(() => setBaseFilters((f) => ({ ...f, purpose: 'forage' })), []);
  const onToggleCompare = useCallback((id: string) => setCompareIds((sel) => toggleCompare(sel, id)), []);
  // "Elegir variedad": toggles the variety panel under that card.
  const handleChooseVariety = useCallback(
    (id: string) => setVarietyFor((cur) => (cur === id ? null : id)), []);
  const openEvidence = (rec: Recommendation) => {
    const echo = response?.status === 'ok' ? response.conditions : null;
    setEvidenceFor({ rec, conditions: evidenceConditions(echo, environment, rec.trust.similarity) });
  };

  const compareFull = compareIds.length >= MAX_COMPARE;
  const compareSelection = recs.filter((r) => compareIds.includes(r.recommendation_id));
  const state = queryKey ? resolvePageState(loading, error, response) : null;

  return (
    <Stack gap="section">
      {!parcelId && (
        <Card padding="md">
          <Stack gap="stack">
            <p className="text-nkz-sm text-nkz-text-muted">{t('app.selectParcelPrompt')}</p>
            <p className="text-nkz-sm font-medium text-nkz-text-primary">{t('whatToSow.explore.title')}</p>
            <KoppenPicker value={filters.climateClass || null} onChange={pickClimate} />
          </Stack>
        </Card>
      )}

      {parcelId && environment && state !== 'needs_climate' && (
        <EnvironmentChips environment={environment} climateClass={usedClimate} irrigationRegime={usedIrrigation} onClimateChange={pickClimate} />
      )}

      {queryKey && (
        <FilterChips filters={filters} expert={expert} hasParcel={parcelId != null}
          onChange={(patch) => setBaseFilters((f) => ({ ...f, ...patch }))} />
      )}

      {state === 'loading' && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4" aria-busy="true">
          {[0, 1, 2].map((i) => <Skeleton key={i} variant="rect" height={220} />)}
        </div>
      )}

      {state === 'error' && (
        <Card padding="md">
          <Inline gap="inline" align="center" wrap>
            <p className="text-nkz-sm text-nkz-danger">{t('whatToSow.error.title')}</p>
            <Button variant="secondary" size="sm" onClick={() => setReload((n) => n + 1)}>
              {t('whatToSow.error.retry')}
            </Button>
          </Inline>
        </Card>
      )}

      {state === 'needs_climate' && (
        <Card padding="lg" className="border-nkz-warning">
          <Stack gap="stack">
            <h3 className="text-nkz-base font-semibold text-nkz-text-primary">{t('whatToSow.needsClimate.title')}</h3>
            <p className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.needsClimate.description')}</p>
            <KoppenPicker value={null} onChange={pickClimate} />
          </Stack>
        </Card>
      )}

      {state === 'empty' && (
        <EmptyState title={t('whatToSow.empty.title')} description={t('whatToSow.empty.hint')} />
      )}

      {state === 'ok' && (
        <Stack gap="stack">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {top.map((rec) => (
              <Stack key={rec.recommendation_id} gap="tight">
                <RecommendationCard
                  rec={rec}
                  scaleMax={scaleMax}
                  expert={expert}
                  compared={compareIds.includes(rec.recommendation_id)}
                  compareDisabled={compareFull}
                  onToggleCompare={() => onToggleCompare(rec.recommendation_id)}
                  onChooseVariety={() => handleChooseVariety(rec.recommendation_id)}
                  onOpenEvidence={() => openEvidence(rec)}
                  onReportValue={() => setReportFor(rec)}
                  onViewForage={viewForage}
                />
                {varietyFor === rec.recommendation_id && (
                  <VarietyPanel rec={rec} parcelId={parcelId} onSelectTool={onSelectTool} onAssigned={onAssigned} />
                )}
              </Stack>
            ))}
          </div>
          <MoreList
            recs={more}
            scaleMax={scaleMax}
            isCompared={(id) => compareIds.includes(id)}
            compareFull={compareFull}
            onToggleCompare={onToggleCompare}
            onViewForage={viewForage}
          />
        </Stack>
      )}

      <CompareTray selection={compareSelection} parcelId={parcelId} onClear={() => setCompareIds([])} />

      {evidenceFor && <EvidenceFor target={evidenceFor} onClose={() => setEvidenceFor(null)} />}
      {reportFor && <ReportValueDialog rec={reportFor} onClose={() => setReportFor(null)} />}
    </Stack>
  );
}
