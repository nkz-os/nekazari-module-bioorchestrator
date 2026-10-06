import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Card, Inline, Stack } from '@nekazari/ui-kit';
import type { Recommendation } from '../../types/recommend';
import type { SourceAttributionItem } from '../../types/attribution';
import { attributionsForRecs } from '../../utils/attribution';
import SourceAttribution from '../../components/shared/SourceAttribution';
import AssignVarietyModal, { type VarietyInfo } from '../../components/AssignVarietyModal';
import { RangeBarValues, useCropName } from './RecommendationCard';
import { showDiseaseSummary, toVarietyInfo, topVarieties, varietyScaleMax } from './varietyModel';
import { formatYield, kgUnitKey, recYieldUnit, unitKey } from './viewModel';

interface VarietyPanelProps {
  rec: Recommendation;
  parcelId: string | null;
  /** `attributions` of the recommend answer `rec` comes from. */
  attributions?: SourceAttributionItem[];
  onSelectTool?: (toolId: string) => void;
  onAssigned?: () => void;
}

export default function VarietyPanel({ rec, parcelId, attributions, onSelectTool, onAssigned }: VarietyPanelProps) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const crop = useCropName(rec);
  const varieties = topVarieties(rec);
  const scaleMax = varietyScaleMax(varieties);
  const [assigning, setAssigning] = useState<VarietyInfo | null>(null);
  const [assigned, setAssigned] = useState<string | null>(null);
  const noData = t('whatToSow.noData');
  const unit = recYieldUnit(rec);
  const unitLabel = t(unitKey(unit));

  return (
    <Card padding="md">
      <Stack gap="stack">
        <h4 className="text-nkz-base font-semibold text-nkz-text-primary">
          {t('whatToSow.variety.title', { crop })}
        </h4>

        {varieties.length === 0 && (
          <p className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.variety.none')}</p>
        )}

        {varieties.map((v) => {
          const info = toVarietyInfo(v, rec.crop);
          const disabled = !parcelId || !info;
          const expected = formatYield(v.expected_kg_ha, unit, i18n.language);
          return (
            <Stack key={v.variety_uri ?? v.variety} gap="tight">
              <Inline gap="inline" align="center" wrap>
                <span className="text-nkz-sm font-medium text-nkz-text-primary">{v.variety}</span>
                <span className="text-nkz-sm text-nkz-text-secondary">
                  {expected == null ? noData : `${expected} ${unitLabel}`}
                </span>
              </Inline>
              <RangeBarValues interval={v.interval} expected={v.expected_kg_ha} scaleMax={scaleMax} />
              <p className="text-nkz-sm text-nkz-text-muted">
                {t('whatToSow.variety.trials', { count: v.n_trials })}
                {showDiseaseSummary(v) && ` · ${t('whatToSow.variety.disease', v.disease_summary)}`}
              </p>
              <div>
                <Button size="sm" variant="secondary" disabled={disabled} onClick={() => info && setAssigning({ ...info, yieldUnit: t(kgUnitKey(unit)) })}>
                  {t('whatToSow.variety.assign')}
                </Button>
                {!parcelId && (
                  <p className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.variety.needsParcel')}</p>
                )}
                {parcelId && !info && (
                  <p className="text-nkz-sm text-nkz-text-muted">{t('whatToSow.variety.noAssignData')}</p>
                )}
              </div>
            </Stack>
          );
        })}

        {assigned && (
          <Stack gap="tight">
            <p className="text-nkz-sm font-medium text-nkz-success" role="status">
              {t('whatToSow.variety.assigned', { name: assigned })}
            </p>
            {onSelectTool && (
              <Inline gap="tight" wrap>
                <Button variant="ghost" size="sm" onClick={() => onSelectTool('parcelStatus')}>
                  {t('whatToSow.variety.toHealth')}
                </Button>
                <Button variant="ghost" size="sm" onClick={() => onSelectTool('waterBudget')}>
                  {t('whatToSow.variety.toWater')}
                </Button>
              </Inline>
            )}
          </Stack>
        )}

        <SourceAttribution attributions={attributionsForRecs(attributions, [rec])} />
      </Stack>

      {assigning && parcelId && (
        <AssignVarietyModal
          variety={assigning}
          parcelId={parcelId}
          onClose={() => setAssigning(null)}
          onAssigned={() => {
            setAssigned(assigning.name);
            setAssigning(null);
            onAssigned?.();
          }}
        />
      )}
    </Card>
  );
}
