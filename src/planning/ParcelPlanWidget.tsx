import React from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from '@nekazari/sdk';
import { SlotShell } from '@nekazari/viewer-kit';
import { Button, Inline, Stack } from '@nekazari/ui-kit';
import { CalendarRange } from 'lucide-react';
import { resolveParcelContext, type ParcelEntityData } from '../utils/entityData';
import { fetchFarm } from './api';
import { ACCENT, parcelUrn } from './format';
import { canPlan, namesIndex, simulationAvailable, warningText, widgetRows } from './matrixModel';
import LoadingLine from './LoadingLine';
import { CycleLine } from './PlanMatrix';
import { useLoad } from './useLoad';

const ParcelPlanWidget: React.FC<{ entityData?: ParcelEntityData }> = ({ entityData }) => {
  const { t, i18n } = useTranslation('bioorchestrator');
  const navigate = useNavigate();
  const { parcelId } = resolveParcelContext(entityData);
  const urn = parcelId ? parcelUrn(parcelId) : null;
  const farm = useLoad((s) => fetchFarm(undefined, urn as string, s), [urn], Boolean(urn));
  const data = farm.data;
  const row = data?.parcels[0];
  return (
    <SlotShell moduleId="bioorchestrator" title={t('planning.widget.title')} icon={<CalendarRange className="w-4 h-4" />} accent={ACCENT}>
      {farm.loading && <LoadingLine text={t('planning.loading.generic')} />}
      {farm.error != null && (
        <Inline gap="inline" align="center" wrap>
          <p className="text-nkz-sm text-nkz-danger">{t('planning.errorLoad')}</p>
          <Button size="sm" variant="secondary" onClick={farm.retry}>{t('planning.retry')}</Button>
        </Inline>
      )}
      {data && row && (
        <Stack gap="tight">
          {widgetRows(row, data.campaign).map(({ campaign, cells }) => (
            <div key={campaign} className="text-nkz-sm">
              <span className="font-medium text-nkz-text-primary">{`${campaign} `}</span>
              {cells.length ? cells.map((c, i) => <CycleLine key={c.crop_id ?? i} cell={c} />)
                : <span className="text-nkz-text-muted">{t('planning.widget.undecided')}</span>}
            </div>
          ))}
          {widgetRows(row, data.campaign).length > 0 && (
            <p className="text-nkz-xs text-nkz-text-muted">{t('planning.matrix.legend')}</p>
          )}
          {row.warnings.map((w, i) => (
            <p key={`${w.code}-${i}`} className="text-nkz-xs text-nkz-warning">{`⚠ ${warningText(w, namesIndex(data), i18n.language, t)}`}</p>
          ))}
          {canPlan(row, data.campaign) && (
            <div>
              <Button size="sm" variant="secondary"
                onClick={() => navigate(`/bioorchestrator?parcel=${encodeURIComponent(row.parcel_id)}&campaign=${data.campaign}`)}>
                {t('planning.widget.plan')}
              </Button>
            </div>
          )}
          {simulationAvailable(row, data.campaign) && (
            <div>
              <Button size="sm" variant="secondary"
                onClick={() => navigate(`/bioorchestrator?view=simulation&parcel=${encodeURIComponent(row.parcel_id)}`)}>
                {t('planning.simulation.open')}
              </Button>
            </div>
          )}
        </Stack>
      )}
    </SlotShell>
  );
};

export default ParcelPlanWidget;
