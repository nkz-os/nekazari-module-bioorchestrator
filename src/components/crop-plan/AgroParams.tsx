import React from 'react';
import { useTranslation } from 'react-i18next';
import type { WaterBudget } from '../../types/cropplan';
import type { AgronomicValue } from '../../types/agronomic';
import AgronomicBadge from '../shared/AgronomicBadge';

function Row({ label, av }: { label: string; av?: AgronomicValue | null }) {
  if (!av) return null;
  return (
    <div className="flex items-center justify-between py-1 border-b border-nkz-border last:border-0">
      <span className="text-nkz-xs text-nkz-text-secondary">{label}</span>
      <span className="flex items-center gap-2">
        <span className="text-nkz-sm font-medium text-nkz-text-primary">{av.value ?? '—'}</span>
        <AgronomicBadge av={av} />
      </span>
    </div>
  );
}

/** Agronomic parameters (Kc, irrigation, ETc…), each carrying its P1 reliability badge. */
const AgroParams: React.FC<{ water?: WaterBudget | null; phenoParams?: Record<string, AgronomicValue> | null }> = ({
  water,
  phenoParams,
}) => {
  const { t } = useTranslation('bioorchestrator');
  const ag = water?.agronomic;
  if (!ag && !phenoParams) {
    return <p className="text-nkz-sm text-nkz-text-muted">{t('cropPlan.params.empty')}</p>;
  }
  return (
    <div>
      <Row label={t('cropPlan.params.irrigation')} av={ag?.irrigation_required_mm} />
      <Row label={t('cropPlan.params.etc')} av={ag?.etc_weekly_mm} />
      <Row label={t('cropPlan.params.kc')} av={ag?.kc ?? phenoParams?.kc} />
      <Row label={t('cropPlan.params.d1')} av={phenoParams?.d1} />
      <Row label={t('cropPlan.params.d2')} av={phenoParams?.d2} />
    </div>
  );
};

export default AgroParams;
