import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge } from '@nekazari/ui-kit';
import { TrendingUp, AlertTriangle, CheckCircle, Info } from 'lucide-react';

interface TrustData {
  confidence: string;
  trials_analyzed: number;
  similar_sites_count: number;
  organic_warning?: string | null;
  inputs_used: Record<string, string>;
  data_gaps: string[];
}

interface Props {
  trust: TrustData;
  compact?: boolean;
}

const RecommendationTrustBadge: React.FC<Props> = ({ trust, compact }) => {
  const { t } = useTranslation('bioorchestrator');

  const intent = trust.confidence === 'high' ? 'positive' : trust.confidence === 'medium' ? 'warning' : 'default';
  const label = t(trust.confidence === 'high' ? 'planning.trustHigh' : trust.confidence === 'medium' ? 'planning.trustMedium' : 'planning.trustLow', { defaultValue: trust.confidence });

  if (compact) {
    return <Badge intent={intent}>{label}</Badge>;
  }

  return (
    <div className="text-nkz-xs space-y-1.5">
      <div className="flex items-center gap-1.5">
        <Badge intent={intent}>{label}</Badge>
        <span className="text-nkz-text-muted">
          {t('planning.trustTrials', { count: trust.trials_analyzed, defaultValue: `${trust.trials_analyzed} trials` })}
        </span>
        {trust.similar_sites_count > 0 && (
          <span className="text-nkz-text-muted">
            · {t('planning.trustSites', { count: trust.similar_sites_count, defaultValue: `${trust.similar_sites_count} sites` })}
          </span>
        )}
      </div>

      {trust.organic_warning && (
        <div className="flex items-center gap-1 text-nkz-text-warning">
          <AlertTriangle className="w-3 h-3" />
          <span>{t('planning.trustOrganicWarning', { defaultValue: trust.organic_warning })}</span>
        </div>
      )}

      {trust.data_gaps.length > 0 && (
        <div className="flex items-center gap-1 text-nkz-text-muted">
          <Info className="w-3 h-3" />
          <span>{t('planning.trustDataGap', { defaultValue: 'Limited data' })}: {trust.data_gaps.join(', ')}</span>
        </div>
      )}
    </div>
  );
};

export default RecommendationTrustBadge;
