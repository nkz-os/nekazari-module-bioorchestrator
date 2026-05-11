import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Spinner, Badge } from '@nekazari/ui-kit';
import { useBioApi } from '../services/api';

const bioAccent = { base: '#14B8A6', soft: '#CCFBF1', strong: '#0D9488' };

const SourceStatusWidget: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [counts, setCounts] = useState<{ total: number; ready: number } | null>(null);

  useEffect(() => {
    api.getSources()
      .then((data: any) => setCounts({ total: data.total ?? 0, ready: data.ready ?? 0 }))
      .catch(() => setCounts(null));
  }, []);

  if (!counts) {
    return (
      <div>
        <div className="flex items-center gap-2">
          <Spinner size="sm" />
          <span className="text-nkz-sm text-nkz-text-muted">{t('sources.loading')}</span>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center gap-2 text-nkz-sm">
        <span className="font-medium text-nkz-text-primary">
          {t('sources.summary.ready')}:
        </span>
        <Badge intent="positive">{counts.ready}</Badge>
        <span className="text-nkz-text-muted">/</span>
        <span className="text-nkz-text-primary">{counts.total}</span>
      </div>
    </div>
  );
};

export default SourceStatusWidget;
