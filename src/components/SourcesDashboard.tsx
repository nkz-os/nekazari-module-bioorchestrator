import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Panel, MetricCard, MetricGrid, Card, Badge, Skeleton, EmptyState, Stack } from '@nekazari/ui-kit';
import { AlertTriangle, Layers } from 'lucide-react';
import { useBioApi } from '../services/api';

interface Source {
  key: string; name: string; domain: string; enabled: boolean;
  type: string; credential_status: string; data_available: boolean; status: string;
  outputs: Array<{ format: string; size_bytes: number; modified: string }>;
}
interface SourcesResponse {
  total: number; ready: number; unavailable: number;
  by_domain: Record<string, Source[]>; sources: Source[];
}

const SourcesDashboard: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [data, setData] = useState<SourcesResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.getSources().then(setData).catch((e: any) => setError(e.message)).finally(() => setLoading(false));
  }, []);

  if (loading) return <Stack gap="stack"><Skeleton variant="rect" height="80px" /><Skeleton variant="rect" height="200px" /></Stack>;
  if (error) return <EmptyState icon={<AlertTriangle className="w-8 h-8 text-nkz-danger" />} title={`${t('sources.errorPrefix')}: ${error}`} />;
  if (!data) return null;

  return (
    <Stack gap="section">
      <MetricGrid columns={3}>
        <MetricCard label={t('sources.summary.total')} value={data.total ?? 0} />
        <MetricCard label={t('sources.summary.ready')} value={data.ready ?? 0} />
        <MetricCard label={t('sources.summary.unavailable')} value={data.unavailable ?? 0} />
      </MetricGrid>

      {Object.entries(data.by_domain || {}).map(([domain, sources]) => {
        const readyCount = sources.filter((s: Source) => s.status === 'ready').length;
        return (
          <Panel key={domain}>
            <Panel.Header>
              <Panel.Title>
                <Layers className="w-4 h-4 text-nkz-accent-base" />
                <span className="text-nkz-text-primary">{domain}</span>
                <Badge intent={readyCount === sources.length ? 'positive' : 'warning'}>{readyCount}/{sources.length}</Badge>
              </Panel.Title>
            </Panel.Header>
            <Panel.Body>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-nkz-stack">
                {sources.map((source: Source) => (
                  <Card key={source.key} padding="md">
                    <Stack gap="tight">
                      <div className="flex items-center justify-between">
                        <span className="text-nkz-sm font-medium text-nkz-text-primary">{source.name}</span>
                        <Badge intent={source.status === 'ready' ? 'positive' : 'warning'}>
                          {source.status === 'ready' ? t('sources.status.ready') : t('sources.status.unavailable')}
                        </Badge>
                      </div>
                      <span className="text-nkz-xs text-nkz-text-muted">{source.type}</span>
                    </Stack>
                  </Card>
                ))}
              </div>
            </Panel.Body>
          </Panel>
        );
      })}
    </Stack>
  );
};

export default SourcesDashboard;
