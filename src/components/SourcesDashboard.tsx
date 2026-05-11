import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Stack, Spinner, Panel, MetricCard, MetricGrid, Skeleton, EmptyState } from '@nekazari/ui-kit';
import { AlertTriangle } from 'lucide-react';
import { useBioApi } from '../services/api';

interface Source { key: string; name: string; domain: string; type: string; status: string; credential_status: string; data_available: boolean; outputs: Array<{ format: string; size_bytes: number }>; }
interface SourcesResponse { total: number; ready: number; unavailable: number; by_domain: Record<string, Source[]>; sources: Source[]; }

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

  const domains = data.by_domain || {};

  return (
    <Stack gap="section">
      <MetricGrid columns={3}>
        <MetricCard label={t('sources.summary.total')} value={data.total ?? 0} />
        <MetricCard label={t('sources.summary.ready')} value={data.ready ?? 0} />
        <MetricCard label={t('sources.summary.unavailable')} value={data.unavailable ?? 0} />
      </MetricGrid>

      {Object.keys(domains).map((domain) => {
        const sources = domains[domain] || [];
        const readyCount = sources.filter((s: Source) => s.status === 'ready').length;
        return (
          <Panel key={domain}>
            <Panel.Header>
              <Panel.Title>
                {domain}
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
