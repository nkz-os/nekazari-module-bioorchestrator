import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Panel, MetricCard, MetricGrid, Card, Badge, Skeleton, EmptyState, Stack } from '@nekazari/ui-kit';
import { Activity, CheckCircle, AlertTriangle, Layers } from 'lucide-react';
import { useBioApi } from '../services/api';

interface Source {
  key: string;
  name: string;
  domain: string;
  enabled: boolean;
  type: string;
  credential_status: string;
  data_available: boolean;
  status: string;
  outputs: Array<{ format: string; size_bytes: number; modified: string }>;
}

interface SourcesResponse {
  total: number;
  ready: number;
  unavailable: number;
  by_domain: Record<string, Source[]>;
  sources: Source[];
}

const DOMAIN_META: Record<string, { label: string; icon: React.FC<{ className?: string }> }> = {
  taxonomy: { label: 'Taxonomy', icon: Activity },
  phytosanitary: { label: 'Phytosanitary', icon: AlertTriangle },
  edaphoclimatic: { label: 'Edaphoclimatic', icon: Activity },
  phenology: { label: 'Phenology', icon: Activity },
  associations: { label: 'Associations', icon: Activity },
  regulatory: { label: 'Regulatory', icon: AlertTriangle },
  biocontrol: { label: 'Biocontrol', icon: Activity },
  management_ontology: { label: 'Management', icon: Activity },
  organic_inputs: { label: 'Organic Inputs', icon: Activity },
  livestock: { label: 'Livestock', icon: Activity },
  forestry: { label: 'Forestry', icon: Activity },
  agroforestry: { label: 'Agroforestry', icon: Activity },
};

const SourcesDashboard: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [data, setData] = useState<SourcesResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.getSources()
      .then(setData)
      .catch((e: any) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <Stack gap="stack">
        <Skeleton variant="rect" height="80px" />
        <Skeleton variant="rect" height="200px" />
        <Skeleton variant="rect" height="200px" />
      </Stack>
    );
  }

  if (error) {
    return (
      <EmptyState
        icon={<AlertTriangle className="w-8 h-8 text-nkz-danger" />}
        title={`${t('sources.errorPrefix')}: ${error}`}
        description=""
      />
    );
  }

  if (!data) return null;

  return (
    <Stack gap="section">
      {/* Summary metrics */}
      <MetricGrid columns={3}>
        <MetricCard
          label={t('sources.summary.total')}
          value={data.total}
        />
        <MetricCard
          label={t('sources.summary.ready')}
          value={data.ready}
        />
        <MetricCard
          label={t('sources.summary.unavailable')}
          value={data.unavailable}
        />
      </MetricGrid>

      {/* Domain sections */}
      {Object.entries(data.by_domain).map(([domain, sources]) => {
        const meta = DOMAIN_META[domain] || { label: domain, icon: Layers };
        const Icon = meta.icon;
        const readyCount = sources.filter((s) => s.status === 'ready').length;
        return (
          <Panel key={domain}>
            <Panel.Header>
              <Panel.Title>
                <Icon className="w-4 h-4 text-nkz-accent-base" />
                <span className="text-nkz-text-primary">{meta.label}</span>
                <Badge intent={readyCount === sources.length ? 'positive' : 'warning'}>
                  {readyCount}/{sources.length}
                </Badge>
              </Panel.Title>
            </Panel.Header>
            <Panel.Body>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-nkz-stack">
                {sources.map((source) => (
                  <Card key={source.key} padding="md">
                    <Stack gap="tight">
                      <div className="flex items-center justify-between">
                        <span className="text-nkz-sm font-medium text-nkz-text-primary">
                          {source.name}
                        </span>
                        <Badge
                          intent={source.status === 'ready' ? 'positive' : 'warning'}
                        >
                          {source.status === 'ready'
                            ? t('sources.status.ready')
                            : t('sources.status.unavailable')}
                        </Badge>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-nkz-xs text-nkz-text-muted">
                          {source.type}
                        </span>
                        {source.credential_status === 'missing' && (
                          <Badge intent="negative">
                            {t('sources.warnings.keyMissing')}
                          </Badge>
                        )}
                        {!source.data_available && (
                          <Badge intent="warning">
                            {t('sources.warnings.noDataFile')}
                          </Badge>
                        )}
                      </div>
                      {source.outputs.length > 0 && (
                        <div className="flex flex-wrap gap-1">
                          {source.outputs.map((o, i) => (
                            <span
                              key={i}
                              className="inline-flex items-center rounded-nkz-full bg-nkz-surface-sunken px-2 py-0.5 text-nkz-2xs text-nkz-text-muted"
                            >
                              {o.format} · {(o.size_bytes / 1024).toFixed(0)}K
                            </span>
                          ))}
                        </div>
                      )}
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
