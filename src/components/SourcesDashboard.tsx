import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Stack, Spinner } from '@nekazari/ui-kit';
import { AlertTriangle, Layers } from 'lucide-react';
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

const DOMAIN_META: Record<string, { label: string }> = {
  taxonomy: { label: 'Taxonomy' },
  phytosanitary: { label: 'Phytosanitary' },
  edaphoclimatic: { label: 'Edaphoclimatic' },
  phenology: { label: 'Phenology' },
  associations: { label: 'Associations' },
  regulatory: { label: 'Regulatory' },
  biocontrol: { label: 'Biocontrol' },
  management_ontology: { label: 'Management' },
  organic_inputs: { label: 'Organic Inputs' },
  livestock: { label: 'Livestock' },
  forestry: { label: 'Forestry' },
  agroforestry: { label: 'Agroforestry' },
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
    return React.createElement('div', { className: 'flex items-center justify-center py-nkz-section' },
      React.createElement(Spinner, { size: 'md' })
    );
  }

  if (error) {
    return React.createElement('div', { className: 'flex flex-col items-center justify-center py-nkz-section' },
      React.createElement(AlertTriangle, { className: 'w-8 h-8 text-nkz-danger mb-nkz-stack' }),
      React.createElement('p', { className: 'text-nkz-sm text-nkz-text-primary font-medium' }, t('sources.errorPrefix')),
      React.createElement('p', { className: 'text-nkz-xs text-nkz-text-muted' }, error)
    );
  }

  if (!data) return null;

  return React.createElement(Stack, { gap: 'section' },
    // Summary
    React.createElement('div', { className: 'grid grid-cols-3 gap-nkz-stack' },
      [
        { label: t('sources.summary.total'), value: data.total ?? 0 },
        { label: t('sources.summary.ready'), value: data.ready ?? 0 },
        { label: t('sources.summary.unavailable'), value: data.unavailable ?? 0 },
      ].map((m) =>
        React.createElement('div', { key: m.label, className: 'bg-nkz-surface border border-nkz-border rounded-nkz-md p-nkz-stack' },
          React.createElement('span', { className: 'text-nkz-xs text-nkz-text-secondary font-medium uppercase tracking-wider' }, m.label),
          React.createElement('span', { className: 'text-nkz-2xl font-semibold text-nkz-text-primary block mt-1' }, String(m.value))
        )
      )
    ),
    // Domain sections
    ...Object.entries(data.by_domain || {}).map(([domain, sources]) => {
      const meta = DOMAIN_META[domain] || { label: domain };
      return React.createElement(Card, { key: domain, padding: 'md' },
        React.createElement(Stack, { gap: 'stack' },
          React.createElement('div', { className: 'flex items-center justify-between' },
            React.createElement('span', { className: 'text-nkz-md font-semibold text-nkz-text-primary' }, meta.label),
            React.createElement(Badge, { intent: sources.every((s) => s.status === 'ready') ? 'positive' : 'warning' },
              `${sources.filter((s) => s.status === 'ready').length}/${sources.length}`
            )
          ),
          React.createElement('div', { className: 'grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-nkz-stack' },
            ...sources.map((source) =>
              React.createElement(Card, { key: source.key, padding: 'md' },
                React.createElement(Stack, { gap: 'tight' },
                  React.createElement('div', { className: 'flex items-center justify-between' },
                    React.createElement('span', { className: 'text-nkz-sm font-medium text-nkz-text-primary' }, source.name),
                    React.createElement(Badge, { intent: source.status === 'ready' ? 'positive' : 'warning' },
                      source.status === 'ready' ? t('sources.status.ready') : t('sources.status.unavailable')
                    )
                  ),
                  React.createElement('div', { className: 'flex items-center gap-2' },
                    React.createElement('span', { className: 'text-nkz-xs text-nkz-text-muted' }, source.type)
                  )
                )
              )
            )
          )
        )
      );
    })
  );
};

export default SourcesDashboard;
