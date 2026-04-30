import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';

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

const DOMAIN_LABELS: Record<string, { label: string; icon: string; color: string }> = {
    taxonomy: { label: 'Taxonomy', icon: '🌱', color: '#059669' },
    phytosanitary: { label: 'Phytosanitary', icon: '🛡️', color: '#0284c7' },
    edaphoclimatic: { label: 'Edaphoclimatic', icon: '🌡️', color: '#d97706' },
    phenology: { label: 'Phenology', icon: '📅', color: '#7c3aed' },
    associations: { label: 'Associations', icon: '🤝', color: '#db2777' },
    regulatory: { label: 'Regulatory', icon: '⚖️', color: '#dc2626' },
    biocontrol: { label: 'Biocontrol', icon: '🐛', color: '#65a30d' },
    management_ontology: { label: 'Management', icon: '📋', color: '#0891b2' },
    organic_inputs: { label: 'Organic Inputs', icon: '🧪', color: '#16a34a' },
    livestock: { label: 'Livestock', icon: '🐄', color: '#92400e' },
    forestry: { label: 'Forestry', icon: '🌲', color: '#166534' },
    agroforestry: { label: 'Agroforestry', icon: '🌳', color: '#15803d' },
};

const SourcesDashboard: React.FC = () => {
    const { t } = useTranslation('bioorchestrator');
    const [data, setData] = useState<SourcesResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        fetch('/api/bioorchestrator/api/v1/sources')
            .then((r) => {
                if (!r.ok) throw new Error(`HTTP ${r.status}`);
                return r.json();
            })
            .then(setData)
            .catch((e) => setError(e.message))
            .finally(() => setLoading(false));
    }, []);

    if (loading) return <div className="bio-loading">{t('sources.loading')}</div>;
    if (error) return <div className="bio-error">{t('sources.errorPrefix')}: {error}</div>;
    if (!data) return null;

    return (
        <div className="sources-dashboard">
            <div className="sources-summary">
                <div className="summary-card summary-card--total">
                    <span className="summary-number">{data.total}</span>
                    <span className="summary-label">{t('sources.summary.total')}</span>
                </div>
                <div className="summary-card summary-card--ready">
                    <span className="summary-number">{data.ready}</span>
                    <span className="summary-label">{t('sources.summary.ready')}</span>
                </div>
                <div className="summary-card summary-card--unavailable">
                    <span className="summary-number">{data.unavailable}</span>
                    <span className="summary-label">{t('sources.summary.unavailable')}</span>
                </div>
            </div>

            {Object.entries(data.by_domain).map(([domain, sources]) => {
                const meta = DOMAIN_LABELS[domain] || { label: domain, icon: '📦', color: '#6b7280' };
                return (
                    <div key={domain} className="domain-section">
                        <h3 className="domain-title" style={{ borderLeftColor: meta.color }}>
                            {meta.icon} {meta.label}
                            <span className="domain-count">{sources.length}</span>
                        </h3>
                        <div className="sources-grid">
                            {sources.map((source) => (
                                <div
                                    key={source.key}
                                    className={`source-card source-card--${source.status}`}
                                >
                                    <div className="source-card-header">
                                        <span className="source-name">{source.name}</span>
                                        <span className={`source-badge source-badge--${source.status}`}>
                                            {source.status === 'ready' ? t('sources.status.ready') : t('sources.status.unavailable')}
                                        </span>
                                    </div>
                                    <div className="source-meta">
                                        <span className="source-type">{source.type}</span>
                                        {source.credential_status === 'missing' && (
                                            <span className="source-warning">{t('sources.warnings.keyMissing')}</span>
                                        )}
                                        {!source.data_available && (
                                            <span className="source-warning">{t('sources.warnings.noDataFile')}</span>
                                        )}
                                    </div>
                                    {source.outputs.length > 0 && (
                                        <div className="source-outputs">
                                            {source.outputs.map((o, i) => (
                                                <span key={i} className="output-badge">
                                                    {o.format} · {(o.size_bytes / 1024).toFixed(0)}K
                                                </span>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            ))}
                        </div>
                    </div>
                );
            })}
        </div>
    );
};

export default SourcesDashboard;
