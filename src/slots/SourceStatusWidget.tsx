import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';

/** Compact data source status widget for the host context-panel slot. */
const SourceStatusWidget: React.FC = () => {
    const { t } = useTranslation('bioorchestrator');
    const [counts, setCounts] = useState<{ total: number; ready: number } | null>(null);

    useEffect(() => {
        fetch('/api/bioorchestrator/api/v1/sources')
            .then((r) => {
                if (!r.ok) throw new Error(`HTTP ${r.status}`);
                return r.json();
            })
            .then((data) => setCounts({ total: data.total, ready: data.ready }))
            .catch(() => setCounts(null));
    }, []);

    if (!counts) {
        return <span className="text-gray-400 text-sm">{t('sources.loading')}</span>;
    }

    return (
        <div className="flex items-center gap-2 text-sm">
            <span className="font-medium">{t('sources.summary.ready')}:</span>
            <span className="text-green-600">{counts.ready}</span>
            <span className="text-gray-400">/</span>
            <span>{counts.total}</span>
        </div>
    );
};

export default SourceStatusWidget;
