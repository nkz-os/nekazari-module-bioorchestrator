import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { SlotShellCompact } from '@nekazari/viewer-kit';
import { Spinner, Badge, Stack } from '@nekazari/ui-kit';

const bioAccent = { base: '#14B8A6', soft: '#CCFBF1', strong: '#0D9488' };

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
        return (
            <SlotShellCompact moduleId="bioorchestrator" accent={bioAccent}>
                <div className="flex items-center gap-2">
                    <Spinner size="sm" />
                    <span className="text-nkz-sm text-nkz-text-muted">{t('sources.loading')}</span>
                </div>
            </SlotShellCompact>
        );
    }

    return (
        <SlotShellCompact moduleId="bioorchestrator" accent={bioAccent}>
            <div className="flex items-center gap-2 text-nkz-sm">
                <span className="font-medium text-nkz-text-primary">{t('sources.summary.ready')}:</span>
                <Badge intent="positive" size="sm">{counts.ready}</Badge>
                <span className="text-nkz-text-muted">/</span>
                <span className="text-nkz-text-primary">{counts.total}</span>
            </div>
        </SlotShellCompact>
    );
};

export default SourceStatusWidget;
