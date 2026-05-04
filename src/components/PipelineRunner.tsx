import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { SlotShell } from '@nekazari/viewer-kit';
import { Button, Badge, Spinner, Stack, Input } from '@nekazari/ui-kit';
import { Play, AlertCircle, CheckCircle, XCircle } from 'lucide-react';

const bioAccent = { base: '#14B8A6', soft: '#CCFBF1', strong: '#0D9488' };

interface PipelineResult {
    success: boolean;
    entities_before_dedup: number;
    entities_after_dedup: number;
    relationships: number;
    crossref_matches: number;
    duration_seconds: number;
    errors: string[];
}

interface ProgressEvent {
    run_id: string;
    step: number;
    total: number;
    connector: string;
    status: string;
    timestamp: string;
}

interface HistoryEntry {
    run_id: string;
    success: boolean;
    entities: number;
    relationships: number;
    duration_seconds: number;
    sources: string[];
    errors: number;
    timestamp: string;
}

const PipelineRunner: React.FC = () => {
    const { t } = useTranslation('bioorchestrator');
    const [sources, setSources] = useState<string>('');
    const [limit, setLimit] = useState<number>(50);
    const [running, setRunning] = useState(false);
    const [result, setResult] = useState<PipelineResult | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [progress, setProgress] = useState<ProgressEvent | null>(null);
    const [history, setHistory] = useState<HistoryEntry[]>([]);
    const eventSourceRef = useRef<EventSource | null>(null);

    useEffect(() => {
        fetch('/api/bioorchestrator/api/pipeline/history?limit=5')
            .then(r => r.ok ? r.json() : null)
            .then(data => setHistory(data?.history || []))
            .catch(() => {});
    }, []);

    useEffect(() => {
        return () => {
            if (eventSourceRef.current) {
                eventSourceRef.current.close();
            }
        };
    }, []);

    const handleRun = async () => {
        setRunning(true);
        setError(null);
        setResult(null);
        setProgress(null);

        // Open SSE connection for progress
        const es = new EventSource('/api/bioorchestrator/api/pipeline/progress');
        eventSourceRef.current = es;
        es.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                setProgress(data);
            } catch {}
        };
        es.onerror = () => {
            es.close();
        };

        try {
            const body: any = { limit };
            if (sources.trim()) {
                body.sources = sources.split(',').map((s) => s.trim());
            }

            const response = await fetch('/api/bioorchestrator/api/pipeline/run', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });

            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            setResult(data);

            // Refresh history
            fetch('/api/bioorchestrator/api/pipeline/history?limit=5')
                .then(r => r.ok ? r.json() : null)
                .then(d => setHistory(d?.history || []))
                .catch(() => {});
        } catch (e: any) {
            setError(e.message);
        } finally {
            setRunning(false);
            es.close();
            eventSourceRef.current = null;
        }
    };

    return (
        <SlotShell
            title={t('pipeline.title')}
            icon={<Play className="w-4 h-4" />}
            collapsible
            accent={bioAccent}
        >
            <Stack gap="stack">
                {/* Controls */}
                <div className="flex gap-3 items-end flex-wrap">
                    <div className="flex-1 min-w-[200px]">
                        <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                            {t('pipeline.sourcesLabel')}
                        </label>
                        <Input
                            type="text"
                            placeholder={t('pipeline.sourcesPlaceholder')}
                            value={sources}
                            onChange={(e) => setSources(e.target.value)}
                            disabled={running}
                            size="sm"
                        />
                    </div>
                    <div className="flex-shrink-0">
                        <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                            {t('pipeline.limitLabel')}
                        </label>
                        <Input
                            type="number"
                            min={1}
                            max={10000}
                            value={limit}
                            onChange={(e) => setLimit(Number(e.target.value))}
                            disabled={running}
                            size="sm"
                        />
                    </div>
                    <Button
                        variant="primary"
                        size="sm"
                        onClick={handleRun}
                        disabled={running}
                        leadingIcon={running ? <Spinner size="sm" /> : <Play className="w-4 h-4" />}
                    >
                        {running ? t('pipeline.running') : t('pipeline.run')}
                    </Button>
                </div>

                {/* Progress bar */}
                {progress && (
                    <div className="bg-nkz-surface-sunken rounded-nkz-md p-nkz-inline">
                        <div className="flex justify-between text-nkz-xs mb-1">
                            <span className="text-nkz-text-muted">
                                {progress.connector}: {progress.status}
                            </span>
                            <span className="text-nkz-text-muted">
                                {progress.step}/{progress.total}
                            </span>
                        </div>
                        <div className="w-full bg-nkz-surface rounded-full h-2">
                            <div
                                className="h-2 rounded-full transition-all duration-300"
                                style={{
                                    width: `${(progress.step / progress.total) * 100}%`,
                                    background: bioAccent.base,
                                }}
                            />
                        </div>
                    </div>
                )}

                {/* Error */}
                {error && (
                    <Badge intent="negative" className="flex items-center gap-2">
                        <AlertCircle className="w-4 h-4" />
                        <span className="text-nkz-xs">{error}</span>
                    </Badge>
                )}

                {/* Result */}
                {result && (
                    <div className={`bg-nkz-surface-sunken rounded-nkz-md p-nkz-stack border ${
                        result.success ? 'border-nkz-success' : 'border-nkz-danger'
                    }`}>
                        <Stack gap="stack">
                            <h3 className="text-nkz-sm font-semibold text-nkz-text-primary flex items-center gap-2">
                                {result.success
                                    ? <CheckCircle className="w-4 h-4 text-nkz-success" />
                                    : <XCircle className="w-4 h-4 text-nkz-danger" />
                                }
                                {result.success ? t('pipeline.result.ok') : t('pipeline.result.withErrors')}
                            </h3>

                            <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
                                <div className="text-center p-nkz-inline bg-nkz-surface rounded-nkz-md">
                                    <span className="block text-nkz-lg font-bold text-nkz-accent-base">{result.entities_before_dedup}</span>
                                    <span className="text-nkz-xs text-nkz-text-muted">{t('pipeline.result.entitiesRaw')}</span>
                                </div>
                                <div className="text-center p-nkz-inline bg-nkz-surface rounded-nkz-md">
                                    <span className="block text-nkz-lg font-bold text-nkz-accent-base">{result.entities_after_dedup}</span>
                                    <span className="text-nkz-xs text-nkz-text-muted">{t('pipeline.result.entitiesDedup')}</span>
                                </div>
                                <div className="text-center p-nkz-inline bg-nkz-surface rounded-nkz-md">
                                    <span className="block text-nkz-lg font-bold text-nkz-accent-base">{result.relationships}</span>
                                    <span className="text-nkz-xs text-nkz-text-muted">{t('pipeline.result.relationships')}</span>
                                </div>
                                <div className="text-center p-nkz-inline bg-nkz-surface rounded-nkz-md">
                                    <span className="block text-nkz-lg font-bold text-nkz-accent-base">{result.crossref_matches}</span>
                                    <span className="text-nkz-xs text-nkz-text-muted">{t('pipeline.result.crossrefs')}</span>
                                </div>
                                <div className="text-center p-nkz-inline bg-nkz-surface rounded-nkz-md">
                                    <span className="block text-nkz-lg font-bold text-nkz-accent-base">{result.duration_seconds}s</span>
                                    <span className="text-nkz-xs text-nkz-text-muted">{t('pipeline.result.duration')}</span>
                                </div>
                            </div>

                            {result.errors.length > 0 && (
                                <div className="bg-nkz-danger-soft rounded-nkz-md p-nkz-inline border border-nkz-danger">
                                    <h4 className="text-nkz-xs font-semibold text-nkz-danger-strong mb-1">{t('pipeline.result.errorsTitle')}</h4>
                                    <ul className="list-disc pl-4 text-nkz-xs text-nkz-danger-strong space-y-0.5">
                                        {result.errors.map((err, i) => (
                                            <li key={i}>{err}</li>
                                        ))}
                                    </ul>
                                </div>
                            )}
                        </Stack>
                    </div>
                )}

                {/* History table */}
                {history.length > 0 && (
                    <div className="bg-nkz-surface-sunken rounded-nkz-md p-nkz-inline">
                        <h4 className="text-nkz-xs font-semibold text-nkz-text-primary mb-2">
                            {t('pipeline.history') || 'Recent Runs'}
                        </h4>
                        <table className="w-full text-nkz-xs">
                            <thead>
                                <tr className="text-nkz-text-muted text-left">
                                    <th className="pb-1 pr-2">Run</th>
                                    <th className="pb-1 pr-2">Status</th>
                                    <th className="pb-1 pr-2">Entities</th>
                                    <th className="pb-1">Duration</th>
                                </tr>
                            </thead>
                            <tbody>
                                {history.map((h, i) => (
                                    <tr key={i} className="border-t border-nkz-border">
                                        <td className="py-1 pr-2 text-nkz-text-muted">
                                            {h.timestamp ? new Date(h.timestamp).toLocaleString() : h.run_id}
                                        </td>
                                        <td className="py-1 pr-2">
                                            {h.success ? '✅' : '❌'}
                                            {h.errors > 0 && <span className="text-nkz-danger ml-1">{h.errors} err</span>}
                                        </td>
                                        <td className="py-1 pr-2 text-nkz-text-primary">{h.entities}</td>
                                        <td className="py-1 text-nkz-text-muted">{h.duration_seconds}s</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
            </Stack>
        </SlotShell>
    );
};

export default PipelineRunner;
