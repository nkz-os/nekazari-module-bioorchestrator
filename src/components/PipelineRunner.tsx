import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';

interface PipelineResult {
    success: boolean;
    entities_before_dedup: number;
    entities_after_dedup: number;
    relationships: number;
    crossref_matches: number;
    duration_seconds: number;
    errors: string[];
}

const PipelineRunner: React.FC = () => {
    const { t } = useTranslation('bioorchestrator');
    const [sources, setSources] = useState<string>('');
    const [limit, setLimit] = useState<number>(50);
    const [running, setRunning] = useState(false);
    const [result, setResult] = useState<PipelineResult | null>(null);
    const [error, setError] = useState<string | null>(null);

    const handleRun = async () => {
        setRunning(true);
        setError(null);
        setResult(null);

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
        } catch (e: any) {
            setError(e.message);
        } finally {
            setRunning(false);
        }
    };

    return (
        <div className="pipeline-runner">
            <div className="pipeline-controls">
                <div className="control-group">
                    <label className="control-label">{t('pipeline.sourcesLabel')}</label>
                    <input
                        className="control-input"
                        type="text"
                        placeholder={t('pipeline.sourcesPlaceholder')}
                        value={sources}
                        onChange={(e) => setSources(e.target.value)}
                        disabled={running}
                    />
                </div>

                <div className="control-group">
                    <label className="control-label">{t('pipeline.limitLabel')}</label>
                    <input
                        className="control-input control-input--small"
                        type="number"
                        min={1}
                        max={10000}
                        value={limit}
                        onChange={(e) => setLimit(Number(e.target.value))}
                        disabled={running}
                    />
                </div>

                <button
                    className={`pipeline-btn ${running ? 'pipeline-btn--running' : ''}`}
                    onClick={handleRun}
                    disabled={running}
                >
                    {running ? t('pipeline.running') : t('pipeline.run')}
                </button>
            </div>

            {error && (
                <div className="pipeline-error">
                    ❌ {error}
                </div>
            )}

            {result && (
                <div className={`pipeline-result ${result.success ? 'pipeline-result--ok' : 'pipeline-result--error'}`}>
                    <h3 className="result-title">
                        {result.success ? t('pipeline.result.ok') : t('pipeline.result.withErrors')}
                    </h3>

                    <div className="result-grid">
                        <div className="result-stat">
                            <span className="result-number">{result.entities_before_dedup}</span>
                            <span className="result-label">{t('pipeline.result.entitiesRaw')}</span>
                        </div>
                        <div className="result-stat">
                            <span className="result-number">{result.entities_after_dedup}</span>
                            <span className="result-label">{t('pipeline.result.entitiesDedup')}</span>
                        </div>
                        <div className="result-stat">
                            <span className="result-number">{result.relationships}</span>
                            <span className="result-label">{t('pipeline.result.relationships')}</span>
                        </div>
                        <div className="result-stat">
                            <span className="result-number">{result.crossref_matches}</span>
                            <span className="result-label">{t('pipeline.result.crossrefs')}</span>
                        </div>
                        <div className="result-stat">
                            <span className="result-number">{result.duration_seconds}s</span>
                            <span className="result-label">{t('pipeline.result.duration')}</span>
                        </div>
                    </div>

                    {result.errors.length > 0 && (
                        <div className="result-errors">
                            <h4>{t('pipeline.result.errorsTitle')}</h4>
                            <ul>
                                {result.errors.map((err, i) => (
                                    <li key={i}>{err}</li>
                                ))}
                            </ul>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};

export default PipelineRunner;
