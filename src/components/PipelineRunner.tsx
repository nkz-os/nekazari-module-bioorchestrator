import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { SlotShell } from '@nekazari/viewer-kit';
import { Button, Badge, Spinner, Stack, Input, ProgressBar } from '@nekazari/ui-kit';
import { Play, CheckCircle, XCircle } from 'lucide-react';
import { useBioApi } from '../services/api';

const bioAccent = { base: '#14B8A6', soft: '#CCFBF1', strong: '#0D9488' };

interface PipelineResult { success: boolean; entities_before_dedup: number; entities_after_dedup: number; relationships: number; crossref_matches: number; duration_seconds: number; errors: string[]; }
interface ProgressEvent { run_id: string; step: number; total: number; connector: string; status: string; timestamp: string; }
interface HistoryEntry { run_id: string; success: boolean; entities: number; relationships: number; duration_seconds: number; sources: string[]; errors: number; timestamp: string; }

const PipelineRunner: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [sources, setSources] = useState<string>('');
  const [limit, setLimit] = useState<number>(50);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<PipelineResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<ProgressEvent | null>(null);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    api.getPipelineHistory(5).then((data: any) => setHistory(data?.history || [])).catch(() => {});
  }, []);

  useEffect(() => () => { if (eventSourceRef.current) eventSourceRef.current.close(); }, []);

  const handleRun = async () => {
    setRunning(true); setError(null); setResult(null); setProgress(null);
    const es = new EventSource('/api/bioorchestrator/api/pipeline/progress');
    eventSourceRef.current = es;
    es.onmessage = (event) => { try { setProgress(JSON.parse(event.data)); } catch {} };
    es.onerror = () => { es.close(); };
    try {
      const body: any = { limit };
      if (sources.trim()) body.sources = sources.split(',').map((s: string) => s.trim());
      const data = await api.runPipeline(body);
      setResult(data);
      api.getPipelineHistory(5).then((d: any) => setHistory(d?.history || [])).catch(() => {});
    } catch (e: any) { setError(e.message); } finally {
      setRunning(false); es.close(); eventSourceRef.current = null;
    }
  };

  const progressPct = progress && progress.total > 0 ? Math.round((progress.step / progress.total) * 100) : 0;

  return (
    <SlotShell moduleId="bioorchestrator" title={t('pipeline.title')} icon={<Play className="w-4 h-4" />} collapsible accent={bioAccent}>
      <Stack gap="stack">
        <div className="flex gap-3 items-end flex-wrap">
          <div className="flex-1 min-w-[200px]">
            <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('pipeline.sourcesLabel')}</label>
            <Input type="text" placeholder={t('pipeline.sourcesPlaceholder')} value={sources} onChange={(e: any) => setSources(e.target.value)} disabled={running} size="sm" />
          </div>
          <div className="w-24">
            <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('pipeline.limitLabel')}</label>
            <Input type="number" min={1} max={10000} value={String(limit)} onChange={(e: any) => setLimit(Number(e.target.value))} disabled={running} size="sm" />
          </div>
          <Button variant="primary" size="sm" onClick={handleRun} disabled={running} leadingIcon={running ? <Spinner size="sm" /> : <Play className="w-4 h-4" />}>
            {running ? t('pipeline.running') : t('pipeline.run')}
          </Button>
        </div>

        {progress && (
          <div className="bg-nkz-surface-sunken rounded-nkz-md p-nkz-inline">
            <div className="flex justify-between text-nkz-xs mb-1">
              <span className="text-nkz-text-muted">{progress.connector}: {progress.status}</span>
              <span className="text-nkz-text-muted">{progress.step}/{progress.total}</span>
            </div>
            <ProgressBar value={progressPct} size="sm" />
          </div>
        )}

        {error && <Badge intent="negative" className="flex items-center gap-2"><span className="text-nkz-xs">{error}</span></Badge>}

        {result && (
          <div className={`bg-nkz-surface-sunken rounded-nkz-md p-nkz-stack border ${result.success ? 'border-nkz-success' : 'border-nkz-danger'}`}>
            <h3 className="text-nkz-sm font-semibold text-nkz-text-primary flex items-center gap-2">
              {result.success ? <CheckCircle className="w-4 h-4 text-nkz-success" /> : <XCircle className="w-4 h-4 text-nkz-danger" />}
              {result.success ? t('pipeline.result.ok') : t('pipeline.result.withErrors')}
            </h3>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-2 mt-2">
              {[
                [t('pipeline.result.entitiesRaw'), result.entities_before_dedup],
                [t('pipeline.result.entitiesDedup'), result.entities_after_dedup],
                [t('pipeline.result.relationships'), result.relationships],
                [t('pipeline.result.crossrefs'), result.crossref_matches],
                [t('pipeline.result.duration'), `${result.duration_seconds}s`],
              ].map(([label, value]) => (
                <div key={label} className="text-center p-nkz-inline bg-nkz-surface rounded-nkz-md">
                  <span className="block text-nkz-lg font-bold text-nkz-accent-base">{value}</span>
                  <span className="text-nkz-xs text-nkz-text-muted">{label}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {history.length > 0 && (
          <div className="bg-nkz-surface-sunken rounded-nkz-md p-nkz-inline">
            <h4 className="text-nkz-xs font-semibold text-nkz-text-primary mb-2">{t('pipeline.history') || 'Recent Runs'}</h4>
            <table className="w-full text-nkz-xs">
              <thead><tr className="text-nkz-text-muted text-left"><th className="pb-1 pr-2">Run</th><th className="pb-1 pr-2">Status</th><th className="pb-1 pr-2">Entities</th><th className="pb-1">Duration</th></tr></thead>
              <tbody>
                {history.map((h, i) => (
                  <tr key={i} className="border-t border-nkz-border">
                    <td className="py-1 pr-2 text-nkz-text-muted">{h.timestamp ? new Date(h.timestamp).toLocaleString() : h.run_id}</td>
                    <td className="py-1 pr-2">{h.success ? <CheckCircle className="w-3.5 h-3.5 text-nkz-success inline" /> : <XCircle className="w-3.5 h-3.5 text-nkz-danger inline" />}</td>
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
