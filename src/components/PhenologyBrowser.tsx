import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Badge, Stack, Spinner, Panel, DetailGrid, DetailItem, Surface } from '@nekazari/ui-kit';
import { Sprout, BookOpen } from 'lucide-react';
import { useBioApi } from '../services/api';

const FALLBACK = ['olive', 'almond', 'grapevine', 'wheat'];

const PhenologyBrowser: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [speciesList, setSpeciesList] = useState<any[]>([]);
  const [species, setSpecies] = useState('olive');
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => { api.getSpecies().then((d: any) => { if (Array.isArray(d)) setSpeciesList(d); }).catch(() => {}); }, []);

  const fetchParams = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ species });
      setData(await api.getPhenologyParams(params));
    } catch { setData(null); }
    finally { setLoading(false); }
  }, [species]);

  useEffect(() => { fetchParams(); }, [fetchParams]);

  const options = speciesList.length > 0 ? speciesList : FALLBACK.map((n) => ({ name: n, scientific_name: undefined, has_phenology: false }));

  return (
    <Stack gap="section">
      <div className="flex gap-3 items-end">
        <div className="min-w-[200px]">
          <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('phenology.species')}</label>
          <select className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-3 text-nkz-sm" value={species} onChange={(e) => setSpecies(e.target.value)}>
            {options.map((s: any) => <option key={s.name} value={s.name}>{s.scientific_name ? `${s.name} (${s.scientific_name})` : s.name}</option>)}
          </select>
        </div>
        <button className="inline-flex items-center gap-1.5 rounded-nkz-md border px-3 py-1.5 text-xs font-medium text-nkz-accent-base hover:bg-nkz-accent-soft" onClick={() => {}}>
          <BookOpen className="w-3.5 h-3.5" />{t('phenology.contribute.button')}
        </button>
      </div>

      {loading && <div className="flex justify-center py-8"><Spinner size="md" /></div>}

      {data && (
        <Panel>
          <Panel.Header><Panel.Title><Sprout className="w-4 h-4 text-nkz-accent-base" />{data.scientific_name || species}</Panel.Title></Panel.Header>
          <Panel.Body>
            <Stack gap="stack">
              <DetailGrid columns={2}>
                <DetailItem label="Kc" value={data.kc?.toFixed(2) || '—'} />
                <DetailItem label="D1 (NWSB)" value={data.d1 != null ? `${data.d1?.toFixed(1)}°C` : '—'} />
                <DetailItem label="D2 (Max Stress)" value={data.d2 != null ? `${data.d2?.toFixed(1)}°C` : '—'} />
                <DetailItem label="MDS Ref" value={data.mds_ref != null ? `${data.mds_ref?.toFixed(0)}µm` : '—'} />
              </DetailGrid>
              {data.provenance && (
                <Surface variant="sunken" padding="stack">
                  <h4 className="text-nkz-xs font-semibold uppercase tracking-wider mb-1">{t('phenology.source')}</h4>
                  <p className="text-nkz-sm"><strong>{data.provenance.short}</strong></p>
                  {data.provenance.doi && <p className="text-nkz-xs">DOI: {data.provenance.doi}</p>}
                </Surface>
              )}
            </Stack>
          </Panel.Body>
        </Panel>
      )}
    </Stack>
  );
};

export default PhenologyBrowser;
