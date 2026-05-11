import React, { useState, useEffect } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Panel, Stack, Button, Input, Badge, Spinner, Surface, IconButton, Card, DataTable } from '@nekazari/ui-kit';
import { Search, Database, Globe, Settings, X, CheckCircle, ExternalLink } from 'lucide-react';
import { useBioApi, getDadisCredentials, setDadisCredentials, clearDadisCredentials } from '../../services/api';

interface DadisBreed { breedName: string; breedId: string; speciesId: number; countryISO3: string; transboundaryId?: string; }
interface DadisCountry { iso3: string; name: string; }
interface DadisSpecies { id: number; name: string; }
const DADIS_DEFAULT_URL = 'https://us-central1-fao-dadis-dev.cloudfunctions.net/api/v1';
const selectCls = "w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base";

const breedColumns = [
  { accessorKey: 'breedName', header: 'Breed' },
  { accessorKey: 'speciesId', header: 'Species ID' },
  { accessorKey: 'countryISO3', header: 'Country' },
  { accessorKey: 'transboundaryId', header: 'Transboundary' },
];

export const BreedDiscovery: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [hasCredentials, setHasCredentials] = useState<boolean>(() => !!getDadisCredentials());
  const [showSettings, setShowSettings] = useState(false);
  const [settingsUrl, setSettingsUrl] = useState('');
  const [settingsToken, setSettingsToken] = useState('');
  const [countries, setCountries] = useState<DadisCountry[]>([]);
  const [species, setSpecies] = useState<DadisSpecies[]>([]);
  const [refLoading, setRefLoading] = useState(false);
  const [breeds, setBreeds] = useState<DadisBreed[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedCountry, setSelectedCountry] = useState<string>('');
  const [selectedSpecies, setSelectedSpecies] = useState<string>('');
  const [classification, setClassification] = useState<string>('all');

  useEffect(() => {
    if (!hasCredentials) return;
    setRefLoading(true);
    Promise.all([api.getDadisCountries().catch(() => []), api.getDadisSpecies().catch(() => [])])
      .then(([c, s]) => { setCountries(Array.isArray(c) ? c : []); setSpecies(Array.isArray(s) ? s : []); })
      .catch(() => {}).finally(() => setRefLoading(false));
  }, [hasCredentials]);

  const saveCreds = () => { if (settingsUrl.trim() && settingsToken.trim()) { setDadisCredentials(settingsUrl.trim(), settingsToken.trim()); setHasCredentials(true); setShowSettings(false); } };
  const clearCreds = () => { clearDadisCredentials(); setHasCredentials(false); setShowSettings(false); setCountries([]); setSpecies([]); setBreeds([]); };

  const handleSearch = async () => {
    if (!hasCredentials) return;
    setLoading(true); setError(null);
    try { setBreeds(await api.getDadisBreeds(classification, selectedCountry ? [selectedCountry] : undefined, selectedSpecies ? [Number(selectedSpecies)] : undefined)); }
    catch (err: any) { setError(err.message || t('dadis.errors.loadBreeds')); setBreeds([]); }
    finally { setLoading(false); }
  };

  if (!hasCredentials) {
    return (
      <Stack gap="section">
        <Panel>
          <Panel.Header><Panel.Title><Database className="w-4 h-4 text-nkz-accent-base" />{t('dadis.title')}</Panel.Title></Panel.Header>
          <Panel.Body>
            <Stack gap="stack">
              <div className="flex items-start gap-3 rounded-nkz-md bg-nkz-warning-soft border border-nkz-warning p-nkz-stack">
                <ExternalLink className="w-4 h-4 text-nkz-warning flex-shrink-0 mt-0.5" />
                <div className="text-nkz-sm"><p className="font-medium mb-1">{t('dadis.settings.description')}</p><a href="https://www.fao.org/dad-is/en/" target="_blank" rel="noopener" className="inline-flex items-center gap-1 text-nkz-accent-base text-nkz-xs hover:underline">{t('dadis.settings.requestAccess')} <ExternalLink className="w-3 h-3" /></a></div>
              </div>
              {showSettings ? (
                <Surface variant="sunken" padding="stack">
                  <Stack gap="stack">
                    <div className="flex items-center justify-between"><h4 className="text-nkz-sm font-medium">{t('dadis.settings.title')}</h4><IconButton aria-label="Close" size="sm" variant="ghost" onClick={() => setShowSettings(false)}><X className="w-4 h-4" /></IconButton></div>
                    <div><label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('dadis.settings.apiUrl')}</label><Input value={settingsUrl} onChange={(e: any) => setSettingsUrl(e.target.value)} placeholder={DADIS_DEFAULT_URL} size="sm" /></div>
                    <div><label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('dadis.settings.apiToken')}</label><Input type="password" value={settingsToken} onChange={(e: any) => setSettingsToken(e.target.value)} placeholder={t('dadis.settings.tokenPlaceholder')} size="sm" /></div>
                    <Button variant="primary" size="sm" onClick={saveCreds}>{t('dadis.settings.save')}</Button>
                  </Stack>
                </Surface>
              ) : (
                <Button variant="secondary" size="sm" onClick={() => setShowSettings(true)}><Settings className="w-4 h-4 mr-1.5" />{t('dadis.settings.configure')}</Button>
              )}
            </Stack>
          </Panel.Body>
        </Panel>

        <Panel>
          <Panel.Header><Panel.Title><Globe className="w-4 h-4 text-nkz-accent-base" />{t('dadis.fallback.title')}</Panel.Title></Panel.Header>
          <Panel.Body>
            <p className="text-nkz-sm text-nkz-text-secondary mb-3">{t('dadis.fallback.description')}</p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {['GBIF Livestock', 'AGROVOC', 'EPPO', 'GlobalTreeSearch', 'EU Pesticides', 'CPVO Varieties'].map((name) => (
                <Card key={name} padding="sm"><div className="flex items-start gap-2"><CheckCircle className="w-3.5 h-3.5 text-nkz-success flex-shrink-0 mt-0.5" /><p className="text-nkz-sm font-medium">{name}</p></div></Card>
              ))}
            </div>
          </Panel.Body>
        </Panel>
      </Stack>
    );
  }

  return (
    <Panel>
      <Panel.Header>
        <Panel.Title><Database className="w-4 h-4 text-nkz-accent-base" />{t('dadis.title')}</Panel.Title>
        <Panel.Actions>
          <Badge intent="positive"><CheckCircle className="w-3 h-3 mr-1" />{t('dadis.settings.connected')}</Badge>
          <IconButton aria-label="Settings" size="sm" variant="ghost" onClick={() => setShowSettings(!showSettings)}>{showSettings ? <X className="w-4 h-4" /> : <Settings className="w-4 h-4" />}</IconButton>
        </Panel.Actions>
      </Panel.Header>
      <Panel.Body>
        <Stack gap="stack">
          {showSettings && (
            <Surface variant="sunken" padding="stack">
              <Stack gap="stack">
                <h4 className="text-nkz-sm font-medium">{t('dadis.settings.title')}</h4>
                <Input value={settingsUrl} onChange={(e: any) => setSettingsUrl(e.target.value)} placeholder={DADIS_DEFAULT_URL} size="sm" />
                <Input type="password" value={settingsToken} onChange={(e: any) => setSettingsToken(e.target.value)} placeholder={t('dadis.settings.tokenPlaceholder')} size="sm" />
                <div className="flex gap-2"><Button variant="primary" size="sm" onClick={saveCreds}>{t('dadis.settings.update')}</Button><Button variant="danger" size="sm" onClick={clearCreds}>{t('dadis.settings.disconnect')}</Button></div>
              </Stack>
            </Surface>
          )}

          {refLoading ? <div className="flex justify-center py-4"><Spinner size="sm" /></div> : (
            <div className="flex gap-3 items-end flex-wrap">
              <div className="min-w-[160px]"><label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('dadis.filters.country')}</label><select className={selectCls} value={selectedCountry} onChange={(e) => setSelectedCountry(e.target.value)}><option value="">{t('dadis.filters.allCountries')}</option>{countries.map((c) => <option key={c.iso3} value={c.iso3}>{c.name}</option>)}</select></div>
              <div className="min-w-[140px]"><label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('dadis.filters.species')}</label><select className={selectCls} value={selectedSpecies} onChange={(e) => setSelectedSpecies(e.target.value)}><option value="">{t('dadis.filters.allSpecies')}</option>{species.map((s) => <option key={s.id} value={String(s.id)}>{s.name || s.id}</option>)}</select></div>
              <div className="min-w-[160px]"><label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t('dadis.filters.classification')}</label><select className={selectCls} value={classification} onChange={(e) => setClassification(e.target.value)}><option value="all">{t('dadis.filters.all')}</option><option value="local">{t('dadis.filters.local')}</option><option value="transboundary">{t('dadis.filters.transboundary')}</option></select></div>
              <Button variant="primary" size="sm" onClick={handleSearch} disabled={loading} leadingIcon={loading ? <Spinner size="sm" /> : <Search className="w-4 h-4" />}>{loading ? t('dadis.searching') : t('dadis.search')}</Button>
            </div>
          )}

          {error && <Badge intent="negative">{error}</Badge>}

          {loading ? <div className="flex justify-center py-8"><Spinner size="md" /></div>
            : breeds.length > 0 ? (
              <div>
                <DataTable columns={breedColumns} data={breeds} />
                <p className="text-nkz-xs text-nkz-text-muted text-right mt-2">{t('dadis.footer', { count: breeds.length })}</p>
              </div>
            ) : !error ? (
              <div className="flex flex-col items-center justify-center py-nkz-section text-center"><Database className="w-8 h-8 text-nkz-text-muted mb-2" /><p className="text-nkz-sm font-medium">{t('dadis.empty.title')}</p><p className="text-nkz-xs text-nkz-text-muted">{t('dadis.empty.hint')}</p></div>
            ) : null}
        </Stack>
      </Panel.Body>
    </Panel>
  );
};
