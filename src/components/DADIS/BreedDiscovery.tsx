import React, { useState, useEffect } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Stack, Button, Input, Badge, Spinner } from '@nekazari/ui-kit';
import { Search, Database, Globe, Settings, X, CheckCircle, ExternalLink } from 'lucide-react';
import { useBioApi, getDadisCredentials, setDadisCredentials, clearDadisCredentials } from '../../services/api';

interface DadisBreed {
  breedName: string;
  breedId: string;
  speciesId: number;
  countryISO3: string;
  transboundaryId?: string;
  lastModification?: string;
}

interface DadisCountry { iso3: string; name: string; }
interface DadisSpecies { id: number; name: string; }

const DADIS_DEFAULT_URL = 'https://us-central1-fao-dadis-dev.cloudfunctions.net/api/v1';

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
  const [classification, setClassification] = useState<'all' | 'local' | 'transboundary'>('all');

  useEffect(() => {
    if (!hasCredentials) return;
    setRefLoading(true);
    Promise.all([
      api.getDadisCountries().catch(() => []),
      api.getDadisSpecies().catch(() => []),
    ]).then(([c, s]) => {
      setCountries(Array.isArray(c) ? c : []);
      setSpecies(Array.isArray(s) ? s : []);
    }).catch(() => {}).finally(() => setRefLoading(false));
  }, [hasCredentials]);

  const handleSaveCredentials = () => {
    if (settingsUrl.trim() && settingsToken.trim()) {
      setDadisCredentials(settingsUrl.trim(), settingsToken.trim());
      setHasCredentials(true);
      setShowSettings(false);
    }
  };

  const handleClearCredentials = () => {
    clearDadisCredentials();
    setHasCredentials(false);
    setShowSettings(false);
    setCountries([]);
    setSpecies([]);
    setBreeds([]);
  };

  const handleSearch = async () => {
    if (!hasCredentials) return;
    setLoading(true);
    setError(null);
    try {
      const countryIds = selectedCountry ? [selectedCountry] : undefined;
      const speciesIds = selectedSpecies ? [Number(selectedSpecies)] : undefined;
      const results = await api.getDadisBreeds(classification, countryIds, speciesIds);
      setBreeds(Array.isArray(results) ? results : []);
    } catch (err: any) {
      setError(err.message || t('dadis.errors.loadBreeds'));
      setBreeds([]);
    } finally {
      setLoading(false);
    }
  };

  // ── No credentials: setup screen ────────────────────────────────────
  if (!hasCredentials) {
    return (
      <Stack gap="section">
        <Card padding="md">
          <Stack gap="stack">
            <div className="flex items-center gap-2">
              <Database className="w-4 h-4 text-nkz-accent-base" />
              <h2 className="text-nkz-md font-semibold text-nkz-text-primary">{t('dadis.title')}</h2>
            </div>

            <div className="flex items-start gap-3 rounded-nkz-md bg-nkz-warning-soft border border-nkz-warning p-nkz-stack">
              <ExternalLink className="w-4 h-4 text-nkz-warning flex-shrink-0 mt-0.5" />
              <div className="text-nkz-sm text-nkz-text-primary">
                <p className="font-medium mb-1">{t('dadis.settings.description')}</p>
                <a
                  href="https://www.fao.org/dad-is/en/"
                  target="_blank"
                  rel="noopener"
                  className="inline-flex items-center gap-1 text-nkz-accent-base text-nkz-xs mt-2 hover:underline"
                >
                  {t('dadis.settings.requestAccess')} <ExternalLink className="w-3 h-3" />
                </a>
              </div>
            </div>

            {showSettings ? (
              <div className="bg-nkz-surface-sunken rounded-nkz-md p-nkz-stack">
                <Stack gap="stack">
                  <div className="flex items-center justify-between">
                    <h4 className="text-nkz-sm font-medium text-nkz-text-primary">
                      {t('dadis.settings.title')}
                    </h4>
                    <button
                      onClick={() => setShowSettings(false)}
                      className="w-7 h-7 inline-flex items-center justify-center rounded-nkz-md text-nkz-text-muted hover:bg-nkz-surface"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                  <div>
                    <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                      {t('dadis.settings.apiUrl')}
                    </label>
                    <Input
                      value={settingsUrl}
                      onChange={(e: any) => setSettingsUrl(e.target.value)}
                      placeholder={DADIS_DEFAULT_URL}
                      size="sm"
                    />
                  </div>
                  <div>
                    <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                      {t('dadis.settings.apiToken')}
                    </label>
                    <Input
                      type="password"
                      value={settingsToken}
                      onChange={(e: any) => setSettingsToken(e.target.value)}
                      placeholder={t('dadis.settings.tokenPlaceholder')}
                      size="sm"
                    />
                  </div>
                  <Button variant="primary" size="sm" onClick={handleSaveCredentials}>
                    {t('dadis.settings.save')}
                  </Button>
                </Stack>
              </div>
            ) : (
              <Button
                variant="secondary"
                size="sm"
                onClick={() => setShowSettings(true)}
              >
                <Settings className="w-4 h-4 mr-1.5" />
                {t('dadis.settings.configure')}
              </Button>
            )}
          </Stack>
        </Card>

        {/* Fallback sources */}
        <Card padding="md">
          <Stack gap="stack">
            <div className="flex items-center gap-2">
              <Globe className="w-4 h-4 text-nkz-accent-base" />
              <h3 className="text-nkz-md font-semibold text-nkz-text-primary">{t('dadis.fallback.title')}</h3>
            </div>
            <p className="text-nkz-sm text-nkz-text-secondary">{t('dadis.fallback.description')}</p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-nkz-stack">
              {[
                { name: 'GBIF Livestock', desc: t('dadis.fallback.sources.gbif') },
                { name: 'AGROVOC', desc: t('dadis.fallback.sources.agrovoc') },
                { name: 'EPPO', desc: t('dadis.fallback.sources.eppo') },
                { name: 'GlobalTreeSearch', desc: t('dadis.fallback.sources.globaltreesearch') },
                { name: 'EU Pesticides', desc: t('dadis.fallback.sources.pesticides') },
                { name: 'CPVO Varieties', desc: t('dadis.fallback.sources.cpvo') },
              ].map((src) => (
                <Card key={src.name} padding="sm">
                  <div className="flex items-start gap-2">
                    <CheckCircle className="w-3.5 h-3.5 text-nkz-success flex-shrink-0 mt-0.5" />
                    <div>
                      <p className="text-nkz-sm font-medium text-nkz-text-primary">{src.name}</p>
                      <p className="text-nkz-xs text-nkz-text-muted">{src.desc}</p>
                    </div>
                  </div>
                </Card>
              ))}
            </div>
          </Stack>
        </Card>
      </Stack>
    );
  }

  // ── Credentials available: search interface ─────────────────────────
  return (
    <Card padding="md">
      <Stack gap="stack">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Database className="w-4 h-4 text-nkz-accent-base" />
            <h2 className="text-nkz-md font-semibold text-nkz-text-primary">{t('dadis.title')}</h2>
            <Badge intent="positive">
              <CheckCircle className="w-3 h-3 mr-1" />
              {t('dadis.settings.connected')}
            </Badge>
          </div>
          <button
            onClick={() => setShowSettings(!showSettings)}
            aria-label="DAD-IS settings"
            className="w-7 h-7 inline-flex items-center justify-center rounded-nkz-md text-nkz-text-muted hover:bg-nkz-surface-sunken transition-colors duration-nkz-fast"
          >
            {showSettings ? <X className="w-4 h-4" /> : <Settings className="w-4 h-4" />}
          </button>
        </div>

        {/* Settings panel (collapsible) */}
        {showSettings && (
          <div className="bg-nkz-surface-sunken rounded-nkz-md p-nkz-stack">
            <Stack gap="stack">
              <h4 className="text-nkz-sm font-medium text-nkz-text-primary">
                {t('dadis.settings.title')}
              </h4>
              <div>
                <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                  {t('dadis.settings.apiUrl')}
                </label>
                <Input
                  value={settingsUrl}
                  onChange={(e: any) => setSettingsUrl(e.target.value)}
                  placeholder={DADIS_DEFAULT_URL}
                  size="sm"
                />
              </div>
              <div>
                <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                  {t('dadis.settings.apiToken')}
                </label>
                <Input
                  type="password"
                  value={settingsToken}
                  onChange={(e: any) => setSettingsToken(e.target.value)}
                  placeholder={t('dadis.settings.tokenPlaceholder')}
                  size="sm"
                />
              </div>
              <div className="flex gap-2">
                <Button variant="primary" size="sm" onClick={handleSaveCredentials}>
                  {t('dadis.settings.update')}
                </Button>
                <Button variant="danger" size="sm" onClick={handleClearCredentials}>
                  {t('dadis.settings.disconnect')}
                </Button>
              </div>
            </Stack>
          </div>
        )}

        {/* Filters */}
        {refLoading ? (
          <div className="flex items-center justify-center py-4">
            <Spinner size="sm" />
          </div>
        ) : (
          <div className="flex gap-3 items-end flex-wrap">
            <div className="min-w-[160px]">
              <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                {t('dadis.filters.country')}
              </label>
              <select
                className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
                value={selectedCountry}
                onChange={(e) => setSelectedCountry(e.target.value)}
              >
                <option value="">{t('dadis.filters.allCountries')}</option>
                {countries.map((c) => (
                  <option key={c.iso3 || c.name} value={c.iso3}>{c.name}</option>
                ))}
              </select>
            </div>
            <div className="min-w-[140px]">
              <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                {t('dadis.filters.species')}
              </label>
              <select
                className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
                value={selectedSpecies}
                onChange={(e) => setSelectedSpecies(e.target.value)}
              >
                <option value="">{t('dadis.filters.allSpecies')}</option>
                {species.map((s) => (
                  <option key={s.id} value={String(s.id)}>{s.name || s.id}</option>
                ))}
              </select>
            </div>
            <div className="min-w-[160px]">
              <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                {t('dadis.filters.classification')}
              </label>
              <select
                className="w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
                value={classification}
                onChange={(e) => setClassification(e.target.value as any)}
              >
                <option value="all">{t('dadis.filters.all')}</option>
                <option value="local">{t('dadis.filters.local')}</option>
                <option value="transboundary">{t('dadis.filters.transboundary')}</option>
              </select>
            </div>
            <Button
              variant="primary"
              size="sm"
              onClick={handleSearch}
              disabled={loading}
            >
              {loading ? (
                <Spinner size="sm" />
              ) : (
                <Search className="w-4 h-4 mr-1.5" />
              )}
              {loading ? t('dadis.searching') : t('dadis.search')}
            </Button>
          </div>
        )}

        {/* Error */}
        {error && (
          <Badge intent="negative" className="flex items-center gap-2">
            <span className="text-nkz-xs">{error}</span>
          </Badge>
        )}

        {/* Results */}
        {loading ? (
          <div className="flex items-center justify-center py-nkz-section">
            <Spinner size="md" />
          </div>
        ) : breeds.length > 0 ? (
          <div>
            <table className="w-full text-nkz-sm">
              <thead>
                <tr className="text-left text-nkz-xs font-semibold text-nkz-text-secondary uppercase tracking-wider border-b border-nkz-border">
                  <th className="px-nkz-stack py-nkz-inline">{t('dadis.table.breedName')}</th>
                  <th className="px-nkz-stack py-nkz-inline">{t('dadis.table.speciesId')}</th>
                  <th className="px-nkz-stack py-nkz-inline">{t('dadis.table.country')}</th>
                  <th className="px-nkz-stack py-nkz-inline">{t('dadis.table.transboundary')}</th>
                </tr>
              </thead>
              <tbody>
                {breeds.map((breed, idx) => (
                  <tr
                    key={`${breed.breedId}-${idx}`}
                    className="border-b border-nkz-border transition-colors duration-nkz-fast hover:bg-nkz-surface-sunken cursor-pointer"
                  >
                    <td className="px-nkz-stack py-nkz-inline font-medium text-nkz-text-primary">
                      {breed.breedName}
                    </td>
                    <td className="px-nkz-stack py-nkz-inline">
                      <Badge intent="info">{breed.speciesId}</Badge>
                    </td>
                    <td className="px-nkz-stack py-nkz-inline text-nkz-text-muted">
                      {breed.countryISO3}
                    </td>
                    <td className="px-nkz-stack py-nkz-inline">
                      {breed.transboundaryId ? (
                        <span className="font-mono text-nkz-xs text-nkz-accent-base">
                          {breed.transboundaryId}
                        </span>
                      ) : (
                        <span className="text-nkz-text-muted">—</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="text-nkz-xs text-nkz-text-muted text-right mt-2">
              {t('dadis.footer', { count: breeds.length })}
            </p>
          </div>
        ) : !error && !refLoading ? (
          <div className="flex flex-col items-center justify-center py-nkz-section text-center">
            <Database className="w-8 h-8 text-nkz-text-muted mb-nkz-stack" />
            <p className="text-nkz-sm text-nkz-text-primary font-medium">{t('dadis.empty.title')}</p>
            <p className="text-nkz-xs text-nkz-text-muted">{t('dadis.empty.hint')}</p>
          </div>
        ) : null}
      </Stack>
    </Card>
  );
};
