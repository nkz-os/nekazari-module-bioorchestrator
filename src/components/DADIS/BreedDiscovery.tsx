import React, { useState, useEffect } from 'react';
import { useTranslation } from '@nekazari/sdk';
import {
  Panel, Stack, Button, Input, Badge, Spinner,
  Surface, IconButton, Card,
} from '@nekazari/ui-kit';
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

interface DadisCountry {
  iso3: string;
  name: string;
}

interface DadisSpecies {
  id: number;
  name: string;
}

const DADIS_DEFAULT_URL = 'https://us-central1-fao-dadis-dev.cloudfunctions.net/api/v1';

export const BreedDiscovery: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();

  // DAD-IS credentials state
  const [hasCredentials, setHasCredentials] = useState<boolean>(() => !!getDadisCredentials());
  const [showSettings, setShowSettings] = useState(false);
  const [settingsUrl, setSettingsUrl] = useState('');
  const [settingsToken, setSettingsToken] = useState('');

  // Reference data
  const [countries, setCountries] = useState<DadisCountry[]>([]);
  const [species, setSpecies] = useState<DadisSpecies[]>([]);
  const [refLoading, setRefLoading] = useState(false);

  // Search
  const [breeds, setBreeds] = useState<DadisBreed[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Filters
  const [selectedCountry, setSelectedCountry] = useState<string>('');
  const [selectedSpecies, setSelectedSpecies] = useState<string>('');
  const [classification, setClassification] = useState<'all' | 'local' | 'transboundary'>('all');

  // Load reference data when credentials are available
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

  // ── No credentials: setup screen ──────────────────────────────────────
  if (!hasCredentials) {
    return (
      <Stack gap="section">
        {/* Setup form */}
        <Panel>
          <Panel.Header>
            <Panel.Title>
              <Database className="w-4 h-4 text-nkz-accent-base" />
              {t('dadis.title')}
            </Panel.Title>
          </Panel.Header>
          <Panel.Body>
            <Stack gap="stack">
              <div className="flex items-start gap-3 rounded-nkz-md bg-nkz-warning-soft border border-nkz-warning p-nkz-stack">
                <ExternalLink className="w-4 h-4 text-nkz-warning flex-shrink-0 mt-0.5" />
                <div className="text-nkz-sm text-nkz-text-primary">
                  <p className="font-medium mb-1">FAO DAD-IS API — Commercial Use Restriction</p>
                  <p className="text-nkz-text-secondary">
                    The FAO DAD-IS API cannot be used for commercial purposes under its current terms.
                    Each user must provide their own API credentials obtained directly from FAO.
                  </p>
                  <a
                    href="https://www.fao.org/dad-is/en/"
                    target="_blank"
                    rel="noopener"
                    className="inline-flex items-center gap-1 text-nkz-accent-base text-nkz-xs mt-2 hover:underline"
                  >
                    Request API access <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
              </div>

              {showSettings ? (
                <Surface variant="sunken" padding="stack">
                  <Stack gap="stack">
                    <div className="flex items-center justify-between">
                      <h4 className="text-nkz-sm font-medium text-nkz-text-primary">
                        DAD-IS API Configuration
                      </h4>
                      <IconButton
                        aria-label="Close settings"
                        size="sm"
                        variant="ghost"
                        onClick={() => setShowSettings(false)}
                      >
                        <X className="w-4 h-4" />
                      </IconButton>
                    </div>
                    <div>
                      <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                        API URL
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
                        API Token
                      </label>
                      <Input
                        type="password"
                        value={settingsToken}
                        onChange={(e: any) => setSettingsToken(e.target.value)}
                        placeholder="Enter your DAD-IS API token..."
                        size="sm"
                      />
                    </div>
                    <div className="flex gap-2">
                      <Button variant="primary" size="sm" onClick={handleSaveCredentials}>
                        Save & Connect
                      </Button>
                    </div>
                  </Stack>
                </Surface>
              ) : (
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => setShowSettings(true)}
                  leadingIcon={<Settings className="w-4 h-4" />}
                >
                  Configure DAD-IS API
                </Button>
              )}
            </Stack>
          </Panel.Body>
        </Panel>

        {/* Fallback sources */}
        <Panel>
          <Panel.Header>
            <Panel.Title>
              <Globe className="w-4 h-4 text-nkz-accent-base" />
              Available Data Sources (without DAD-IS)
            </Panel.Title>
          </Panel.Header>
          <Panel.Body>
            <Stack gap="stack">
              <p className="text-nkz-sm text-nkz-text-secondary">
                The BioOrchestrator integrates multiple biodiversity data sources via IkerKeta
                that are available without DAD-IS credentials:
              </p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-nkz-stack">
                {[
                  { name: 'GBIF Livestock', desc: 'Global livestock occurrence records via GBIF-mediated data' },
                  { name: 'AGROVOC', desc: 'FAO agricultural thesaurus — species, breeds, and practices' },
                  { name: 'EPPO', desc: 'European plant protection organisation — pest and disease data' },
                  { name: 'GlobalTreeSearch', desc: 'Botanic Gardens Conservation International — tree species' },
                  { name: 'EU Pesticides', desc: 'European Commission — authorised plant protection products' },
                  { name: 'CPVO Varieties', desc: 'Community Plant Variety Office — registered crop varieties' },
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
          </Panel.Body>
        </Panel>
      </Stack>
    );
  }

  // ── Credentials available: search interface ───────────────────────────
  return (
    <Panel>
      <Panel.Header>
        <Panel.Title>
          <Database className="w-4 h-4 text-nkz-accent-base" />
          {t('dadis.title')}
        </Panel.Title>
        <Panel.Actions>
          <Badge intent="positive">
            <span className="flex items-center gap-1">
              <CheckCircle className="w-3 h-3" />
              DAD-IS Connected
            </span>
          </Badge>
          <IconButton
            aria-label="DAD-IS settings"
            size="sm"
            variant="ghost"
            onClick={() => setShowSettings(!showSettings)}
          >
            {showSettings ? <X className="w-4 h-4" /> : <Settings className="w-4 h-4" />}
          </IconButton>
        </Panel.Actions>
      </Panel.Header>

      <Panel.Body>
        <Stack gap="stack">
          {/* Settings panel (collapsible) */}
          {showSettings && (
            <Surface variant="sunken" padding="stack">
              <Stack gap="stack">
                <h4 className="text-nkz-sm font-medium text-nkz-text-primary">
                  DAD-IS API Configuration
                </h4>
                <div>
                  <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
                    API URL
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
                    API Token
                  </label>
                  <Input
                    type="password"
                    value={settingsToken}
                    onChange={(e: any) => setSettingsToken(e.target.value)}
                    placeholder="Enter new token..."
                    size="sm"
                  />
                </div>
                <div className="flex gap-2">
                  <Button variant="primary" size="sm" onClick={handleSaveCredentials}>
                    Update Credentials
                  </Button>
                  <Button variant="danger" size="sm" onClick={handleClearCredentials}>
                    Disconnect
                  </Button>
                </div>
              </Stack>
            </Surface>
          )}

          {/* Filters */}
          {refLoading ? (
            <div className="flex gap-3">
              <div className="flex items-center justify-center py-4 w-full">
                <Spinner size="sm" />
              </div>
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
                leadingIcon={loading ? undefined : <Search className="w-4 h-4" />}
                loading={loading}
              >
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
            <Stack gap="stack">
              <div className="flex items-center justify-center py-nkz-section">
                <Spinner size="md" />
              </div>
            </Stack>
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
      </Panel.Body>
    </Panel>
  );
};
