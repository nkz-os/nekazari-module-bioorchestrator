import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Card, Stack, Button, Badge, Panel, Surface, IconButton, DataTable } from '@nekazari/ui-kit';
import { Database, Settings, X, Globe, CheckCircle, ExternalLink } from 'lucide-react';
import { getDadisCredentials } from '../../services/api';

const DADIS_DEFAULT_URL = 'https://us-central1-fao-dadis-dev.cloudfunctions.net/api/v1';

export const BreedDiscovery: React.FC = () => {
  const { t } = useTranslation('bioorchestrator');
  const hasCredentials = !!getDadisCredentials();

  if (!hasCredentials) {
    return (
      <Stack gap="section">
        <Panel>
          <Panel.Header><Panel.Title><Database className="w-4 h-4 text-nkz-accent-base" />{t('dadis.title')}</Panel.Title></Panel.Header>
          <Panel.Body>
            <Stack gap="stack">
              <Surface variant="sunken" padding="stack">
                <div className="flex items-start gap-3">
                  <ExternalLink className="w-4 h-4 text-nkz-warning flex-shrink-0 mt-0.5" />
                  <div className="text-nkz-sm">
                    <p className="font-medium mb-1">{t('dadis.settings.description')}</p>
                    <a href="https://www.fao.org/dad-is/en/" target="_blank" rel="noopener" className="inline-flex items-center gap-1 text-nkz-accent-base text-nkz-xs hover:underline">
                      {t('dadis.settings.requestAccess')} <ExternalLink className="w-3 h-3" />
                    </a>
                  </div>
                </div>
              </Surface>
              <Button variant="secondary" size="sm"><Settings className="w-4 h-4 mr-1.5" />{t('dadis.settings.configure')}</Button>
            </Stack>
          </Panel.Body>
        </Panel>

        <Panel>
          <Panel.Header><Panel.Title><Globe className="w-4 h-4 text-nkz-accent-base" />{t('dadis.fallback.title')}</Panel.Title></Panel.Header>
          <Panel.Body>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {['GBIF Livestock', 'AGROVOC', 'EPPO', 'GlobalTreeSearch', 'EU Pesticides', 'CPVO Varieties'].map((name) => (
                <Card key={name} padding="sm">
                  <div className="flex items-start gap-2">
                    <CheckCircle className="w-3.5 h-3.5 text-nkz-success flex-shrink-0 mt-0.5" />
                    <p className="text-nkz-sm font-medium">{name}</p>
                  </div>
                </Card>
              ))}
            </div>
          </Panel.Body>
        </Panel>
      </Stack>
    );
  }

  // Test DataTable with credentials
  const testData = [{ breedName: 'Test', breedId: '1', speciesId: 123, countryISO3: 'ESP', transboundaryId: null }];
  const columns = [
    { accessorKey: 'breedName', header: 'Breed' },
    { accessorKey: 'speciesId', header: 'Species ID' },
    { accessorKey: 'countryISO3', header: 'Country' },
  ];

  return (
    <Panel>
      <Panel.Header>
        <Panel.Title><Database className="w-4 h-4 text-nkz-accent-base" />{t('dadis.title')}</Panel.Title>
        <Panel.Actions>
          <Badge intent="positive"><CheckCircle className="w-3 h-3 mr-1" />Connected</Badge>
          <IconButton aria-label="Settings" size="sm" variant="ghost"><Settings className="w-4 h-4" /></IconButton>
        </Panel.Actions>
      </Panel.Header>
      <Panel.Body>
        <DataTable columns={columns} data={testData} />
      </Panel.Body>
    </Panel>
  );
};
