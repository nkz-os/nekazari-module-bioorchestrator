import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Input, Stack, Surface } from '@nekazari/ui-kit';
import { CheckCircle, XCircle, X, Upload } from 'lucide-react';
import { useBioApi } from '../services/api';

interface Props { onClose: () => void; }

// ── Inline field wrapper (replaces FormField) ───────────────────────────

const Field: React.FC<{
  label: string;
  required?: boolean;
  children: React.ReactNode;
}> = ({ label, required, children }) => (
  <div>
    <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">
      {label}{required ? ' *' : ''}
    </label>
    {children}
  </div>
);

// ── Inline form grid (replaces FormGrid) ───────────────────────────────

const Grid2: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div className="grid grid-cols-2 gap-nkz-stack">{children}</div>
);

// ── Main component ──────────────────────────────────────────────────────

const PhenologyContribute: React.FC<Props> = ({ onClose }) => {
  const { t } = useTranslation('bioorchestrator');
  const api = useBioApi();
  const [species, setSpecies] = useState('');
  const [stage, setStage] = useState('');
  const [cultivar, setCultivar] = useState('');
  const [kc, setKc] = useState('');
  const [d1, setD1] = useState('');
  const [d2, setD2] = useState('');
  const [mdsRef, setMdsRef] = useState('');
  const [doi, setDoi] = useState('');
  const [author, setAuthor] = useState('');
  const [conditions, setConditions] = useState('');
  const [email, setEmail] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!species || !stage || !kc) return;

    setSubmitting(true);
    setResult(null);
    try {
      const params = new URLSearchParams({ species, stage, kc });
      if (d1) params.set('d1', d1);
      if (d2) params.set('d2', d2);
      if (mdsRef) params.set('mds_ref', mdsRef);
      if (cultivar) params.set('cultivar', cultivar);
      if (doi) params.set('doi', doi);
      if (author) params.set('author', author);
      if (conditions) params.set('conditions', conditions);
      if (email) params.set('contact_email', email);

      await api.contributePhenology(params);
      setResult('success');
      setTimeout(onClose, 2000);
    } catch (e: any) {
      setResult(e.message || 'Unknown error');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/30"
      onClick={onClose}
    >
      <div
        className="w-full max-w-lg mx-4 max-h-[90vh] overflow-auto bg-nkz-surface rounded-nkz-xl shadow-nkz-lg"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-nkz-stack py-nkz-inline border-b border-nkz-border">
          <h3 className="text-nkz-md font-semibold text-nkz-text-primary">
            {t('phenology.contribute.title')}
          </h3>
          <button
            onClick={onClose}
            className="w-7 h-7 inline-flex items-center justify-center rounded-nkz-md text-nkz-text-muted hover:bg-nkz-surface-sunken transition-colors duration-nkz-fast"
            aria-label="Close"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {result === 'success' ? (
          <div className="p-nkz-section text-center">
            <CheckCircle className="w-10 h-10 text-nkz-success mx-auto mb-nkz-stack" />
            <p className="text-nkz-base text-nkz-text-primary font-medium">
              {t('phenology.contribute.success')}
            </p>
          </div>
        ) : (
          <form onSubmit={handleSubmit} className="p-nkz-stack">
            <Stack gap="stack">
              <p className="text-nkz-sm text-nkz-text-secondary">
                {t('phenology.contribute.description')}
              </p>

              {/* Main parameters */}
              <Grid2>
                <Field label={t('phenology.species')} required>
                  <Input
                    value={species}
                    onChange={(e: any) => setSpecies(e.target.value)}
                    placeholder="e.g. olive, almond"
                    size="sm"
                    required
                  />
                </Field>
                <Field label={t('phenology.stage')} required>
                  <Input
                    value={stage}
                    onChange={(e: any) => setStage(e.target.value)}
                    placeholder="e.g. vegetative, pit_hardening"
                    size="sm"
                    required
                  />
                </Field>
                <Field label={t('phenology.cultivar')}>
                  <Input
                    value={cultivar}
                    onChange={(e: any) => setCultivar(e.target.value)}
                    placeholder="e.g. Picual, Arbequina"
                    size="sm"
                  />
                </Field>
                <Field label="Kc" required>
                  <Input
                    type="number"
                    step="0.01"
                    min={0}
                    max={2}
                    value={kc}
                    onChange={(e: any) => setKc(e.target.value)}
                    placeholder="0.85"
                    size="sm"
                    required
                  />
                </Field>
                <Field label="D1 (NWSB) °C">
                  <Input
                    type="number"
                    step="0.1"
                    value={d1}
                    onChange={(e: any) => setD1(e.target.value)}
                    placeholder="2.0"
                    size="sm"
                  />
                </Field>
                <Field label="D2 (Max Stress) °C">
                  <Input
                    type="number"
                    step="0.1"
                    value={d2}
                    onChange={(e: any) => setD2(e.target.value)}
                    placeholder="8.0"
                    size="sm"
                  />
                </Field>
                <Field label="MDS Ref (µm)">
                  <Input
                    type="number"
                    step="1"
                    value={mdsRef}
                    onChange={(e: any) => setMdsRef(e.target.value)}
                    placeholder="150"
                    size="sm"
                  />
                </Field>
              </Grid2>

              {/* Provenance */}
              <Surface variant="sunken" padding="stack">
                <Stack gap="stack">
                  <h4 className="text-nkz-sm font-medium text-nkz-text-primary">
                    {t('phenology.contribute.provenance')}
                  </h4>
                  <Grid2>
                    <Field label="DOI">
                      <Input
                        value={doi}
                        onChange={(e: any) => setDoi(e.target.value)}
                        placeholder="10.1234/example"
                        size="sm"
                      />
                    </Field>
                    <Field label={t('phenology.contribute.author')}>
                      <Input
                        value={author}
                        onChange={(e: any) => setAuthor(e.target.value)}
                        placeholder="e.g. Orgaz, Fereres"
                        size="sm"
                      />
                    </Field>
                    <Field label="Email">
                      <Input
                        type="email"
                        value={email}
                        onChange={(e: any) => setEmail(e.target.value)}
                        placeholder="investigador@csic.es"
                        size="sm"
                      />
                    </Field>
                  </Grid2>
                  <Field label={t('phenology.contribute.conditions')}>
                    <textarea
                      value={conditions}
                      onChange={(e) => setConditions(e.target.value)}
                      placeholder="e.g. Olive, cv. Picual, Córdoba, deficit irrigation 30% ETc, 4x6m spacing"
                      rows={3}
                      className="w-full rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack py-nkz-tight text-nkz-sm text-nkz-text-primary placeholder:text-nkz-text-muted resize-y focus-visible:ring-2 focus-visible:ring-nkz-accent-base"
                    />
                  </Field>
                </Stack>
              </Surface>

              {/* Error */}
              {result && result !== 'success' && (
                <div className="flex items-center gap-2 text-nkz-sm text-nkz-danger bg-nkz-danger-soft rounded-nkz-md px-nkz-stack py-nkz-inline border border-nkz-danger">
                  <XCircle className="w-4 h-4 flex-shrink-0" />
                  {result}
                </div>
              )}

              {/* Actions */}
              <div className="flex justify-end gap-2">
                <Button variant="ghost" size="sm" onClick={onClose} type="button">
                  {t('phenology.contribute.cancel')}
                </Button>
                <Button
                  variant="primary"
                  size="sm"
                  type="submit"
                  disabled={submitting}
                  leadingIcon={submitting ? undefined : <Upload className="w-4 h-4" />}
                  loading={submitting}
                >
                  {submitting ? t('phenology.contribute.submitting') || '...' : t('phenology.contribute.submit')}
                </Button>
              </div>
            </Stack>
          </form>
        )}
      </div>
    </div>
  );
};

export default PhenologyContribute;
