import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Select, Stack } from '@nekazari/ui-kit';
import { saveDeclaredCrop, saveDeclaredTexture, saveParcelRegime, USDA_TEXTURE_CLASSES } from '../api';
import type { UsdaTextureClass } from '../api';
import { anyCompletion, attemptWrite, completionNeeds, declarableCrops } from '../completionModel';
import { cropName } from '../format';
import type { WriteKind } from '../completionModel';
import type { FitResponse, ParcelRow, Regime } from '../types';

interface Props {
  fit: FitResponse;
  row: Pick<ParcelRow, 'parcel_id' | 'regime' | 'regime_source'>;
  /** A fact was saved: the fit is read again. */
  onSaved: () => void;
}

/**
 * One control per missing fact, above the recommended crops. Each writes to the module that owns the fact
 * and then asks for the fit again; a refused write says so and leaves the list as it is.
 */
export default function CompletionBar({ fit, row, onSaved }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const base = 'planning.wizard.crop.complete';
  const needs = completionNeeds(fit, row);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // "No lo sé": the control is put away for this visit; nothing is saved.
  const [skipped, setSkipped] = useState<ReadonlySet<string>>(new Set());
  const skip = (key: string) => setSkipped((prev) => new Set(prev).add(key));
  const history = needs.history.filter((c) => !skipped.has(`history:${c}`));
  const showRegime = needs.regime;
  const showSoil = needs.soil && !skipped.has('soil');
  if (!anyCompletion({ regime: showRegime, soil: showSoil, history })) return null;

  const run = async (kind: WriteKind, write: () => Promise<unknown>) => {
    setBusy(true);
    setError(null);
    const outcome = await attemptWrite(kind, write);
    setBusy(false);
    if (outcome.ok) onSaved();
    else setError(outcome.errorKey);
  };

  const crops = declarableCrops(fit.species);
  const cropOptions = crops.map((s) => ({ value: s.eppo as string, label: cropName(s.names, s.species, lang) }));
  const textureOptions = USDA_TEXTURE_CLASSES.map((c) => ({ value: c, label: t(`planning.texture.${c}`) }));
  const regimes: Regime[] = ['rainfed', 'irrigated'];

  return (
    <section aria-label={t(`${base}.title`)} className="rounded-nkz-md border border-nkz-border p-3">
      <Stack gap="stack">
        <h4 className="text-nkz-sm font-semibold text-nkz-text-primary">{t(`${base}.title`)}</h4>
        {showRegime && (
          <Stack gap="tight">
            <p className="text-nkz-sm text-nkz-text-primary">{t(`${base}.regime.label`)}</p>
            <div className="flex flex-wrap items-center gap-2">
              {regimes.map((r) => (
                <Button key={r} size="sm" variant="secondary" disabled={busy}
                  onClick={() => run('regime', () => saveParcelRegime(row.parcel_id, r))}>{t(`planning.regime.${r}`)}</Button>
              ))}
            </div>
            <p className="text-nkz-xs text-nkz-text-muted">{t(`${base}.regime.why`)}</p>
          </Stack>
        )}
        {showSoil && (
          <Stack gap="tight">
            <p className="text-nkz-sm text-nkz-text-primary">{t(`${base}.soil.label`)}</p>
            <div className="flex flex-wrap items-center gap-2">
              <Select value="" size="sm" placeholder={t(`${base}.soil.placeholder`)} options={textureOptions} disabled={busy}
                onValueChange={(v) => v && run('soil', () => saveDeclaredTexture(row.parcel_id, v as UsdaTextureClass))} />
              <Button size="sm" variant="ghost" disabled={busy} onClick={() => skip('soil')}>{t(`${base}.unknown`)}</Button>
            </div>
            <p className="text-nkz-xs text-nkz-text-muted">{t(`${base}.soil.why`)}</p>
          </Stack>
        )}
        {history.length > 0 && (
          <Stack gap="tight">
            <p className="text-nkz-sm text-nkz-text-primary">{t(`${base}.history.label`)}</p>
            {history.map((campaign) => (
              <div key={campaign} className="flex flex-wrap items-center gap-2">
                <span className="text-nkz-sm text-nkz-text-muted">{t(`${base}.history.campaign`, { campaign })}</span>
                <Select value="" size="sm" placeholder={t(`${base}.history.placeholder`)} options={cropOptions} disabled={busy}
                  onValueChange={(v) => v && run('history', () => saveDeclaredCrop(row.parcel_id, campaign, v))} />
                <Button size="sm" variant="ghost" disabled={busy} onClick={() => skip(`history:${campaign}`)}>{t(`${base}.unknown`)}</Button>
              </div>
            ))}
            <p className="text-nkz-xs text-nkz-text-muted">{t(`${base}.history.why`)}</p>
          </Stack>
        )}
        {error && <p role="alert" className="text-nkz-sm text-nkz-danger">{t(`${base}.error.${error}`)}</p>}
      </Stack>
    </section>
  );
}
