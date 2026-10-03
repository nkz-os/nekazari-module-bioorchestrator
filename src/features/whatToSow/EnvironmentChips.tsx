import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, Inline, Stack } from '@nekazari/ui-kit';
import type { ParcelEnvironment } from '../../types/recommend';
import KoppenPicker from './KoppenPicker';
import { knownText } from './pageModel';

interface EnvironmentChipsProps {
  environment: ParcelEnvironment;
  /** Class actually used (override or the parcel's own). */
  climateClass: string | null;
  /** Irrigation regime actually used (override or the parcel's inferred one). */
  irrigationRegime: string | null;
  onClimateChange: (code: string) => void;
}

/** What the recommendation is based on: climate, soil texture, irrigation and area. */
export default function EnvironmentChips({ environment, climateClass, irrigationRegime, onClimateChange }: EnvironmentChipsProps) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const [picking, setPicking] = useState(false);
  const noData = t('whatToSow.noData');

  const climate = knownText(climateClass);
  const climateLabel = climate
    ? `${climate} · ${t(`whatToSow.koppen.${climate}`, { defaultValue: climate })}`
    : noData;
  const texture = knownText(environment.soil?.texture ?? null) ?? noData;
  const irrigation = knownText(irrigationRegime);
  const area = environment.area_ha;

  return (
    <Stack gap="tight">
      <Inline gap="inline" wrap align="center">
        <Button
          variant="secondary"
          size="sm"
          aria-expanded={picking}
          onClick={() => setPicking((p) => !p)}
        >
          {t('whatToSow.env.climate')}: {climateLabel}
        </Button>
        <Badge intent="default">{t('whatToSow.env.soil')}: {texture}</Badge>
        <Badge intent="default">
          {t('whatToSow.env.irrigation')}: {irrigation ? t(`whatToSow.irrigation.${irrigation}`, { defaultValue: irrigation }) : noData}
        </Badge>
        <Badge intent="default">
          {t('whatToSow.env.area')}: {area != null ? `${area.toLocaleString(i18n.language, { maximumFractionDigits: 1 })} ha` : noData}
        </Badge>
      </Inline>
      {picking && (
        <KoppenPicker
          value={climate}
          onChange={(code) => {
            setPicking(false);
            onClimateChange(code);
          }}
        />
      )}
    </Stack>
  );
}
