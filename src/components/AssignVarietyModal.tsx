import React, { useState, useEffect } from "react";
import { useTranslation } from '@nekazari/sdk';
import { Button, Card, FormField, FormGrid, Inline, Input, Stack } from '@nekazari/ui-kit';
import { assignCrop, AssignCropRequest } from "../services/api";
import { useParcelContext } from "../context/ParcelContext";

export interface VarietyInfo {
  name: string;
  scientificName?: string;
  cropEppo?: string;
  cropUri: string;
  varietyUri: string;
  expectedYield: number;
  confidenceInterval: [number, number];
  trialCount: number;
  /** Unit of the two yield figures above, as displayed; kg/ha when absent. */
  yieldUnit?: string;
}

interface Props {
  variety: VarietyInfo;
  parcelId?: string;
  onClose: () => void;
  onAssigned: (parcelId: string) => void;
}

export default function AssignVarietyModal({ variety, parcelId: propParcelId, onClose, onAssigned }: Props) {
  const { t } = useTranslation('bioorchestrator');
  const { selectedParcel: ctxParcelId } = useParcelContext();
  const selectedParcel = propParcelId || ctxParcelId;
  const [management, setManagement] = useState<"conventional" | "organic">("conventional");
  const [seasonStart, setSeasonStart] = useState("");
  const [seasonEnd, setSeasonEnd] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    // Default season: current year for start, next year for end
    const now = new Date();
    const y = now.getFullYear();
    if (!seasonStart) setSeasonStart(`${y}-10-15`);
    if (!seasonEnd) setSeasonEnd(`${y + 1}-06-30`);
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const handleAssign = async () => {
    if (!selectedParcel) {
      setError(t("assign.noParcels", { defaultValue: "No parcel selected" }));
      return;
    }
    setLoading(true);
    setError("");
    try {
      const payload: AssignCropRequest = {
        parcel_id: selectedParcel,
        variety_uri: variety.varietyUri,
        crop_uri: variety.cropUri,
        management,
        season_start: seasonStart,
        season_end: seasonEnd,
      };
      await assignCrop(payload);
      onAssigned(selectedParcel);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={onClose}>
      <div
        role="dialog"
        aria-modal="true"
        aria-label={t("assign.title")}
        className="w-full max-w-lg max-h-[80vh] overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        <Card padding="lg">
          <Stack gap="stack">
            <h2 className="text-nkz-lg font-semibold text-nkz-text-primary">{t("assign.title")}</h2>

            <div className="rounded-nkz-md bg-nkz-surface-sunken p-nkz-inline">
              <Stack gap="tight">
                <span className="text-nkz-base font-semibold text-nkz-text-primary">{variety.name}</span>
                {variety.scientificName && (
                  <span className="text-nkz-sm italic text-nkz-text-muted">{variety.scientificName}</span>
                )}
                <span className="text-nkz-sm text-nkz-text-secondary">
                  {t("assign.expectedYield")}: {variety.expectedYield.toLocaleString()} {variety.yieldUnit ?? "kg/ha"}
                  {" ["}
                  {variety.confidenceInterval[0].toLocaleString()} –{" "}
                  {variety.confidenceInterval[1].toLocaleString()}
                  {"]"}
                </span>
                <span className="text-nkz-xs text-nkz-text-muted">
                  {variety.trialCount} {t("assign.trialsLabel")}
                </span>
              </Stack>
            </div>

            <FormField label={t("assign.parcelLabel")}>
              <div className="rounded-nkz-md bg-nkz-surface-sunken p-nkz-inline text-nkz-sm text-nkz-text-primary break-all">
                {selectedParcel || t("assign.noParcelSelected", { defaultValue: "No parcel selected — please select one in the platform" })}
              </div>
            </FormField>

            <FormField label={t("assign.managementLabel")}>
              <Inline gap="inline" role="radiogroup" aria-label={t("assign.managementLabel")}>
                {(["conventional", "organic"] as const).map((m) => (
                  <Button
                    key={m}
                    size="sm"
                    role="radio"
                    aria-checked={management === m}
                    variant={management === m ? "primary" : "secondary"}
                    onClick={() => setManagement(m)}
                  >
                    {t(`assign.${m}`)}
                  </Button>
                ))}
              </Inline>
            </FormField>

            {management === "organic" && (
              <div role="note" className="rounded-nkz-md border border-nkz-warning bg-nkz-surface-sunken p-nkz-inline">
                <Stack gap="tight">
                  <span className="text-nkz-sm font-semibold text-nkz-warning">{t("assign.organicWarningTitle")}</span>
                  <span className="text-nkz-sm text-nkz-text-secondary">{t("assign.organicWarningBody")}</span>
                </Stack>
              </div>
            )}

            <Stack gap="tight">
              <span className="text-nkz-sm font-medium text-nkz-text-primary">{t("assign.seasonLabel")}</span>
              <FormGrid columns={2}>
                <FormField label={t("assign.sowingDate")}>
                  <Input type="date" value={seasonStart} onChange={(e) => setSeasonStart(e.target.value)} />
                </FormField>
                <FormField label={t("assign.harvestDate")}>
                  <Input type="date" value={seasonEnd} onChange={(e) => setSeasonEnd(e.target.value)} />
                </FormField>
              </FormGrid>
            </Stack>

            {error && <p role="alert" className="text-nkz-sm text-nkz-danger">{error}</p>}

            <Inline gap="inline" justify="end">
              <Button variant="secondary" onClick={onClose} disabled={loading}>
                {t("assign.cancel")}
              </Button>
              <Button variant="primary" onClick={handleAssign} loading={loading} disabled={loading || !selectedParcel}>
                {t("assign.confirm")}
              </Button>
            </Inline>
          </Stack>
        </Card>
      </div>
    </div>
  );
}
