import React, { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { assignCrop, AssignCropRequest } from "../services/api";

interface Parcel {
  id: string;
  name: string;
}

interface VarietyInfo {
  name: string;
  scientificName?: string;
  cropEppo: string;
  cropUri: string;
  varietyUri: string;
  expectedYield: number;
  confidenceInterval: [number, number];
  trialCount: number;
}

interface Props {
  variety: VarietyInfo;
  onClose: () => void;
  onAssigned: (parcelId: string) => void;
}

export default function AssignVarietyModal({ variety, onClose, onAssigned }: Props) {
  const { t } = useTranslation();
  const [parcels, setParcels] = useState<Parcel[]>([]);
  const [selectedParcel, setSelectedParcel] = useState("");
  const [management, setManagement] = useState<"conventional" | "organic">("conventional");
  const [seasonStart, setSeasonStart] = useState("");
  const [seasonEnd, setSeasonEnd] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    const sdk = (window as any).__NKZ_SDK__;
    if (sdk?.getParcels) {
      sdk.getParcels().then((p: Parcel[]) => setParcels(p));
    }
  }, []);

  const handleAssign = async () => {
    if (!selectedParcel) {
      setError(t("assign.noParcels"));
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

  const organicTrials = 37;
  const conventionalTrials = variety.trialCount;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        className="modal-content"
        onClick={(e) => e.stopPropagation()}
        style={{ maxWidth: 520, padding: 24 }}
      >
        <h2>{t("assign.title")}</h2>

        <div
          style={{
            marginBottom: 16,
            padding: 12,
            background: "#f5f5f5",
            borderRadius: 8,
          }}
        >
          <strong>{variety.name}</strong>
          {variety.scientificName && (
            <div style={{ fontSize: 13, color: "#666" }}>
              {variety.scientificName}
            </div>
          )}
          <div style={{ marginTop: 4 }}>
            {t("assign.expectedYield")}: {variety.expectedYield.toLocaleString()} kg/ha
            {" ["}
            {variety.confidenceInterval[0].toLocaleString()} –{" "}
            {variety.confidenceInterval[1].toLocaleString()}
            {"]"}
          </div>
          <div style={{ fontSize: 13, color: "#666" }}>
            {variety.trialCount} {t("assign.trialsLabel")}
          </div>
        </div>

        <div style={{ marginBottom: 16 }}>
          <label>{t("assign.parcelLabel")}</label>
          <select
            value={selectedParcel}
            onChange={(e) => setSelectedParcel(e.target.value)}
            style={{ width: "100%", padding: 8, marginTop: 4 }}
          >
            <option value="">{t("assign.parcelPlaceholder")}</option>
            {parcels.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        </div>

        <div style={{ marginBottom: 16 }}>
          <label>{t("assign.managementLabel")}</label>
          <div style={{ display: "flex", gap: 12, marginTop: 4 }}>
            <label>
              <input
                type="radio"
                value="conventional"
                checked={management === "conventional"}
                onChange={() => setManagement("conventional")}
              />
              {t("assign.conventional")}
            </label>
            <label>
              <input
                type="radio"
                value="organic"
                checked={management === "organic"}
                onChange={() => setManagement("organic")}
              />
              {t("assign.organic")}
            </label>
          </div>
        </div>

        {management === "organic" && (
          <div
            style={{
              marginBottom: 16,
              padding: 12,
              background: "#fff3cd",
              borderRadius: 8,
              fontSize: 13,
            }}
          >
            <strong>⚠️ {t("assign.organicWarningTitle")}</strong>
            <p style={{ margin: "4px 0 0 0" }}>
              {t("assign.organicWarningBody", {
                organicTrials,
                conventionalTrials,
                pct: 18,
              })}
            </p>
          </div>
        )}

        <div style={{ marginBottom: 16 }}>
          <label>{t("assign.seasonLabel")}</label>
          <div style={{ display: "flex", gap: 12, marginTop: 4 }}>
            <div>
              <div style={{ fontSize: 12, color: "#666" }}>
                {t("assign.sowingDate")}
              </div>
              <input
                type="date"
                value={seasonStart}
                onChange={(e) => setSeasonStart(e.target.value)}
                style={{ padding: 6 }}
              />
            </div>
            <div>
              <div style={{ fontSize: 12, color: "#666" }}>
                {t("assign.harvestDate")}
              </div>
              <input
                type="date"
                value={seasonEnd}
                onChange={(e) => setSeasonEnd(e.target.value)}
                style={{ padding: 6 }}
              />
            </div>
          </div>
        </div>

        {error && <div style={{ color: "red", marginBottom: 12 }}>{error}</div>}

        <div style={{ display: "flex", justifyContent: "flex-end", gap: 12 }}>
          <button onClick={onClose} disabled={loading}>
            {t("assign.cancel")}
          </button>
          <button
            onClick={handleAssign}
            disabled={loading || !selectedParcel}
            style={{
              background: "#4caf50",
              color: "white",
              border: "none",
              padding: "8px 16px",
              borderRadius: 4,
            }}
          >
            {loading ? "..." : t("assign.confirm")}
          </button>
        </div>
      </div>
    </div>
  );
}
