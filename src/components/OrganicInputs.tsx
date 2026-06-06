import React, { useState, useEffect } from "react";
import { useTranslation } from "@nekazari/sdk";
import type { CropListCrop, OrganicInputsResult } from "../services/api";

export default function OrganicInputs() {
  const { t } = useTranslation('bioorchestrator');
  const [availableCrops, setAvailableCrops] = useState<CropListCrop[]>([]);
  const [selectedCrop, setSelectedCrop] = useState("");
  const [result, setResult] = useState<OrganicInputsResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";

  useEffect(() => {
    fetch(`${API_BASE}/api/graph/agriculture/crops`, { credentials: "include" })
      .then(r => r.json())
      .then(d => setAvailableCrops(d.crops || []))
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (!selectedCrop) { setResult(null); return; }
    let cancelled = false;
    setLoading(true); setError(""); setResult(null);
    fetch(`${API_BASE}/api/graph/agriculture/organic-inputs?crop=${encodeURIComponent(selectedCrop)}`, { credentials: "include" })
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data: OrganicInputsResult) => { if (!cancelled) setResult(data); })
      .catch((e: Error) => { if (!cancelled) setError(e.message); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [selectedCrop]);

  const selectedCropName = availableCrops.find(c => c.eppo_code === selectedCrop)?.scientific_name || selectedCrop;

  return (
    <div>
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary mb-4">🍃 {t("organic.title")}</h2>

      <div style={{ marginBottom: 16, maxWidth: 400 }}>
        <select
          value={selectedCrop}
          onChange={e => setSelectedCrop(e.target.value)}
          style={{ width: "100%", padding: 8 }}
        >
          <option value="">{t("organic.selectCrop")}</option>
          {availableCrops.map(c => {
            const name = t(`crops.${c.eppo_code}`, { defaultValue: c.scientific_name === '(unknown)' ? c.eppo_code : c.scientific_name });
            return (
              <option key={c.eppo_code} value={c.eppo_code}>
                {c.eppo_code} — {name}
              </option>
            );
          })}
        </select>
      </div>

      {loading && <div style={{ padding: 20, color: "#999" }}>⏳ {t("common.loading")}</div>}
      {error && <div style={{ color: "red", marginBottom: 12 }}>{error}</div>}

      {result && result.source_unavailable && (
        <div style={{ padding: 10, background: "#fff3cd", borderRadius: 6, marginBottom: 12, fontSize: 13 }}>
          ⚠️ {t("organic.eppoUnavailable")}
        </div>
      )}

      {result && !result.source_unavailable && result.inputs.length === 0 && selectedCrop && (
        <div style={{ padding: 20, color: "#999", textAlign: "center" }}>
          {t("organic.empty")}
        </div>
      )}

      {result && result.inputs.length > 0 && (
        <div>
          <div style={{ marginBottom: 8, fontSize: 13, color: "#666" }}>
            {t("organic.title")} — {selectedCropName} ({result.inputs.length})
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ background: "#f5f5f5", textAlign: "left" }}>
                  <th style={{ padding: 8 }}>{t("organic.product")}</th>
                  <th style={{ padding: 8 }}>{t("organic.activeSubstance")}</th>
                  <th style={{ padding: 8 }}>{t("organic.category")}</th>
                </tr>
              </thead>
              <tbody>
                {result.inputs.map((item, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid #eee" }}>
                    <td style={{ padding: 8 }}>{item.product}</td>
                    <td style={{ padding: 8 }}>{item.active_substance}</td>
                    <td style={{ padding: 8, color: "#888" }}>{item.category}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
