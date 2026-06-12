import React, { useState, useEffect, useMemo } from "react";
import { useTranslation } from "@nekazari/sdk";
import { Card, Stack, Select, DataTable, Badge } from "@nekazari/ui-kit";
import ContextEmptyState from "./shared/ContextEmptyState";
import DataTableSkeleton from "./shared/DataTableSkeleton";
import type { CropListCrop, OrganicInputsResult } from "../services/api";

interface InputRow {
  [key: string]: unknown;
  product: string;
  active_substance: string;
  category: string;
}

const selectCls =
  "w-full h-9 rounded-nkz-md border border-nkz-border bg-nkz-surface px-nkz-stack text-nkz-sm text-nkz-text-primary focus-visible:ring-2 focus-visible:ring-nkz-accent-base";

export default function OrganicInputs() {
  const { t } = useTranslation("bioorchestrator");
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

  const cropOptions = availableCrops.map(c => ({
    value: c.eppo_code,
    label: `${c.eppo_code} — ${t(`crops.${c.eppo_code}`, { defaultValue: c.scientific_name === "(unknown)" ? c.eppo_code : c.scientific_name })}`,
  }));

  const columns = useMemo(
    () => [
      { accessorKey: "product", header: t("organic.product") },
      { accessorKey: "active_substance", header: t("organic.activeSubstance") },
      {
        accessorKey: "category",
        header: t("organic.category"),
        cell: (info: { getValue: () => unknown }) => (
          <Badge intent="info">{info.getValue() as string}</Badge>
        ),
      },
    ],
    [t],
  );

  return (
    <Stack gap="section">
      <h2 className="text-nkz-lg font-bold text-nkz-text-primary">🍃 {t("organic.title")}</h2>

      <div className="max-w-sm">
        <label className="text-nkz-xs font-medium text-nkz-text-muted block mb-1">{t("organic.selectCrop")}</label>
        <Select
          value={selectedCrop}
          onValueChange={setSelectedCrop}
          options={cropOptions}
        />
      </div>

      {loading && <DataTableSkeleton columns={3} />}
      {error && <ContextEmptyState message={error} variant="warning" actionLabel={t("panel.retry")} onAction={() => setSelectedCrop(selectedCrop)} />}

      {result && result.source_unavailable && (
        <div className="bg-nkz-warning-soft border border-nkz-warning rounded-nkz-md p-3 text-sm">
          ⚠️ {t("organic.eppoUnavailable")}
        </div>
      )}

      {result && !result.source_unavailable && result.inputs.length === 0 && selectedCrop && (
        <ContextEmptyState message={t("organic.empty")} variant="info" />
      )}

      {result && result.inputs.length > 0 && (
        <Stack gap="stack">
          <div className="text-sm text-nkz-text-muted">
            {t("organic.title")} — {selectedCropName} ({result.inputs.length})
          </div>
          <Card padding="none">
            <DataTable columns={columns} data={result.inputs as InputRow[]} />
          </Card>
        </Stack>
      )}
    </Stack>
  );
}
