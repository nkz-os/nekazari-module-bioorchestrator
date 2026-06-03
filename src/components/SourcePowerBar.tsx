import React, { useEffect, useState } from "react";
import { useBioApi } from "../services/api";

interface SourceItem {
  key: string;
  name: string;
  domain: string;
  status: string;
}

const DOMAIN_ICONS: Record<string, string> = {
  "EPPO": "🔬", "EcoCrop": "🌍", "CPVO": "🧬", "Feedipedia": "🌾",
  "FiBL": "🛡️", "CABI": "🐛", "GBIF": "🦋", "Copernicus": "🛰️",
  "ERA5": "🌡️", "SoilGrids": "🪨", "Natura2000": "🏞️", "USDA": "🌿",
  "AGROVOC": "📚", "GlobalTreeSearch": "🌳", "IUCN": "🦜",
  "DADIS": "🐄", "INTIA": "🧪", "IFAPA": "🔬", "ITACyL": "🌱",
  "JRC MARS": "📡", "Legumes Translated": "📖", "IKERKETA": "⚙️",
  "AgriKnowledge": "🧠",
};

function domainIcon(domain: string): string {
  for (const [key, icon] of Object.entries(DOMAIN_ICONS)) {
    if (domain.toLowerCase().includes(key.toLowerCase())) return icon;
  }
  return "📦";
}

function statusColor(status: string): string {
  if (status === "ready") return "#4caf50";
  if (status === "degraded") return "#ff9800";
  return "#ccc";
}

export default function SourcePowerBar() {
  const api = useBioApi();
  const [sources, setSources] = useState<SourceItem[]>([]);

  useEffect(() => {
    api.getSources().then((data: any) => {
      const all: SourceItem[] = [];
      const byDomain = data.by_domain || {};
      for (const domain of Object.keys(byDomain)) {
        for (const s of byDomain[domain]) {
          all.push({ key: s.key, name: s.name, domain: s.domain || domain, status: s.status });
        }
      }
      setSources(all);
    }).catch(() => {});
  }, []);

  if (sources.length === 0) return null;

  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 6, padding: "4px 8px",
      background: "#f8f9fa", borderBottom: "1px solid #e9ecef",
      overflowX: "auto", fontSize: 11, flexWrap: "wrap", minHeight: 32,
    }}>
      <span style={{ color: "#888", marginRight: 4, whiteSpace: "nowrap", fontWeight: 600, fontSize: 10 }}>
        SOURCES
      </span>
      {sources.slice(0, 25).map((s) => (
        <span
          key={s.key}
          title={`${s.name} (${s.domain})\nStatus: ${s.status}`}
          style={{
            display: "inline-flex", alignItems: "center", gap: 2,
            padding: "1px 5px", borderRadius: 3, background: "#fff",
            border: "1px solid #e0e0e0", cursor: "default", whiteSpace: "nowrap",
          }}
        >
          <span style={{ fontSize: 10, color: statusColor(s.status) }}>●</span>
          <span style={{ fontSize: 13 }}>{domainIcon(s.domain)}</span>
        </span>
      ))}
      {sources.length > 25 && (
        <span style={{ color: "#999", fontSize: 10 }}>+{sources.length - 25} more</span>
      )}
    </div>
  );
}
