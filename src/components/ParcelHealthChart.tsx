import React, { useState } from "react";
import { useTranslation } from '@nekazari/sdk';
import { HistoryPoint } from "../services/api";

interface Props {
  data: HistoryPoint[];
}

const COLORS = { cwsi: "#4A90D9", mds: "#E8913A", balance: "#50B86C" };
const RANGES = [7, 14, 30];

function svgPath(
  points: (number | null)[],
  xScale: (i: number) => number,
  yScale: (v: number) => number,
  yMin: number
): string {
  const valid: [number, number][] = [];
  points.forEach((v, i) => {
    if (v != null) valid.push([xScale(i), yScale(v)]);
  });
  if (valid.length === 0) return "";
  if (valid.length === 1) {
    const [cx, cy] = valid[0];
    return `M${cx},${yScale(yMin)} L${cx},${cy}`;
  }
  return valid
    .map(([x, y], i) => `${i === 0 ? "M" : "L"}${x},${y}`)
    .join(" ");
}

export default function ParcelHealthChart({ data }: Props) {
  const { t } = useTranslation('bioorchestrator');
  const [days, setDays] = useState(14);

  const filtered = data.slice(-days);
  if (filtered.length === 0) {
    return (
      <div style={{ padding: 12, fontSize: 12, color: "#999" }}>
        📊 {t("parcelHealth.noHistory")}
      </div>
    );
  }

  const W = 600,
    H = 200,
    PAD = { top: 10, right: 10, bottom: 30, left: 45 };
  const iW = W - PAD.left - PAD.right,
    iH = H - PAD.top - PAD.bottom;

  const xScale = (i: number) =>
    PAD.left + (i / Math.max(1, filtered.length - 1)) * iW;

  // CWSI shares 0-1 scale on left axis
  const cwsiVals = filtered.map((p) => p.cwsi).filter((v) => v != null) as number[];
  const cwsiMax = Math.max(1.0, ...cwsiVals);
  const yScaleCwsi = (v: number) => PAD.top + iH - (v / cwsiMax) * iH;

  // MDS uses its own scale
  const mdsVals = filtered.map((p) => p.mds).filter((v) => v != null) as number[];
  const mdsMax = Math.max(200, ...mdsVals);
  const yScaleMds = (v: number) => PAD.top + iH - (v / mdsMax) * iH;

  // Water balance uses right axis
  const balVals = filtered
    .map((p) => p.balance)
    .filter((v) => v != null) as number[];
  const balMin = Math.min(-20, ...balVals, 0);
  const balMax = Math.max(20, ...balVals);
  const yScaleBal = (v: number) =>
    PAD.top + iH - ((v - balMin) / Math.max(1, balMax - balMin)) * iH;

  const cwsiPath = svgPath(
    filtered.map((p) => p.cwsi),
    xScale,
    yScaleCwsi,
    0
  );
  const mdsPath = svgPath(
    filtered.map((p) => p.mds),
    xScale,
    yScaleMds,
    0
  );
  const balPath = svgPath(
    filtered.map((p) => p.balance),
    xScale,
    yScaleBal,
    balMin
  );

  const xLabels = filtered.filter(
    (_, i) =>
      i % Math.max(1, Math.floor(filtered.length / 5)) === 0 ||
      i === filtered.length - 1
  );

  return (
    <div style={{ marginBottom: 16 }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 8,
          maxWidth: W,
        }}
      >
        <strong style={{ fontSize: 13 }}>
          📈 {t("parcelHealth.historyTitle")}
        </strong>
        <div style={{ display: "flex", gap: 4 }}>
          {RANGES.map((r) => (
            <button
              key={r}
              onClick={() => setDays(r)}
              style={{
                padding: "2px 8px",
                fontSize: 11,
                border: "1px solid #ccc",
                borderRadius: 4,
                background: days === r ? "#4A90D9" : "#fff",
                color: days === r ? "#fff" : "#333",
                cursor: "pointer",
              }}
            >
              {r}d
            </button>
          ))}
        </div>
      </div>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        style={{
          width: "100%",
          maxWidth: W,
          border: "1px solid #eee",
          borderRadius: 8,
          background: "#fafafa",
        }}
      >
        {/* Y-axis grid lines for CWSI */}
        {[0, 0.25, 0.5, 0.75, 1.0].map((v) => (
          <line
            key={`g-${v}`}
            x1={PAD.left}
            y1={yScaleCwsi(v)}
            x2={W - PAD.right}
            y2={yScaleCwsi(v)}
            stroke="#eee"
            strokeWidth={0.5}
          />
        ))}
        {/* Zero line for water balance */}
        <line
          x1={PAD.left}
          y1={yScaleBal(0)}
          x2={W - PAD.right}
          y2={yScaleBal(0)}
          stroke="#ccc"
          strokeWidth={1}
          strokeDasharray="4 2"
        />
        {/* CWSI line */}
        {cwsiPath && (
          <path d={cwsiPath} fill="none" stroke={COLORS.cwsi} strokeWidth={2} />
        )}
        {/* MDS line */}
        {mdsPath && (
          <path
            d={mdsPath}
            fill="none"
            stroke={COLORS.mds}
            strokeWidth={2}
            strokeDasharray="6 3"
          />
        )}
        {/* Water balance line */}
        {balPath && (
          <path
            d={balPath}
            fill="none"
            stroke={COLORS.balance}
            strokeWidth={1.5}
          />
        )}
        {/* X labels */}
        {xLabels.map((p, i) => (
          <text
            key={`xl-${i}`}
            x={xScale(filtered.indexOf(p))}
            y={H - 8}
            textAnchor="middle"
            fontSize={9}
            fill="#999"
          >
            {p.date.slice(5, 10)}
          </text>
        ))}
      </svg>
      {/* Legend */}
      <div
        style={{
          display: "flex",
          gap: 16,
          marginTop: 6,
          fontSize: 11,
          color: "#666",
          maxWidth: W,
        }}
      >
        <span>
          <span style={{ color: COLORS.cwsi, fontWeight: 600 }}>─</span> CWSI
        </span>
        <span>
          <span style={{ color: COLORS.mds, fontWeight: 600 }}>- -</span> MDS
          (µm)
        </span>
        <span>
          <span style={{ color: COLORS.balance, fontWeight: 600 }}>─</span>{" "}
          {t("parcelHealth.waterBalance")}
        </span>
      </div>
    </div>
  );
}
