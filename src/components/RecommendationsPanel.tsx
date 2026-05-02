import React, { useEffect, useState } from 'react';

interface RecCrop {
    name: string;
    scientific_name?: string;
}

interface FertRec {
    element: string;
    uptake_kg_ha_day: number;
    soil_level: number;
    status: string;
    action: string;
}

interface SoilData {
    ph_min: number;
    ph_max: number;
    textures: string[];
    drainage: string[];
    depth_min_cm: number;
    salinity_max_ds_m: number;
    source_short?: string;
}

interface Props {
    parcelId?: string;
    parcelName?: string;
    cropType?: string;
}

const RecommendationsPanel: React.FC<Props> = ({ parcelId, parcelName, cropType = 'olive' }) => {
    const [nextCrops, setNextCrops] = useState<RecCrop[]>([]);
    const [fertilizer, setFertilizer] = useState<FertRec[]>([]);
    const [soil, setSoil] = useState<SoilData | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            setLoading(true);
            try {
                const base = '/api/bioorchestrator/api/graph';
                const [cropRes, soilRes] = await Promise.allSettled([
                    fetch(`${base}/recommendations/next-crop?previous_crop=${cropType}`).then(r => r.json()),
                    fetch(`${base}/soil-suitability?species=${cropType}`).then(r => r.ok ? r.json() : null),
                ]);

                if (cropRes.status === 'fulfilled' && cropRes.value?.suggested_crops) {
                    setNextCrops(cropRes.value.suggested_crops);
                }
                if (soilRes.status === 'fulfilled' && soilRes.value) {
                    setSoil(soilRes.value);
                }
            } catch {} finally { setLoading(false); }
        };
        fetchData();
    }, [cropType]);

    if (loading) return <div className="rec-loading">Cargando recomendaciones...</div>;

    return (
        <div className="rec-panel">
            <h3 className="rec-title">📋 Recomendaciones</h3>
            {parcelName && <p className="rec-parcel">{parcelName}</p>}

            {/* Next crop */}
            <div className="rec-section">
                <h4>🔄 Rotación de cultivo</h4>
                <p className="rec-current">Cultivo actual: <strong>{cropType}</strong></p>
                {nextCrops.length > 0 ? (
                    <div className="rec-crops">
                        {nextCrops.map(c => (
                            <span key={c.name} className="rec-crop-badge">
                                {c.scientific_name || c.name}
                            </span>
                        ))}
                    </div>
                ) : (
                    <p className="rec-none">No hay restricciones de rotación para este cultivo.</p>
                )}
            </div>

            {/* Soil suitability */}
            {soil && (
                <div className="rec-section">
                    <h4>🌍 Requisitos de suelo</h4>
                    <table className="rec-table">
                        <tbody>
                            <tr><td>pH</td><td>{soil.ph_min} – {soil.ph_max}</td></tr>
                            <tr><td>Textura</td><td>{(soil.textures || []).join(', ')}</td></tr>
                            <tr><td>Drenaje</td><td>{(soil.drainage || []).join(', ')}</td></tr>
                            <tr><td>Profundidad mín.</td><td>{soil.depth_min_cm} cm</td></tr>
                            <tr><td>Salinidad máx.</td><td>{soil.salinity_max_ds_m} dS/m</td></tr>
                        </tbody>
                    </table>
                    {soil.source_short && <p className="rec-source">📄 {soil.source_short}</p>}
                </div>
            )}

            {/* Scenario Simulation */}
            <div className="rec-section">
                <h4>🔮 Simular escenario</h4>
                <ScenarioSimulator currentCrop={cropType} />
            </div>

            {/* Fertilizer */}
            {fertilizer.length > 0 && (
                <div className="rec-section">
                    <h4>🧪 Fertilización</h4>
                    {fertilizer.map(f => (
                        <div key={f.element} className={`rec-fert-item rec-fert-${f.status}`}>
                            <span className="rec-fert-element">{f.element.toUpperCase()}</span>
                            <span className="rec-fert-uptake">{f.uptake_kg_ha_day} kg/ha/día</span>
                            <span className={`rec-fert-status rec-fert-${f.status}`}>{f.action}</span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};

// ── Scenario Simulator sub-component ──────────────────────────────────────

const SCENARIO_CROPS = ['wheat', 'sunflower', 'almond', 'olive', 'grapevine', 'legume'];

const ScenarioSimulator: React.FC<{ currentCrop: string }> = ({ currentCrop }) => {
    const [scenario, setScenario] = useState('');
    const [result, setResult] = useState<any>(null);
    const [loading, setLoading] = useState(false);

    const runSimulation = async () => {
        if (!scenario) return;
        setLoading(true);
        try {
            const resp = await fetch(
                `/api/bioorchestrator/api/graph/recommendations/simulate?baseline_crop=${currentCrop}&scenario_crop=${scenario}`
            );
            if (resp.ok) setResult(await resp.json());
        } catch {} finally { setLoading(false); }
    };

    return (
        <div className="rec-simulate">
            <div className="rec-sim-controls">
                <select value={scenario} onChange={e => setScenario(e.target.value)}>
                    <option value="">Seleccionar alternativa...</option>
                    {SCENARIO_CROPS.filter(c => c !== currentCrop).map(c => (
                        <option key={c} value={c}>{c}</option>
                    ))}
                </select>
                <button onClick={runSimulation} disabled={!scenario || loading}>
                    {loading ? '...' : 'Comparar'}
                </button>
            </div>

            {result && (
                <div className="rec-sim-result">
                    <p className="rec-sim-recommendation">
                        {result.rotation_ok ? '✅' : '⚠️'} {result.recommendation}
                    </p>
                    {result.rotation_issue && (
                        <p className="rec-sim-issue">🔄 {result.rotation_issue}</p>
                    )}
                    {result.soil_issues?.map((s: string, i: number) => (
                        <p key={i} className="rec-sim-issue">🌍 {s}</p>
                    ))}
                    {result.fertilizer_delta?.map((f: any, i: number) => (
                        <p key={i} className="rec-sim-delta">
                            🧪 {f.element}: {f.delta_kg_ha_day > 0 ? '+' : ''}{f.delta_kg_ha_day} kg/ha/day — {f.note}
                        </p>
                    ))}
                </div>
            )}
        </div>
    );
};

export default RecommendationsPanel;
