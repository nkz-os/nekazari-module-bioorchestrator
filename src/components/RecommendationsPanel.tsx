import React, { useEffect, useState } from 'react';

interface RecCrop { name: string; scientific_name?: string; }
interface FertRec { element: string; uptake_kg_ha_day: number; soil_level: number; status: string; action: string; }
interface SoilData { ph_min: number; ph_max: number; textures: string[]; drainage: string[]; depth_min_cm: number; salinity_max_ds_m: number; source_short?: string; }

interface Props { parcelId?: string; parcelName?: string; cropType?: string; lat?: number; lon?: number; }

const PESTICIDE_EMOJI: Record<string, string> = { approved: '✅', not_approved: '❌', withdrawn: '⚠️' };

const RecommendationsPanel: React.FC<Props> = ({ parcelId, parcelName, cropType = 'olive', lat, lon }) => {
    const [nextCrops, setNextCrops] = useState<RecCrop[]>([]);
    const [soil, setSoil] = useState<SoilData | null>(null);
    const [realSoil, setRealSoil] = useState<any>(null);
    const [protectedArea, setProtectedArea] = useState<any>(null);
    const [varieties, setVarieties] = useState<any[]>([]);
    const [pesticides, setPesticides] = useState<any[]>([]);
    const [pollinators, setPollinators] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            setLoading(true);
            try {
                const base = '/api/bioorchestrator/api/graph';
                const fetches: Promise<any>[] = [
                    fetch(`${base}/recommendations/next-crop?previous_crop=${cropType}`).then(r => r.json()),
                    fetch(`${base}/soil-suitability?species=${cropType}`).then(r => r.ok ? r.json() : null),
                ];
                if (lat != null && lon != null) {
                    fetches.push(
                        fetch(`${base}/soil-data?lat=${lat}&lon=${lon}`).then(r => r.ok ? r.json() : null),
                        fetch(`${base}/protected-area-check?lat=${lat}&lon=${lon}`).then(r => r.ok ? r.json() : null),
                        fetch(`${base}/varieties?species=${cropType}`).then(r => r.ok ? r.json() : null),
                        fetch(`${base}/pesticides?crop=${cropType}`).then(r => r.ok ? r.json() : null),
                        fetch(`${base}/pollinators?lat=${lat}&lon=${lon}`).then(r => r.ok ? r.json() : null),
                    );
                }
                const results = await Promise.allSettled(fetches);
                const get = (i: number) => results[i]?.status === 'fulfilled' ? results[i].value : null;
                setNextCrops(get(0)?.suggested_crops || []);
                if (get(1)) setSoil(get(1));
                if (get(2)) setRealSoil(get(2));
                if (get(3)) setProtectedArea(get(3));
                if (get(4)) setVarieties(get(4)?.varieties || []);
                if (get(5)) setPesticides(get(5)?.substances || []);
                if (get(6)) setPollinators(get(6)?.pollinators || []);
            } catch {} finally { setLoading(false); }
        };
        fetchData();
    }, [cropType, lat, lon]);

    if (loading) return <div className="rec-loading">Cargando recomendaciones...</div>;

    return (
        <div className="rec-panel">
            <h3 className="rec-title">📋 Recomendaciones</h3>
            {parcelName && <p className="rec-parcel">{parcelName}</p>}

            {/* Rotation */}
            <div className="rec-section">
                <h4>🔄 Rotación</h4>
                <p className="rec-current">Cultivo: <strong>{cropType}</strong></p>
                {nextCrops.length > 0 ? (
                    <div className="rec-crops">{nextCrops.map(c => <span key={c.name} className="rec-crop-badge">{c.scientific_name || c.name}</span>)}</div>
                ) : <p className="rec-none">Sin restricciones de rotación.</p>}
            </div>

            {/* Real Soil (SoilGrids) */}
            {realSoil && !realSoil.error && (
                <div className="rec-section">
                    <h4>🔬 Suelo real (SoilGrids 2.0)</h4>
                    <table className="rec-table"><tbody>
                        {realSoil.ph != null && <tr><td>pH</td><td>{realSoil.ph}</td></tr>}
                        {realSoil.texture_class && <tr><td>Textura</td><td>{realSoil.texture_class} ({realSoil.sand_pct}% arena, {realSoil.clay_pct}% arcilla)</td></tr>}
                        {realSoil.cec_cmol_kg != null && <tr><td>CIC</td><td>{realSoil.cec_cmol_kg} cmol/kg</td></tr>}
                    </tbody></table>
                    <p className="rec-source">📄 {realSoil.source}</p>
                </div>
            )}

            {/* Soil Suitability */}
            {soil && (
                <div className="rec-section">
                    <h4>🌍 Requisitos de suelo ({cropType})</h4>
                    <table className="rec-table"><tbody>
                        <tr><td>pH</td><td>{soil.ph_min} – {soil.ph_max}</td></tr>
                        <tr><td>Textura</td><td>{(soil.textures || []).join(', ')}</td></tr>
                        <tr><td>Drenaje</td><td>{(soil.drainage || []).join(', ')}</td></tr>
                    </tbody></table>
                </div>
            )}

            {/* Natura 2000 */}
            {protectedArea && protectedArea.in_protected_area && (
                <div className="rec-section" style={{ borderLeft: '3px solid #16a34a', background: '#f0fdf4' }}>
                    <h4>🌿 Espacio Protegido</h4>
                    <p><strong>{protectedArea.site_name}</strong> ({protectedArea.site_code})</p>
                    {protectedArea.restrictions && <p>⚠️ {protectedArea.restrictions}</p>}
                </div>
            )}

            {/* Varieties */}
            {varieties.length > 0 && (
                <div className="rec-section">
                    <h4>🌾 Variedades registradas (CPVO)</h4>
                    <div className="rec-crops">
                        {varieties.slice(0, 6).map((v: any, i: number) => (
                            <span key={i} className="rec-crop-badge" title={`Mantenedor: ${v.maintainer || 'N/A'} · Año: ${v.registration_year || 'N/A'}`}>
                                {v.variety_name || v.denomination}
                            </span>
                        ))}
                    </div>
                    {varieties.length > 6 && <p className="rec-none">+{varieties.length - 6} más</p>}
                </div>
            )}

            {/* Pesticides */}
            {pesticides.length > 0 && (
                <div className="rec-section">
                    <h4>🧪 Fitosanitarios autorizados (EU)</h4>
                    {pesticides.slice(0, 5).map((p: any, i: number) => (
                        <div key={i} className="rec-pest-item">
                            <span>{PESTICIDE_EMOJI[p.status] || '❓'} {p.substance}</span>
                            {p.mrl_mg_kg != null && <span className="rec-pest-mrl">LMR: {p.mrl_mg_kg} mg/kg</span>}
                        </div>
                    ))}
                </div>
            )}

            {/* Pollinators */}
            {pollinators.length > 0 && (
                <div className="rec-section">
                    <h4>🐝 Polinizadores en la zona (GBIF, 5km)</h4>
                    {pollinators.slice(0, 5).map((p: any, i: number) => (
                        <div key={i} className="rec-pollinator-item">
                            🦋 {p.species} <span className="rec-pollinator-count">({p.record_count} registros)</span>
                        </div>
                    ))}
                </div>
            )}

            {/* Scenario */}
            <div className="rec-section">
                <h4>🔮 Simular escenario</h4>
                <ScenarioSimulator currentCrop={cropType} />
            </div>
        </div>
    );
};

const SCENARIO_CROPS = ['wheat', 'sunflower', 'almond', 'olive', 'grapevine', 'legume'];

const ScenarioSimulator: React.FC<{ currentCrop: string }> = ({ currentCrop }) => {
    const [scenario, setScenario] = useState('');
    const [result, setResult] = useState<any>(null);
    const [loading, setLoading] = useState(false);

    const runSimulation = async () => {
        if (!scenario) return;
        setLoading(true);
        try {
            const resp = await fetch(`/api/bioorchestrator/api/graph/recommendations/simulate?baseline_crop=${currentCrop}&scenario_crop=${scenario}`);
            if (resp.ok) setResult(await resp.json());
        } catch {} finally { setLoading(false); }
    };

    return (
        <div className="rec-simulate">
            <div className="rec-sim-controls">
                <select value={scenario} onChange={e => setScenario(e.target.value)}>
                    <option value="">Alternativa...</option>
                    {SCENARIO_CROPS.filter(c => c !== currentCrop).map(c => <option key={c} value={c}>{c}</option>)}
                </select>
                <button onClick={runSimulation} disabled={!scenario || loading}>{loading ? '...' : 'Comparar'}</button>
            </div>
            {result && (
                <div className="rec-sim-result">
                    <p>{result.rotation_ok ? '✅' : '⚠️'} {result.recommendation}</p>
                    {result.rotation_issue && <p>🔄 {result.rotation_issue}</p>}
                    {result.fertilizer_delta?.map((f: any, i: number) => (
                        <p key={i}>🧪 {f.element}: {f.delta_kg_ha_day > 0 ? '+' : ''}{f.delta_kg_ha_day} kg/ha/día — {f.note}</p>
                    ))}
                </div>
            )}
        </div>
    );
};

export default RecommendationsPanel;
