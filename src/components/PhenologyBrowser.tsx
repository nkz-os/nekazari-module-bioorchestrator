import React, { useEffect, useState, useCallback } from 'react';
import { useTranslation } from '@nekazari/sdk';

interface PhenologyParams {
    species: string;
    scientific_name?: string;
    stage: string;
    stage_description?: string;
    kc: number;
    kc_confidence_interval?: [number, number];
    d1: number;
    d1_confidence_interval?: [number, number];
    d2: number;
    d2_confidence_interval?: [number, number];
    mds_ref: number;
    mds_ref_confidence_interval?: [number, number];
    cultivar?: string;
    management?: string;
    climate_zone?: string;
    match_level: string;
    is_default: boolean;
    provenance?: {
        doi?: string;
        short?: string;
        author?: string;
        year?: number;
        institution?: string;
        method?: string;
        conditions?: string;
    };
    alternatives?: Array<{
        kc: number;
        sourceShort?: string;
        sourceDoi?: string;
        conditions?: string;
    }>;
}

const PhenologyBrowser: React.FC = () => {
    const { t } = useTranslation('bioorchestrator');
    const [species, setSpecies] = useState('olive');
    const [stage, setStage] = useState('');
    const [cultivar, setCultivar] = useState('');
    const [management, setManagement] = useState('');
    const [data, setData] = useState<PhenologyParams | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const fetchParams = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const params = new URLSearchParams({ species });
            if (stage) params.set('stage', stage);
            if (cultivar) params.set('cultivar', cultivar);
            if (management) params.set('management', management);

            const resp = await fetch(`/api/bioorchestrator/api/graph/phenology-params?${params}`);
            if (resp.status === 404) {
                setData(null);
                setError(t('phenology.notFound'));
            } else if (!resp.ok) {
                throw new Error(`HTTP ${resp.status}`);
            } else {
                setData(await resp.json());
            }
        } catch (e: any) {
            setError(e.message);
            setData(null);
        } finally {
            setLoading(false);
        }
    }, [species, stage, cultivar, management, t]);

    useEffect(() => {
        fetchParams();
    }, [fetchParams]);

    const matchColor = (level: string) => {
        switch (level) {
            case 'exact': return '#16a34a';
            case 'management': return '#0284c7';
            case 'generic': return '#d97706';
            case 'species_only': return '#dc2626';
            default: return '#6b7280';
        }
    };

    return (
        <div className="phenology-browser">
            <div className="pheno-controls">
                <div className="control-group">
                    <label>{t('phenology.species')}</label>
                    <select value={species} onChange={(e) => setSpecies(e.target.value)}>
                        <option value="olive">Olive (Olea europaea)</option>
                        <option value="almond">Almond (Prunus dulcis)</option>
                        <option value="grapevine">Grapevine (Vitis vinifera)</option>
                        <option value="wheat">Wheat (Triticum aestivum)</option>
                    </select>
                </div>

                <div className="control-group">
                    <label>{t('phenology.stage')}</label>
                    <select value={stage} onChange={(e) => setStage(e.target.value)}>
                        <option value="">{t('phenology.anyStage')}</option>
                        {species === 'olive' && (
                            <>
                                <option value="vegetative">Vegetative</option>
                                <option value="pit_hardening">Pit Hardening</option>
                                <option value="fruit_growth">Fruit Growth</option>
                            </>
                        )}
                        {species === 'almond' && (
                            <>
                                <option value="vegetative">Vegetative</option>
                                <option value="kernel_fill">Kernel Fill</option>
                            </>
                        )}
                        {species === 'grapevine' && (
                            <>
                                <option value="vegetative">Vegetative</option>
                                <option value="veraison">Veraison</option>
                            </>
                        )}
                        {species === 'wheat' && (
                            <>
                                <option value="vegetative">Vegetative</option>
                                <option value="stem_elongation">Stem Elongation</option>
                            </>
                        )}
                    </select>
                </div>

                <div className="control-group">
                    <label>{t('phenology.cultivar')}</label>
                    <select value={cultivar} onChange={(e) => setCultivar(e.target.value)}>
                        <option value="">{t('phenology.anyCultivar')}</option>
                        <option value="Picual">Picual</option>
                        <option value="Nonpareil">Nonpareil</option>
                        <option value="Tempranillo">Tempranillo</option>
                    </select>
                </div>

                <div className="control-group">
                    <label>{t('phenology.management')}</label>
                    <select value={management} onChange={(e) => setManagement(e.target.value)}>
                        <option value="">{t('phenology.standardIrrigation')}</option>
                        <option value="deficit_irrigation">Deficit Irrigation</option>
                        <option value="regulated_deficit_irrigation">Regulated Deficit (RDI)</option>
                    </select>
                </div>
            </div>

            {loading && <div className="pheno-loading">{t('phenology.loading')}</div>}
            {error && <div className="pheno-error">{error}</div>}

            {data && (
                <div className="pheno-result">
                    <div className="pheno-match" style={{ borderLeftColor: matchColor(data.match_level) }}>
                        <span className="match-badge" style={{ background: matchColor(data.match_level) }}>
                            {data.match_level.toUpperCase()}
                        </span>
                        <span className="match-text">
                            {data.scientific_name && <em>{data.scientific_name}</em>}
                            {data.stage && ` — ${data.stage}`}
                            {data.stage_description && ` (${data.stage_description})`}
                        </span>
                    </div>

                    {data.is_default && (
                        <div className="pheno-warning">{t('phenology.usingDefaults')}</div>
                    )}

                    <table className="pheno-table">
                        <thead>
                            <tr>
                                <th>{t('phenology.parameter')}</th>
                                <th>{t('phenology.value')}</th>
                                <th>{t('phenology.ci')}</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td className="param-name">Kc</td>
                                <td className="param-value">{data.kc?.toFixed(2)}</td>
                                <td className="param-ci">
                                    {data.kc_confidence_interval
                                        ? `${data.kc_confidence_interval[0]?.toFixed(2)} – ${data.kc_confidence_interval[1]?.toFixed(2)}`
                                        : '—'}
                                </td>
                            </tr>
                            <tr>
                                <td className="param-name">D1 (NWSB)</td>
                                <td className="param-value">{data.d1?.toFixed(1)}°C</td>
                                <td className="param-ci">
                                    {data.d1_confidence_interval
                                        ? `${data.d1_confidence_interval[0]?.toFixed(1)} – ${data.d1_confidence_interval[1]?.toFixed(1)}`
                                        : '—'}
                                </td>
                            </tr>
                            <tr>
                                <td className="param-name">D2 (Max Stress)</td>
                                <td className="param-value">{data.d2?.toFixed(1)}°C</td>
                                <td className="param-ci">
                                    {data.d2_confidence_interval
                                        ? `${data.d2_confidence_interval[0]?.toFixed(1)} – ${data.d2_confidence_interval[1]?.toFixed(1)}`
                                        : '—'}
                                </td>
                            </tr>
                            <tr>
                                <td className="param-name">MDS Ref</td>
                                <td className="param-value">{data.mds_ref?.toFixed(0)}µm</td>
                                <td className="param-ci">
                                    {data.mds_ref_confidence_interval
                                        ? `${data.mds_ref_confidence_interval[0]?.toFixed(0)} – ${data.mds_ref_confidence_interval[1]?.toFixed(0)}`
                                        : '—'}
                                </td>
                            </tr>
                        </tbody>
                    </table>

                    {data.provenance && (
                        <div className="pheno-provenance">
                            <h4>{t('phenology.source')}</h4>
                            <p>
                                <strong>{data.provenance.short}</strong>
                                {data.provenance.author && ` — ${data.provenance.author}`}
                                {data.provenance.year && ` (${data.provenance.year})`}
                            </p>
                            {data.provenance.institution && <p>{data.provenance.institution}</p>}
                            {data.provenance.doi && (
                                <p>
                                    DOI: <a href={`https://doi.org/${data.provenance.doi}`} target="_blank" rel="noopener">
                                        {data.provenance.doi}
                                    </a>
                                </p>
                            )}
                            {data.provenance.method && <p className="provenance-method">{data.provenance.method}</p>}
                            {data.provenance.conditions && <p className="provenance-conditions">{data.provenance.conditions}</p>}
                        </div>
                    )}

                    {(data.alternatives || []).length > 0 && (
                        <div className="pheno-alternatives">
                            <h4>{t('phenology.alternatives')}</h4>
                            {data.alternatives!.map((alt, i) => (
                                <div key={i} className="alt-item">
                                    <span className="alt-kc">Kc = {alt.kc?.toFixed(2)}</span>
                                    {alt.sourceShort && <span className="alt-source"> — {alt.sourceShort}</span>}
                                    {alt.sourceDoi && (
                                        <a href={`https://doi.org/${alt.sourceDoi}`} target="_blank" rel="noopener" className="alt-doi">
                                            DOI
                                        </a>
                                    )}
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};

export default PhenologyBrowser;
