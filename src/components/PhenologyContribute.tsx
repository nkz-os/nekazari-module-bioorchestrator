import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';

interface Props {
    onClose: () => void;
}

const PhenologyContribute: React.FC<Props> = ({ onClose }) => {
    const { t } = useTranslation('bioorchestrator');
    const [species, setSpecies] = useState('');
    const [stage, setStage] = useState('');
    const [cultivar, setCultivar] = useState('');
    const [kc, setKc] = useState('');
    const [d1, setD1] = useState('');
    const [d2, setD2] = useState('');
    const [mdsRef, setMdsRef] = useState('');
    const [doi, setDoi] = useState('');
    const [author, setAuthor] = useState('');
    const [conditions, setConditions] = useState('');
    const [email, setEmail] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const [result, setResult] = useState<string | null>(null);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!species || !stage || !kc) return;

        setSubmitting(true);
        setResult(null);
        try {
            const params = new URLSearchParams({ species, stage, kc });
            if (d1) params.set('d1', d1);
            if (d2) params.set('d2', d2);
            if (mdsRef) params.set('mds_ref', mdsRef);
            if (cultivar) params.set('cultivar', cultivar);
            if (doi) params.set('doi', doi);
            if (author) params.set('author', author);
            if (conditions) params.set('conditions', conditions);
            if (email) params.set('contact_email', email);

            const resp = await fetch(`/api/bioorchestrator/api/graph/phenology-params/contribute?${params}`, {
                method: 'POST',
            });
            const data = await resp.json();
            if (resp.ok) {
                setResult('success');
                setTimeout(onClose, 2000);
            } else {
                setResult(data.detail || `HTTP ${resp.status}`);
            }
        } catch (e: any) {
            setResult(e.message);
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <div className="pheno-contribute-overlay" onClick={onClose}>
            <div className="pheno-contribute-modal" onClick={e => e.stopPropagation()}>
                <div className="pheno-contribute-header">
                    <h3>{t('phenology.contribute.title')}</h3>
                    <button className="pheno-close-btn" onClick={onClose}>✕</button>
                </div>

                {result === 'success' ? (
                    <div className="pheno-contribute-success">
                        ✅ {t('phenology.contribute.success')}
                    </div>
                ) : (
                    <form onSubmit={handleSubmit} className="pheno-contribute-form">
                        <p className="pheno-contribute-desc">{t('phenology.contribute.description')}</p>

                        <div className="pheno-form-grid">
                            <div className="pheno-form-group">
                                <label>{t('phenology.species')} *</label>
                                <input value={species} onChange={e => setSpecies(e.target.value)}
                                    placeholder="e.g. olive, almond" required />
                            </div>
                            <div className="pheno-form-group">
                                <label>{t('phenology.stage')} *</label>
                                <input value={stage} onChange={e => setStage(e.target.value)}
                                    placeholder="e.g. vegetative, pit_hardening" required />
                            </div>
                            <div className="pheno-form-group">
                                <label>{t('phenology.cultivar')}</label>
                                <input value={cultivar} onChange={e => setCultivar(e.target.value)}
                                    placeholder="e.g. Picual, Arbequina" />
                            </div>
                            <div className="pheno-form-group">
                                <label>Kc *</label>
                                <input type="number" step="0.01" min="0" max="2"
                                    value={kc} onChange={e => setKc(e.target.value)}
                                    placeholder="0.85" required />
                            </div>
                            <div className="pheno-form-group">
                                <label>D1 (NWSB) °C</label>
                                <input type="number" step="0.1" value={d1}
                                    onChange={e => setD1(e.target.value)} placeholder="2.0" />
                            </div>
                            <div className="pheno-form-group">
                                <label>D2 (Max Stress) °C</label>
                                <input type="number" step="0.1" value={d2}
                                    onChange={e => setD2(e.target.value)} placeholder="8.0" />
                            </div>
                            <div className="pheno-form-group">
                                <label>MDS Ref (µm)</label>
                                <input type="number" step="1" value={mdsRef}
                                    onChange={e => setMdsRef(e.target.value)} placeholder="150" />
                            </div>
                        </div>

                        <div className="pheno-form-section">
                            <h4>{t('phenology.contribute.provenance')}</h4>
                            <div className="pheno-form-grid">
                                <div className="pheno-form-group">
                                    <label>DOI</label>
                                    <input value={doi} onChange={e => setDoi(e.target.value)}
                                        placeholder="10.1234/example" />
                                </div>
                                <div className="pheno-form-group">
                                    <label>{t('phenology.contribute.author')}</label>
                                    <input value={author} onChange={e => setAuthor(e.target.value)}
                                        placeholder="e.g. Orgaz, Fereres" />
                                </div>
                                <div className="pheno-form-group">
                                    <label>Email</label>
                                    <input type="email" value={email} onChange={e => setEmail(e.target.value)}
                                        placeholder="investigador@csic.es" />
                                </div>
                            </div>
                            <div className="pheno-form-group">
                                <label>{t('phenology.contribute.conditions')}</label>
                                <textarea value={conditions} onChange={e => setConditions(e.target.value)}
                                    placeholder="e.g. Olive, cv. Picual, Córdoba, deficit irrigation 30% ETc, 4x6m spacing"
                                    rows={3} />
                            </div>
                        </div>

                        {result && result !== 'success' && (
                            <div className="pheno-contribute-error">❌ {result}</div>
                        )}

                        <div className="pheno-contribute-actions">
                            <button type="button" className="pheno-btn-cancel" onClick={onClose}>
                                {t('phenology.contribute.cancel')}
                            </button>
                            <button type="submit" className="pheno-btn-submit" disabled={submitting}>
                                {submitting ? '...' : t('phenology.contribute.submit')}
                            </button>
                        </div>
                    </form>
                )}
            </div>
        </div>
    );
};

export default PhenologyContribute;
