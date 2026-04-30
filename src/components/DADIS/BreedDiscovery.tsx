import React, { useState, useEffect } from 'react';
import { getBreeds, getCountries, getSpecies, DadisBreed, DadisCountry, DadisSpecies } from '../../api/dadisApi';
import { useTranslation } from '@nekazari/sdk';

// Use components from host UI kit if available, otherwise fallbacks
const Card = (window as any).__NKZ_UI__?.Card || (({ children, className }: any) => <div className={`bg-white rounded-lg shadow p-4 ${className}`}>{children}</div>);
const Button = (window as any).__NKZ_UI__?.Button || (({ children, onClick, disabled, className }: any) => <button onClick={onClick} disabled={disabled} className={`px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 ${className}`}>{children}</button>);

export const BreedDiscovery: React.FC = () => {
    const { t } = useTranslation('bioorchestrator');
    const [breeds, setBreeds] = useState<DadisBreed[]>([]);
    const [countries, setCountries] = useState<DadisCountry[]>([]);
    const [species, setSpecies] = useState<DadisSpecies[]>([]);

    const [loading, setLoading] = useState<boolean>(false);
    const [error, setError] = useState<string | null>(null);

    // Filters
    const [selectedCountry, setSelectedCountry] = useState<string>('');
    const [selectedSpecies, setSelectedSpecies] = useState<number | ''>('');
    const [classification, setClassification] = useState<'all' | 'local' | 'transboundary'>('all');

    // Load initial reference data
    useEffect(() => {
        const loadReferenceData = async () => {
            try {
                const [c, s] = await Promise.all([
                    getCountries(),
                    getSpecies()
                ]);
                setCountries(c || []);
                setSpecies(s || []);
            } catch (err: any) {
                console.error("Failed to load reference data", err);
                // Don't show hard error here so UI remains usable for manual search
            }
        };

        loadReferenceData();
    }, []);

    const handleSearch = async () => {
        setLoading(true);
        setError(null);
        try {
            const countryIds = selectedCountry ? [selectedCountry] : undefined;
            const speciesIds = selectedSpecies !== '' ? [Number(selectedSpecies)] : undefined;

            const results = await getBreeds(classification, countryIds, speciesIds);
            setBreeds(results || []);
        } catch (err: any) {
            setError(err.message || t('dadis.errors.loadBreeds'));
            setBreeds([]);
        } finally {
            setLoading(false);
        }
    };

    return (
        <Card className="flex flex-col h-full">
            <div className="flex justify-between items-center mb-4">
                <h2 className="text-xl font-semibold text-gray-800 flex items-center gap-2">
                    <svg className="w-6 h-6 text-green-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
                    </svg>
                    {t('dadis.title')}
                </h2>
                <div className="text-xs font-mono bg-gray-100 text-gray-600 px-2 py-1 rounded">
                    {t('dadis.badge')}
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">{t('dadis.filters.country')}</label>
                    <select
                        className="w-full border-gray-300 rounded-md shadow-sm text-sm p-2 border"
                        value={selectedCountry}
                        onChange={(e) => setSelectedCountry(e.target.value)}
                    >
                        <option value="">{t('dadis.filters.allCountries')}</option>
                        {countries.map(c => (
                            <option key={c.iso3 || c.name} value={c.iso3}>{c.name}</option>
                        ))}
                    </select>
                </div>
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">{t('dadis.filters.species')}</label>
                    <select
                        className="w-full border-gray-300 rounded-md shadow-sm text-sm p-2 border"
                        value={selectedSpecies}
                        onChange={(e) => setSelectedSpecies(e.target.value ? Number(e.target.value) : '')}
                    >
                        <option value="">{t('dadis.filters.allSpecies')}</option>
                        {species.map(s => (
                            <option key={s.id} value={s.id}>{s.name || s.id}</option>
                        ))}
                    </select>
                </div>
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">{t('dadis.filters.classification')}</label>
                    <select
                        className="w-full border-gray-300 rounded-md shadow-sm text-sm p-2 border"
                        value={classification}
                        onChange={(e) => setClassification(e.target.value as any)}
                    >
                        <option value="all">{t('dadis.filters.all')}</option>
                        <option value="local">{t('dadis.filters.local')}</option>
                        <option value="transboundary">{t('dadis.filters.transboundary')}</option>
                    </select>
                </div>
                <div className="flex items-end">
                    <Button
                        onClick={handleSearch}
                        disabled={loading}
                        className="w-full flex justify-center items-center h-[38px]"
                    >
                        {loading ? t('dadis.searching') : t('dadis.search')}
                    </Button>
                </div>
            </div>

            {error && (
                <div className="bg-red-50 text-red-600 p-3 rounded-md text-sm mb-4">
                    {error}
                </div>
            )}

            <div className="flex-1 overflow-auto border rounded-md min-h-[300px]">
                {loading ? (
                    <div className="flex items-center justify-center h-full text-gray-500">
                        <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-gray-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                        {t('dadis.loading')}
                    </div>
                ) : breeds.length > 0 ? (
                    <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50 sticky top-0">
                            <tr>
                                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{t('dadis.table.breedName')}</th>
                                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{t('dadis.table.speciesId')}</th>
                                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{t('dadis.table.country')}</th>
                                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{t('dadis.table.transboundary')}</th>
                            </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                            {breeds.map((breed, idx) => (
                                <tr key={`${breed.breedId}-${idx}`} className="hover:bg-gray-50 cursor-pointer">
                                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{breed.breedName}</td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                                            {breed.speciesId}
                                        </span>
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{breed.countryISO3}</td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        {breed.transboundaryId ? (
                                            <span className="text-purple-600 font-mono text-xs">{breed.transboundaryId}</span>
                                        ) : (
                                            <span className="text-gray-300">-</span>
                                        )}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                ) : (
                    <div className="flex flex-col items-center justify-center h-full text-gray-400 p-8 text-center">
                        <svg className="w-12 h-12 mb-2 text-gray-300" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                        <p>{t('dadis.empty.title')}</p>
                        <p className="text-xs mt-1">{t('dadis.empty.hint')}</p>
                    </div>
                )}
            </div>
            {breeds.length > 0 && (
                <div className="mt-2 text-xs text-gray-500 text-right">
                    {t('dadis.footer', { count: breeds.length })}
                </div>
            )}
        </Card>
    );
};
