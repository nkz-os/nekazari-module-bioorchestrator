/**
 * DAD-IS API Client for BioOrchestrator
 * Uses NKZ API through the backend proxy avoiding CORS and auth issues.
 */

// Access host-provided dependencies based on the platform architecture (IIFE context)
const api = (window as any).__NKZ_SDK__?.api;

if (!api) {
    console.warn("[BioOrchestrator] NKZ SDK API not found. Calls will fail.");
}

export interface DadisBreed {
    breedName: string;
    breedId: string;
    speciesId: number;
    countryISO3: string;
    transboundaryId?: string;
    lastModification?: string;
    [key: string]: any;
}

export interface DadisCountry {
    iso3: string;
    name: string;
    [key: string]: any;
}

export interface DadisSpecies {
    id: number;
    name: string;
    [key: string]: any;
}

/**
 * Get all breeds, optionally filtered
 */
export const getBreeds = async (
    classification: 'all' | 'local' | 'transboundary' = 'all',
    countryIds?: string[],
    speciesIds?: number[]
): Promise<DadisBreed[]> => {
    try {
        const response = await api.post('/bioorchestrator/v1/dadis/breeds', {
            classification,
            countryIds: countryIds || [],
            speciesIds: speciesIds || []
        });
        return response.data;
    } catch (error) {
        console.error("Error fetching DAD-IS breeds:", error);
        throw error;
    }
};

/**
 * Get a specific breed by ID
 */
export const getBreedById = async (breedId: string, lang: string = 'en'): Promise<any> => {
    try {
        const response = await api.get(`/bioorchestrator/v1/dadis/breeds/${breedId}?lang=${lang}`);
        return response.data;
    } catch (error) {
        console.error(`Error fetching breed ${breedId}:`, error);
        throw error;
    }
};

/**
 * Get all available countries
 */
export const getCountries = async (): Promise<DadisCountry[]> => {
    try {
        const response = await api.get('/bioorchestrator/v1/dadis/countries');
        return response.data;
    } catch (error) {
        console.error("Error fetching DAD-IS countries:", error);
        throw error;
    }
};

/**
 * Get all available species
 */
export const getSpecies = async (): Promise<DadisSpecies[]> => {
    try {
        const response = await api.get('/bioorchestrator/v1/dadis/species');
        return response.data;
    } catch (error) {
        console.error("Error fetching DAD-IS species:", error);
        throw error;
    }
};
