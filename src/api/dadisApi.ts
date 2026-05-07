/**
 * DAD-IS API Client for BioOrchestrator
 * Uses NKZ API through the backend proxy avoiding CORS and auth issues.
 */

// Resolve SDK API lazily at call time — the host initialises __NKZ_SDK__
// asynchronously and the IIFE bundle may load before it is ready.
function getApi(): any {
    const sdk = (window as any).__NKZ_SDK__;
    if (!sdk?.api) {
        throw new Error("[BioOrchestrator] NKZ SDK API not available");
    }
    return sdk.api;
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
        const response = await getApi().post('/bioorchestrator/api/dadis/breeds', {
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
        const response = await getApi().get(`/bioorchestrator/api/dadis/breeds/${breedId}?lang=${lang}`);
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
        const response = await getApi().get('/bioorchestrator/api/dadis/countries');
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
        const response = await getApi().get('/bioorchestrator/api/dadis/species');
        return response.data;
    } catch (error) {
        console.error("Error fetching DAD-IS species:", error);
        throw error;
    }
};
