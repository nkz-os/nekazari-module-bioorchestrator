declare module '@nekazari/sdk' {
  export interface NKZClientOptions {
    baseUrl?: string;
    getToken?: () => string | undefined;
    getTenantId?: () => string | undefined;
  }

  export class NKZClient {
    constructor(options: NKZClientOptions);
    get<T = any>(path: string, config?: any): Promise<T>;
    post<T = any>(path: string, data?: any, config?: any): Promise<T>;
    put<T = any>(path: string, data?: any, config?: any): Promise<T>;
    delete<T = any>(path: string, config?: any): Promise<T>;
  }

  export interface AuthContextValue {
    user: any;
    token: string | undefined;
    tenantId: string | undefined;
    isAuthenticated: boolean;
    hasRole: (role: string) => boolean;
    hasAnyRole: (roles: string[]) => boolean;
    getToken: () => string | undefined;
  }

  export function useAuth(): AuthContextValue;

  export interface TranslationContextValue {
    t: (key: string, params?: Record<string, any>) => string;
    i18n: { language: string };
  }

  export function useTranslation(namespace?: string): TranslationContextValue;

  export const i18n: {
    addResourceBundle(
      lng: string,
      ns: string,
      resources: Record<string, unknown>,
      deep?: boolean,
      overwrite?: boolean,
    ): unknown;
  };

  export interface ViewerContextValue {
    selectedEntityId: string | null;
    selectedEntityType: string | null;
    currentDate: Date;
    isTimelinePlaying: boolean;
    activeLayers: Set<string>;
    setSelectedEntity: (id: string | null, type?: string | null) => void;
    setCurrentDate: (date: Date) => void;
  }

  export function useViewer(): ViewerContextValue;
  export function useViewerOptional(): ViewerContextValue | null;
}
