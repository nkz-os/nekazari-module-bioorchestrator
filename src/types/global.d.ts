import type { NKZClient, AuthContextValue } from '@nekazari/sdk';

declare global {
  interface Window {
    __NKZ_SDK__: {
      NKZClient: typeof NKZClient;
      useAuth: () => AuthContextValue;
      useTranslation: (ns?: string) => { t: (key: string, params?: Record<string, any>) => string; i18n: { language: string } };
      useViewer: () => any;
      i18n: {
        addResourceBundle(lng: string, ns: string, resources: Record<string, unknown>, deep?: boolean, overwrite?: boolean): unknown;
      };
    };

    __NKZ_UI__: {
      Card: any;
      Button: any;
      Badge: any;
      Spinner: any;
      Stack: any;
      Inline: any;
      Input: any;
      Select: any;
      Tabs: any;
      Panel: any;
      Surface: any;
      DataTable: any;
      MetricCard: any;
      MetricGrid: any;
      EmptyState: any;
      Skeleton: any;
      ProgressBar: any;
      IconButton: any;
      FormGrid: any;
      FormField: any;
      DetailGrid: any;
      DetailItem: any;
    };

    __NKZ_VIEWER_KIT__: {
      SlotShell: any;
      SlotShellCompact: any;
    };

    __NKZ__: {
      register: (registration: {
        id: string;
        viewerSlots?: any[];
        main?: any;
        provider?: any;
        version?: string;
      }) => void;
      getRegistered: (id: string) => any;
      getRegisteredIds: () => string[];
    };
  }
}

export {};
