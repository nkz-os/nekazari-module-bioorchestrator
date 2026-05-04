import { defineConfig } from 'vite';
import { nkzModulePreset } from '@nekazari/module-builder';

export default defineConfig(
    nkzModulePreset({
        moduleId: 'bioorchestrator',
        entry: 'src/moduleEntry.ts',
        additionalExternals: {
            '@nekazari/design-tokens': '__NKZ_DESIGN_TOKENS__',
            '@nekazari/viewer-kit': '__NKZ_VIEWER_KIT__',
        },
        viteConfig: {
            server: {
                port: 5174,
                proxy: {
                    '/api/bioorchestrator': {
                        target: 'http://localhost:8420',
                        rewrite: (path) => path.replace('/api/bioorchestrator', ''),
                    },
                },
            },
        },
    }),
);
