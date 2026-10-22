import { defineConfig } from 'vite';
import { nkzModulePreset } from '@nekazari/module-builder';

export default defineConfig(
  nkzModulePreset({
    viteConfig: {
      server: {
        port: 5174,
        proxy: {
          '/api/bioorchestrator': {
            target: process.env.VITE_PROXY_TARGET || 'http://localhost:8420',
            rewrite: (path) => path.replace('/api/bioorchestrator', ''),
          },
        },
      },
    },
  }),
);
