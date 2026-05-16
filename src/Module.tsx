import { defineModule } from '@nekazari/module-kit';
import { lazy } from 'react';
import './i18n';
import { moduleSlots } from './slots';
import pkg from '../package.json';

const MainPage = lazy(() => import('./App'));

export default defineModule({
  id: 'bioorchestrator',
  displayName: 'BioOrchestrator',
  version: pkg.version,
  hostApiVersion: '^2.0.0',
  description: 'Biological pipeline orchestrator — Nekazari Platform Module',
  accent: { base: '#10B981', soft: '#D1FAE5', strong: '#047857' },
  icon: 'flask-conical',
  main: MainPage,
  slots: moduleSlots as never,
});
