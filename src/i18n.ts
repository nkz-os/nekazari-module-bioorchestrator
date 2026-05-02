import { i18n } from '@nekazari/sdk';
import en from './locales/en.json';
import es from './locales/es.json';
import ca from './locales/ca.json';
import eu from './locales/eu.json';
import fr from './locales/fr.json';
import pt from './locales/pt.json';

const BIO_NAMESPACE = 'bioorchestrator';

export function registerBioOrchestratorTranslations(): void {
  if (!i18n || typeof (i18n as any).addResourceBundle !== 'function') return;
  i18n.addResourceBundle('en', BIO_NAMESPACE, en, true, true);
  i18n.addResourceBundle('es', BIO_NAMESPACE, es, true, true);
  i18n.addResourceBundle('ca', BIO_NAMESPACE, ca, true, true);
  i18n.addResourceBundle('eu', BIO_NAMESPACE, eu, true, true);
  i18n.addResourceBundle('fr', BIO_NAMESPACE, fr, true, true);
  i18n.addResourceBundle('pt', BIO_NAMESPACE, pt, true, true);
}

registerBioOrchestratorTranslations();
