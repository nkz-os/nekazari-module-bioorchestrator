import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge } from '@nekazari/ui-kit';
import { TrendingUp, Mountain, ShieldCheck, AlertTriangle } from 'lucide-react';
import RecommendationTrustBadge from './RecommendationTrustBadge';

interface Props {
  suggestion: any;
}

const CropExpandPanel: React.FC<Props> = ({ suggestion }) => {
  const { t } = useTranslation('bioorchestrator');
  const [tab, setTab] = React.useState<'varieties' | 'soil' | 'trust'>('varieties');

  const tabs = [
    { id: 'varieties' as const, icon: TrendingUp, label: t('varietyFinder.rankedVarieties', { defaultValue: 'Variedades' }) },
    { id: 'soil' as const, icon: Mountain, label: t('soilPanel.title', { defaultValue: 'Suelo' }) },
    { id: 'trust' as const, icon: ShieldCheck, label: t('planning.trustWhy', { defaultValue: 'Fiabilidad' }) },
  ];

  return (
    <div className="mt-2 pt-2 border-t border-nkz-border-subtle">
      {/* Tab bar */}
      <div className="flex gap-1 mb-2">
        {tabs.map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`flex items-center gap-1 px-2 py-1 rounded text-nkz-xs font-medium transition-colors ${
              tab === t.id ? 'bg-nkz-accent-base text-nkz-text-on-accent' : 'text-nkz-text-secondary hover:text-nkz-text-primary'
            }`}
          >
            <t.icon className="w-3 h-3" />
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === 'varieties' && (
        <div className="text-nkz-xs space-y-1">
          <div className="flex items-center gap-2">
            <span className="text-nkz-text-secondary">{t('varietyFinder.variety', { defaultValue: 'Variedad' })}:</span>
            <span className="font-medium text-nkz-text-primary">{suggestion.best_variety || suggestion.crop_eppo}</span>
          </div>
          {suggestion.agronomics?.confidence_interval && (
            <div className="flex items-center gap-2">
              <span className="text-nkz-text-secondary">IC 95%:</span>
              <span className="text-nkz-text-primary">{suggestion.agronomics.confidence_interval[0]?.toLocaleString()} – {suggestion.agronomics.confidence_interval[1]?.toLocaleString()} kg/ha</span>
            </div>
          )}
          <div className="flex items-center gap-2">
            <span className="text-nkz-text-secondary">{t('varietyFinder.trials', { defaultValue: 'Ensayos' })}:</span>
            <Badge intent="info">{suggestion.agronomics?.trials_analyzed}</Badge>
          </div>
          {suggestion.thermal_risk !== 'none' && (
            <div className="flex items-center gap-1 text-nkz-text-warning">
              <AlertTriangle className="w-3 h-3" />
              <span>{t('planning.rotationWarning', { defaultValue: 'Riesgo térmico' })}: {suggestion.thermal_risk}</span>
            </div>
          )}
        </div>
      )}

      {tab === 'soil' && (
        <div className="text-nkz-xs space-y-1">
          <div className="flex items-center gap-2">
            <span className="text-nkz-text-secondary">{t('soilPanel.title', { defaultValue: 'Compatibilidad' })}:</span>
            <Badge intent={suggestion.suitability?.overall === 'suitable' ? 'positive' : 'warning'}>
              {suggestion.suitability?.overall === 'suitable' ? '✓ Adecuado' : '⚠ Con avisos'}
            </Badge>
          </div>
          {suggestion.suitability?.warnings?.length > 0 && (
            <div className="space-y-0.5 mt-1">
              {suggestion.suitability.warnings.map((w: string, i: number) => (
                <div key={i} className="flex items-center gap-1 text-nkz-text-warning">
                  <AlertTriangle className="w-3 h-3" />
                  <span>{w}</span>
                </div>
              ))}
            </div>
          )}
          {suggestion.water_demand && (
            <div className="flex items-center gap-2 mt-1">
              <span className="text-nkz-text-secondary">💧 Demanda hídrica:</span>
              <Badge intent={suggestion.water_demand.level === 'low' ? 'positive' : suggestion.water_demand.level === 'medium' ? 'warning' : 'negative'}>
                {suggestion.water_demand.level === 'low' ? 'Baja' : suggestion.water_demand.level === 'medium' ? 'Media' : 'Alta'}
                {' '}({suggestion.water_demand.season_etc_mm} mm ETc)
              </Badge>
            </div>
          )}
        </div>
      )}

      {tab === 'trust' && (
        <div className="text-nkz-xs">
          <RecommendationTrustBadge trust={suggestion.recommendation_trust} />
        </div>
      )}
    </div>
  );
};

export default CropExpandPanel;
