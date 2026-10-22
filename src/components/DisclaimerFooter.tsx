import React from 'react';
import { useTranslation } from '@nekazari/sdk';

export default function DisclaimerFooter() {
  const { t } = useTranslation('bioorchestrator');
  return (
    <div className="pt-4 mt-4 border-t border-nkz-border text-center">
      <p className="text-nkz-xs text-nkz-text-muted">
        ⚠️ {t('disclaimer.text')}
      </p>
    </div>
  );
}
