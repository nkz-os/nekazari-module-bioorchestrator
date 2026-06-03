import React from "react";
import { useTranslation } from "react-i18next";

export default function DisclaimerFooter() {
  const { t } = useTranslation();
  return (
    <div style={{
      padding: "8px 12px", fontSize: 11, color: "#999",
      background: "#fafafa", borderTop: "1px solid #f0f0f0",
      textAlign: "center", marginTop: 16,
    }}>
      ⚠️ {t("disclaimer.text")}
    </div>
  );
}
