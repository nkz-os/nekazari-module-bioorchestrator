"""El modelo declarado debe coincidir con el que realmente se ejecuta."""
from datetime import date

from app.services.wofost_service import _run_fallback_simulation


def _weather(days: int = 30) -> list[dict]:
    return [
        {"tmin": 8.0, "tmax": 22.0, "precip": 1.0, "eto": 3.5}
        for _ in range(days)
    ]


def test_fallback_does_not_claim_pcse_is_unavailable():
    """PCSE está instalado en producción: el mensaje anterior era falso."""
    result = _run_fallback_simulation(
        crop_slug="wheat",
        sowing_date=date(2026, 3, 1),
        weather_data=_weather(),
        soil_hydraulic_props={"awc_mm_per_metre": 140},
    )
    assert "unavailable" not in result["model"].lower()
    assert result["model"] == "FAO-33 simplified (PCSE integration not operational)"
    assert result["method"] == "empirical"


def test_fallback_returns_stress_factor_not_absolute_yield():
    """La reserva devuelve un factor de estrés; el rendimiento lo pone quien llama."""
    result = _run_fallback_simulation(
        crop_slug="wheat",
        sowing_date=date(2026, 3, 1),
        weather_data=_weather(),
        soil_hydraulic_props={"awc_mm_per_metre": 140},
    )
    assert result["projected_yield_kg_ha"] is None
    assert 0.0 < result["total_stress_factor"] <= 1.0
