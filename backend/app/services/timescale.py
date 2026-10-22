"""Helpers for per-parcel data series (trend computation).

Vegetation/climate series are now read directly from `EOProduct` entities in the
broker (one entity per sensingDate), so bioorch no longer queries TimescaleDB.
"""


def compute_trend(observations: list[dict]) -> dict:
    """Compute simple trend from observation series."""
    if len(observations) < 2:
        return {"direction": "stable", "delta": 0.0, "label": "Sin tendencia (datos insuficientes)"}

    first = observations[0]["value"]
    last = observations[-1]["value"]
    delta = round(last - first, 4)

    if delta > 0.02:
        direction = "up"
        label = f"+{delta:.2f} desde {observations[0]['date']}"
    elif delta < -0.02:
        direction = "down"
        label = f"{delta:.2f} desde {observations[0]['date']}"
    else:
        direction = "stable"
        label = "estable"

    return {"direction": direction, "delta": delta, "label": label}
