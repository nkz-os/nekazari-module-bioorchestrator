"""TimescaleDB query helper for telemetry data."""
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

from app.core.config import settings

PERIOD_MAP = {
    "1m": timedelta(days=30),
    "3m": timedelta(days=90),
    "6m": timedelta(days=180),
    "1y": timedelta(days=365),
}


def _connect():
    """Connect to TimescaleDB. DSN is mandatory (K8s secret) — no localhost fallback."""
    dsn = settings.timescale_dsn
    if not dsn:
        raise RuntimeError(
            "TIMESCALE_DSN is required (no localhost fallback). "
            "Set it via the K8s secret."
        )
    return psycopg2.connect(dsn, connect_timeout=10)


def query_vegetation_timeseries(
    entity_id: str,
    attr_name: str,
    period: str,
    since: datetime | None = None,
) -> list[dict]:
    """Query vegetation index time series from TimescaleDB.

    Args:
        entity_id: NGSI-LD entity ID (e.g. VegetationIndex:{tenant}:{parcel})
        attr_name: Attribute name (e.g. ndviMean)
        period: Preset key (1m, 3m, 6m, 1y) or 'season'
        since: Override start date (used for 'season' period)

    Returns list of {date, value}.
    """
    if period == "season" and since is not None:
        start_date = since
    else:
        delta = PERIOD_MAP.get(period, PERIOD_MAP["3m"])
        start_date = datetime.utcnow() - delta

    conn = _connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT
                    time_bucket('1 day', time) AS day,
                    AVG(value) AS value
                FROM telemetry
                WHERE entity_id = %s
                  AND attr_name = %s
                  AND time >= %s
                GROUP BY day
                ORDER BY day ASC
                """,
                (entity_id, attr_name, start_date),
            )
            rows = cur.fetchall()

            return [
                {"date": row["day"].strftime("%Y-%m-%d"), "value": round(float(row["value"]), 4)}
                for row in rows
            ]
    finally:
        conn.close()


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
