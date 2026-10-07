"""Read-only collector of the API outputs that depend on the trial graph (plan task 11).

``collect(driver)`` runs the app's own DAO / endpoint functions for a fixed set of synthetic parcels and
returns one JSON-safe dict keyed by call. The same module runs unchanged against a local build graph and
against a graph the operator may only read: every session is wrapped so that a query containing a write
clause is refused before it is sent.

The parcel set is data, not a deployment: Köppen class + ISO country + crop, nothing else.
"""
from __future__ import annotations

import json
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from typing import Any

from neo4j import READ_ACCESS

PARCELS: tuple[tuple[str, str], ...] = (  # (Köppen class, ISO country)
    ("Csa", "ES"), ("Csb", "ES"), ("BSk", "ES"), ("Cfb", "ES"), ("Dfb", "ES"), ("Cfa", "ES"),
    ("Csa", "IT"), ("Cfa", "IT"), ("Cfb", "IT"), ("Dfb", "IT"),
    ("Cfb", "FR"),
)
CROPS: tuple[str, ...] = ("HORVX", "TRZAX", "ZEAMX", "BRSNN")
IRRIGATION: tuple[str | None, ...] = (None, "secano", "regadío")

_WRITE = re.compile(r"\b(CREATE|MERGE|SET|DELETE|REMOVE|DROP|FOREACH)\b|LOAD\s+CSV|CALL\s+(apoc|db\.create|dbms)", re.IGNORECASE)


class ReadOnlyViolation(RuntimeError):
    pass


def _strip_strings(cypher: str) -> str:
    return re.sub(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"", "''", cypher)


class _Session:
    def __init__(self, inner: Any):
        self._inner = inner

    async def __aenter__(self):
        self._s = await self._inner.__aenter__()
        return self

    async def __aexit__(self, *exc):
        return await self._inner.__aexit__(*exc)

    async def run(self, query, *a, **k):
        text = query if isinstance(query, str) else str(query)
        if _WRITE.search(_strip_strings(text)):
            raise ReadOnlyViolation(text[:120])
        return await self._s.run(query, *a, **k)

    def __getattr__(self, name):
        if name in ("begin_transaction", "execute_write", "write_transaction"):
            raise ReadOnlyViolation(name)
        return getattr(self._s, name)


class ReadOnlyDriver:
    """Duck-typed driver: ``session()`` always READ_ACCESS, writes refused client side."""

    def __init__(self, driver: Any):
        self._driver = driver

    def session(self, **kw):
        kw["default_access_mode"] = READ_ACCESS
        return _Session(self._driver.session(**kw))

    async def close(self):
        await self._driver.close()


def _json(obj: Any) -> Any:
    return json.loads(json.dumps(obj, default=str, sort_keys=True))


class _Cond:
    """Stand-in for ``app.api.v1.recommend._Conditions`` (same attribute names)."""

    def __init__(self, climate_class: str, irrigation: str | None, purpose: str):
        self.climate_class = climate_class
        self.soil_type = self.soil_ph = self.soil_texture = None
        self.irrigation_regime = irrigation
        self.management = "any"
        self.season = "all"
        self.purpose = purpose
        self.annual_rainfall_mm = self.annual_et0_mm = self.coldest_month_min_c = self.annual_temp_c = None
        self.frost_margin_c = None


async def collect(driver: Any, *, progress=None) -> dict[str, Any]:
    from app.api.v1 import recommend as rec_api
    from app.graph import dao as dao_mod

    ro = ReadOnlyDriver(driver)
    dao = dao_mod.GraphDAO(ro)
    out: dict[str, Any] = {}

    async def call(key: str, coro):
        try:
            out[key] = _json(await coro)
        except Exception as exc:  # noqa: BLE001 — the failure is itself a result to compare
            out[key] = {"__error__": f"{type(exc).__name__}: {str(exc)[:200]}"}
        if progress:
            progress(key)

    await call("trial-sites", dao.get_trial_sites_summary())
    await call("crops", dao.get_available_crops())
    for climate, country in PARCELS:
        pk = f"{country}-{climate}"
        for purpose in ("main", "forage"):
            for irr in IRRIGATION:
                if purpose == "forage" and irr is not None:
                    continue
                dao_mod._RECOMMEND_CACHE.clear()
                await call(f"recommend|{pk}|{purpose}|{irr}", dao.recommend_for_conditions({
                    "climate_class": climate, "country": country, "irrigation_regime": irr,
                    "management": "any", "season": "all", "purpose": purpose,
                    "crops": list(CROPS), "top_n": 10}))
        await call(f"similar-sites|{pk}", dao.get_similar_sites(climate_class=climate, limit=20))
        await call(f"similar-sites-agg|{pk}", dao.get_similar_sites(
            climate_class=climate, limit=None, include_aggregate=True, country=country))
        for crop in CROPS:
            await call(f"variety-trials|{pk}|{crop}", dao.get_variety_trials(
                crop=crop, climate_class=climate, limit=50))
            await call(f"extrapolate|{pk}|{crop}", dao.extrapolate_varieties(
                crop=crop, climate_class=climate, top_n=10))
            for tier in ("field", "regional"):
                await call(f"evidence|{pk}|{crop}|{tier}", rec_api.recommend_evidence(
                    driver=ro, cond=_Cond(climate, None, "main"), crop=crop, variety=None, page=1,
                    page_size=50, similarity="koppen", tier=tier, country=None))
            # the endpoint with the parcel's country (the regional list of the recommendation)
            await call(f"evidence-country|{pk}|{crop}|regional", rec_api.recommend_evidence(
                driver=ro, cond=_Cond(climate, None, "main"), crop=crop, variety=None, page=1,
                page_size=50, similarity="koppen", tier="regional", country=country))
    return out


# ═════════════════════════════════════════════════════════════════════════════
# the same calls through the public HTTP API of a deployed instance (GET only)
# ═════════════════════════════════════════════════════════════════════════════

def fetch_http(base: str, *, pause_s: float = 0.2, progress=None) -> dict[str, Any]:
    """Same keys as ``collect`` (minus the DAO-only ones) through ``{base}/agriculture/...`` (GET, no auth)."""
    out: dict[str, Any] = {}
    ctx = ssl.create_default_context()

    def get(key: str, path: str, **query: Any) -> None:
        url = base.rstrip("/") + path + "?" + urllib.parse.urlencode({k: v for k, v in query.items() if v is not None})
        for attempt in range(4):
            try:
                with urllib.request.urlopen(url, timeout=120, context=ctx) as response:
                    out[key] = json.loads(response.read())
                break
            except urllib.error.HTTPError as exc:
                if exc.code == 429:
                    time.sleep(5 * (attempt + 1))
                    continue
                out[key] = {"__error__": f"HTTP {exc.code}"}
                break
            except (urllib.error.URLError, TimeoutError, ValueError) as exc:
                out[key] = {"__error__": f"{type(exc).__name__}: {exc}"}
                break
        time.sleep(pause_s)
        if progress:
            progress(key)

    get("trial-sites", "/agriculture/trial-sites")
    get("crops", "/agriculture/crops")
    for climate, country in PARCELS:
        pk = f"{country}-{climate}"
        for purpose in ("main", "forage"):
            for irr in IRRIGATION:
                if purpose == "forage" and irr is not None:
                    continue
                get(f"recommend|{pk}|{purpose}|{irr}", "/agriculture/recommend", climate_class=climate,
                    country=country, irrigation_regime=irr, purpose=purpose, crops=",".join(CROPS), top_n=10)
        get(f"similar-sites|{pk}", "/agriculture/similar-sites", climate_class=climate, limit=20)
        for crop in CROPS:
            get(f"variety-trials|{pk}|{crop}", "/agriculture/variety-trials", crop=crop, climate_class=climate,
                limit=50)
            get(f"extrapolate|{pk}|{crop}", "/agriculture/extrapolate", crop=crop, climate_class=climate, top_n=10)
            for tier in ("field", "regional"):
                get(f"evidence|{pk}|{crop}|{tier}", "/agriculture/recommend/evidence", climate_class=climate,
                    crop=crop, tier=tier, page_size=50)
    return out


# ═════════════════════════════════════════════════════════════════════════════
# comparison
# ═════════════════════════════════════════════════════════════════════════════

CROP_CELL = tuple[Any, ...]


def recommendation_cells(response: dict[str, Any]) -> dict[str, CROP_CELL]:
    """crop -> (tier, trial_count, expected_kg_ha, trust level, top variety) of a recommend response."""
    cells: dict[str, CROP_CELL] = {}
    for rec in response.get("recommendations") or []:
        yld = rec.get("yield") or {}
        cells[rec["crop"]["eppo"]] = (
            rec["evidence"].get("tier"), rec["evidence"].get("trial_count"), yld.get("expected_kg_ha"),
            rec["trust"]["level"], ((rec.get("varieties") or [{}])[0]).get("variety"))
    return cells


def classify_recommendations(prod: dict[str, Any], new: dict[str, Any]) -> tuple[Counter, list[tuple]]:
    """Every (parcel, purpose, irrigation, crop) cell of the recommend calls, by cause of difference.

    Causes (all intended by the rebuild or by the owner's decisions): ``irrigation`` = derived regimes gone and
    source-stated ones kept (filtered calls); ``zones`` = the zone/unlabelled aggregates of the country replace
    the city sites (both regional); ``coverage`` = a parcel the city climates did not reach now has the
    country's zone evidence; ``country`` = a parcel no longer gets another country's evidence (the reader
    matches aggregates by country). A cell none of them explains is returned in the second element.
    """
    causes: Counter = Counter()
    unexplained: list[tuple] = []
    for key in sorted(k for k in prod if k.startswith("recommend|")):
        _, _parcel, _purpose, irrigation = key.split("|")
        before, after = recommendation_cells(prod[key]), recommendation_cells(new[key])
        for crop in sorted(before.keys() | after.keys() | set(CROPS)):
            a, b = before.get(crop), after.get(crop)
            if a == b:
                causes["identical" if a else "both empty"] += 1
            elif irrigation != "None":
                causes["irrigation"] += 1
            elif a and b and a[0] == b[0] == "regional":
                causes["zones"] += 1
            elif a is None:
                causes["coverage"] += 1
            elif b is None:
                causes["country"] += 1
            else:
                unexplained.append((key, crop, a, b))
    return causes, unexplained
