"""Reads of the nutrient-balance reference data (crop coefficients and regional yields)."""
from __future__ import annotations

from neo4j import AsyncDriver


class BalanceDao:
    def __init__(self, driver: AsyncDriver) -> None:
        self._driver = driver

    async def get_crop_coefficients(self, eppo: str) -> dict | None:
        async with self._driver.session() as s:
            result = await s.run(
                "MATCH (n:CropBalanceCoefficients {eppo: $eppo}) RETURN properties(n) AS props",
                eppo=eppo,
            )
            record = await result.single()
            return dict(record["props"]) if record else None

    async def get_regional_yields(self, eppo: str, nuts: str) -> list[dict]:
        async with self._driver.session() as s:
            result = await s.run(
                """
                MATCH (n:RegionalYieldStat {eppo: $eppo, nuts: $nuts})
                RETURN properties(n) AS props
                ORDER BY n.year
                """,
                eppo=eppo, nuts=nuts,
            )
            return [dict(r["props"]) for r in await result.data()]

    async def get_regional_yields_batch(self, eppos: list[str], nuts: list[str]) -> list[dict]:
        """Region and country rows of several crops in one query (`nuts` = [NUTS2 code, country code])."""
        async with self._driver.session() as s:
            result = await s.run(
                """
                MATCH (n:RegionalYieldStat)
                WHERE n.eppo IN $eppos AND n.nuts IN $nuts AND n.yieldTHa IS NOT NULL
                RETURN properties(n) AS props
                ORDER BY n.eppo, n.nuts, n.year
                """,
                eppos=eppos, nuts=nuts,
            )
            return [dict(r["props"]) for r in await result.data()]

    async def list_yield_regions(self, country: str) -> list[dict]:
        async with self._driver.session() as s:
            result = await s.run(
                """
                MATCH (n:RegionalYieldStat)
                WHERE n.nutsLevel = 2 AND n.nuts STARTS WITH $country AND n.yieldTHa IS NOT NULL
                RETURN n.nuts AS nuts, collect(DISTINCT n.eppo) AS eppos,
                       max(n.year) AS latest_year
                ORDER BY nuts
                """,
                country=country,
            )
            return [{"nuts": r["nuts"], "eppos": sorted(r["eppos"]), "latest_year": r["latest_year"]}
                    for r in await result.data()]
