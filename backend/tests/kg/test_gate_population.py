"""Population scans of the gate: scale, cloned and bimodal studies, near sites, cross-source copies (warnings)."""
from __future__ import annotations

import pytest

from app.kg.gate_population import (
    scan_cross_source,
    scan_population,
    scan_site_aliases,
)
from app.kg.model import SiteRow, UnitRow
from app.kg.registries import load_registries

REGISTRIES = load_registries()
STUDY_A = "a" * 64
STUDY_B = "b" * 64


def unit(i: int, y: float | None, *, crop="TRZAX", site="S1", year=2021, study=STUDY_A, irrigation=None,
         production=None, variety=None, source="GENVCE-TRIALS") -> UnitRow:
    return UnitRow.model_construct(
        source_id=source, document_key="d" * 64, crop_eppo=crop, raw_variety=f"V{i}", study_key=study,
        site_key=site, year=year, irrigation_regime=irrigation, production_system=production,
        variety_key=variety or f"{i:064x}", yield_kg_ha=y, factor_levels=())


def field(site_id: str, lat: float | None = None, lon: float | None = None, kind: str = "field") -> SiteRow:
    return SiteRow.model_construct(site_id=site_id, name=site_id, site_kind=kind, country="ES",
                                   latitude=lat, longitude=lon)


SITES = {f"S{i}": field(f"S{i}") for i in range(1, 12)}


def rules(hits):
    return sorted({h[0] for h in hits})


def environments(means: list[float], **kw) -> list[UnitRow]:
    """One unit per environment: the same crop at sites S1..Sn in one year."""
    return [unit(i, m, site=f"S{i + 1}", **kw) for i, m in enumerate(means)]


# ── scale_environment ────────────────────────────────────────────────────────

def test_an_environment_four_times_its_crop_is_a_scale_warning():
    hits = scan_population(environments([6000, 6100, 5900, 6050, 5950, 24000]), SITES)
    scale = [h for h in hits if h[0] == "scale_environment"]
    assert len(scale) == 1 and "S6" in scale[0][2]


def test_an_environment_twice_its_crop_or_a_crop_with_four_environments_is_not():
    assert "scale_environment" not in rules(scan_population(environments([6000, 6100, 5900, 6050, 12000]), SITES))
    assert "scale_environment" not in rules(scan_population(environments([6000, 6100, 5900, 24000]), SITES))


def test_organic_and_regional_environments_are_never_compared_with_conventional_field_ones():
    conventional = environments([6000, 6100, 5900, 6050, 5950])
    organic = [unit(9, 1500, site="S10", production="ecológico")]
    regional = [unit(10, 1500, site="R1")]
    sites = {**SITES, "R1": field("R1", kind="aggregate")}
    assert "scale_environment" not in rules(scan_population(conventional + organic + regional, sites))


# ── scale_block ──────────────────────────────────────────────────────────────

def block(year: int, value: float, start: int) -> list[UnitRow]:
    return [unit(start + i, value + 10 * i, year=year, study=f"{year:064d}") for i in range(5)]


def test_a_year_two_and_a_half_times_the_other_years_is_a_block_warning():
    hits = scan_population(block(2019, 6000, 0) + block(2020, 6100, 10) + block(2021, 15000, 20), SITES)
    assert [h[1] for h in hits if h[0] == "scale_block"] == ["TRZAX|2021|field|conventional"]


def test_a_block_of_four_units_or_units_without_year_are_not_judged():
    small = block(2019, 6000, 0) + block(2020, 6100, 10) + block(2021, 15000, 20)[:4]
    assert "scale_block" not in rules(scan_population(small, SITES))
    no_year = block(2019, 6000, 0) + [unit(30 + i, 15000, year=None) for i in range(5)]
    assert "scale_block" not in rules(scan_population(no_year, SITES))


# ── cloned_studies ───────────────────────────────────────────────────────────

def test_two_studies_with_the_same_five_yields_are_cloned():
    values = [5000, 5200, 5400, 5600, 5800]
    units = [unit(i, v, study=STUDY_A) for i, v in enumerate(values)]
    units += [unit(10 + i, v, study=STUDY_B, site="S2") for i, v in enumerate(reversed(values))]
    hits = [h for h in scan_population(units, SITES) if h[0] == "cloned_studies"]
    assert len(hits) == 2 and {h[2].split()[0] for h in hits} == {f"study:{STUDY_A}", f"study:{STUDY_B}"}


def test_four_shared_yields_are_not_a_clone():
    values = [5000, 5200, 5400, 5600]
    units = [unit(i, v, study=STUDY_A) for i, v in enumerate(values)]
    units += [unit(10 + i, v, study=STUDY_B) for i, v in enumerate(values)]
    assert "cloned_studies" not in rules(scan_population(units, SITES))


# ── bimodal_study ────────────────────────────────────────────────────────────

def test_a_study_split_in_two_groups_is_bimodal():
    values = [3000, 3010, 3020, 3030, 3040, 3050, 3060, 9000, 9010, 9020, 9030, 9040]
    hits = [h for h in scan_population([unit(i, v) for i, v in enumerate(values)], SITES) if h[0] == "bimodal_study"]
    assert len(hits) == 1 and hits[0][2].startswith(f"study:{STUDY_A}")


def test_eleven_values_or_a_smooth_study_are_not_bimodal():
    split = [3000, 3010, 3020, 3030, 3040, 3050, 9000, 9010, 9020, 9030, 9040]
    assert "bimodal_study" not in rules(scan_population([unit(i, v) for i, v in enumerate(split)], SITES))
    smooth = [5000 + 100 * i for i in range(14)]
    assert "bimodal_study" not in rules(scan_population([unit(i, v) for i, v in enumerate(smooth)], SITES))


# ── degenerate input ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("units", [[], [unit(0, 6000)], [unit(i, None) for i in range(20)], [unit(i, 0.0) for i in range(20)]])
def test_no_unit_one_unit_no_yields_or_zero_yields_raise_nothing(units):
    assert scan_population(units, SITES) == []


# ── field_sites_within_2km ───────────────────────────────────────────────────

def test_two_field_sites_one_km_apart_are_a_possible_alias_but_not_at_150_m_or_2_5_km():
    base = field("ES-TEST-A", 41.65, -4.72)
    near = {"ES-TEST-A": base, "ES-TEST-B": field("ES-TEST-B", 41.659, -4.72)}  # about 1 km north
    hits = scan_site_aliases(near, REGISTRIES)
    assert [h[1] for h in hits] == ["ES-TEST-A ~ ES-TEST-B"]
    closer = {"ES-TEST-A": base, "ES-TEST-B": field("ES-TEST-B", 41.65135, -4.72)}  # about 150 m
    farther = {"ES-TEST-A": base, "ES-TEST-B": field("ES-TEST-B", 41.6725, -4.72)}  # about 2.5 km
    assert scan_site_aliases(closer, REGISTRIES) == [] and scan_site_aliases(farther, REGISTRIES) == []


# ── cross_source_duplicate ───────────────────────────────────────────────────

class Others:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def other_source_yields(self, source_id):
        self.calls.append(source_id)
        return list(self.rows)


def test_the_same_yield_of_the_same_variety_crop_and_year_in_another_source_is_a_copy():
    units = [unit(1, 3304.0, variety="f" * 64), unit(2, 3400.0, variety="e" * 64)]
    graph = Others([("TRZAX", 2021, "f" * 64, 3304.04, "GENVCE"), ("TRZAX", 2022, "e" * 64, 3400.0, "GENVCE")])
    hits = scan_cross_source(units, graph)
    assert graph.calls == ["GENVCE-TRIALS"]
    assert len(hits) == 1 and hits[0][0] == "cross_source_duplicate" and hits[0][1] == "GENVCE"


def test_organic_and_conventional_units_of_one_study_are_never_pooled_in_the_study_scans():
    organic = [unit(i, 3000 + 10 * i, production="ecológico") for i in range(4)]
    conventional = [unit(10 + i, 5900 + 25 * i) for i in range(8)]
    assert "bimodal_study" not in rules(scan_population(organic + conventional, SITES))
