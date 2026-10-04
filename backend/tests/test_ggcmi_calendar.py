"""GGCMI Phase 3 crop calendar lookup (pan-EU, 0.5° grid)."""
import json

import pytest

from app.services import ggcmi_calendar as g

MADRID, PARIS, WARSAW, DUBLIN = (40.4, -3.7), (48.85, 2.35), (52.23, 21.0), (53.35, -6.26)


def test_default_file_is_backend_data_and_cites_source():
    assert g.DEFAULT_PATH.parts[-2:] == ("data", "ggcmi_calendar_europe.json")
    doc = g.load()
    assert doc["doi"] == "10.5281/zenodo.5062513"
    assert doc["license"] == "CC BY 4.0"
    assert doc["grid"] == {"lat_north": 72.0, "lon_west": -32.0, "res": 0.5, "nrows": 90, "ncols": 154}


def test_every_mapped_crop_has_both_regimes():
    layers = g.load()["layers"]
    for crop in set(g.EPPO_TO_GGCMI.values()) | set(g.WHEAT_FALLBACK.values()):
        for regime in ("rf", "ir"):
            assert f"{crop}_{regime}" in layers


@pytest.mark.parametrize("point,doy", [(MADRID, 332), (PARIS, 304), (WARSAW, 276)])
def test_winter_wheat_rainfed_planting_at_capitals(point, doy):
    # These rainfed cells are SAGE-sourced.
    out = g.lookup("TRZAX", *point, irrigation=None)
    assert out["planting_doy"] == doy
    assert out["maturity_doy"] is not None and out["cycle_days"] is not None
    assert out["source"] == g.citation("SAGE")


def test_mirca_cells_give_none():
    # Dublin winter wheat (rf and ir), Madrid irrigated winter wheat and rainfed
    # barley everywhere are MIRCA2000 cells in GGCMI: dropped, no calendar.
    assert g.lookup("TRZAX", *DUBLIN, irrigation=None) is None
    assert g.lookup("TRZAX", *DUBLIN, irrigation="regadío") is None
    for point in (MADRID, PARIS, WARSAW, DUBLIN):
        assert g.lookup("HORVX", *point, irrigation=None) is None


def test_irrigated_wheat_uses_rainfed_winter_wheat_not_spring_wheat():
    # Madrid irrigated winter wheat is MIRCA-sourced (dropped); rainfed winter
    # wheat there is SAGE (day 332) and wins over spring wheat (day 94).
    out = g.lookup("TRZAX", *MADRID, irrigation="regadío")
    assert out["planting_doy"] == 332 and out["layer"] == "rf" and out["rainfed_fallback"] is True
    assert out["source"] == g.citation("SAGE", rainfed_fallback=True)


def test_citation_names_the_underlying_dataset():
    assert g.citation("SAGE") == ("GGCMI Phase 3 crop calendar (SAGE), Jägermeyr et al. 2021, CC BY 4.0, "
                                  "doi:10.5281/zenodo.5062513")
    assert g.lookup("ZEAMX", *PARIS)["source"].startswith("GGCMI Phase 3 crop calendar (Iizumi et al. 2019)")


def test_file_lists_only_allowed_sources():
    assert g.load()["data_sources"] == {"2": "SAGE", "3": "Iizumi et al. 2019", "4": "RiceAtlas",
                                        "5": "Dimou et al. 2018"}


def test_sea_point_is_none():
    assert g.lookup("TRZAX", 45.0, -20.0, irrigation=None) is None  # mid-Atlantic
    assert g.lookup("TRZAX", 72.0, 0.0, irrigation=None) is None  # north edge row, Norwegian Sea


@pytest.mark.parametrize("lat,lon", [(26.9, 0.0), (72.1, 0.0), (50.0, -32.1), (50.0, 45.0), (None, 0.0)])
def test_outside_box_or_missing_coordinate_is_none(lat, lon):
    assert g.lookup("TRZAX", lat, lon, irrigation=None) is None


@pytest.mark.parametrize("lat,lon", [(float("nan"), 2.35), (48.85, float("nan")), (float("inf"), 0.0),
                                     (48.85, float("-inf"))])
def test_non_finite_coordinate_is_none(lat, lon):
    assert g.lookup("TRZAX", lat, lon, irrigation=None) is None


@pytest.mark.parametrize("eppo", ["AVESA", "TTLSS", "CIEAR", "LENCU", "VICFX", "VICER", "VICSA",
                                  "LTHSA", "MEDSA", "LYPES", "ZZZZZ"])
def test_unmapped_crops_have_no_calendar(eppo):
    assert g.lookup(eppo, *PARIS, irrigation=None) is None


def test_mapping_is_the_ruled_one():
    assert g.EPPO_TO_GGCMI == {
        "HORVX": "bar", "ZEAMX": "mai", "HELAN": "sun", "BRSNN": "rap", "BRPNA": "rap",
        "SECCE": "rye", "SOLTU": "pot", "PISSA": "pea", "PIBSX": "pea", "BETVU": "sgb",
        "GLXMA": "soy", "ORYSA": "ri1", "TRZAX": "wwh", "TRZAW": "wwh", "TRZDU": "wwh",
    }
    assert "bea" not in g.EPPO_TO_GGCMI.values()


# --- synthetic grid: regime choice and wheat fallback -------------------------

def _doc(layers):
    return {"grid": {"lat_north": 50.0, "lon_west": 0.0, "res": 0.5, "nrows": 2, "ncols": 2},
            "data_sources": {"2": "SAGE", "3": "Iizumi et al. 2019"}, "layers": layers}


def _trim(full):
    """Full grid rows -> the file's trimmed rows ``[first_col, values]``."""
    out = []
    for row in full:
        kept = [i for i, v in enumerate(row) if v is not None]
        out.append([kept[0], row[kept[0]:kept[-1] + 1]] if kept else [0, []])
    return out


def _layer(p, m, c, src=None):
    """Kept cells default to source 2 (SAGE); pass ``src`` for dropped (e.g. 1 MIRCA) cells."""
    if src is None:
        src = [[None if v is None else 2 for v in row] for row in p]
    return {"planting_day": _trim(p), "maturity_day": _trim(m), "growing_season_length": _trim(c),
            "data_source": _trim(src)}


def _cell(v):
    return [[v, None], [None, None]]  # only the north-west cell (49.5..50, 0..0.5) has data


def test_regadio_reads_irrigated_layer_and_secano_or_unknown_rainfed():
    doc = _doc({"mai_rf": _layer(_cell(120), _cell(250), _cell(130)),
                "mai_ir": _layer(_cell(110), _cell(260), _cell(150))})
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation="regadío", data=doc)["planting_doy"] == 110
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation="secano", data=doc)["planting_doy"] == 120
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation=None, data=doc)["cycle_days"] == 130


def test_wheat_falls_back_to_spring_wheat_when_winter_pixel_missing():
    doc = _doc({"wwh_rf": _layer(_cell(None), _cell(None), _cell(None)),
                "swh_rf": _layer(_cell(80), _cell(200), _cell(120))})
    out = g.lookup("TRZDU", 49.8, 0.2, irrigation=None, data=doc)
    assert out == {"planting_doy": 80, "maturity_doy": 200, "cycle_days": 120, "source": g.citation("SAGE"),
                   "layer": "rf", "rainfed_fallback": False}


def test_citation_marks_rainfed_fallback():
    assert g.citation("SAGE", rainfed_fallback=True) == (
        "GGCMI Phase 3 crop calendar (SAGE; rainfed calendar used for irrigated parcel), "
        "Jägermeyr et al. 2021, CC BY 4.0, doi:10.5281/zenodo.5062513")


# --- irrigated parcel: ir layer, else rf layer --------------------------------

def _empty():
    return _layer(_cell(None), _cell(None), _cell(None))


def test_irrigated_uses_ir_when_present():
    doc = _doc({"mai_ir": _layer(_cell(110), _cell(260), _cell(150)),
                "mai_rf": _layer(_cell(120), _cell(250), _cell(130))})
    out = g.lookup("ZEAMX", 49.8, 0.2, irrigation="regadío", data=doc)
    assert out["planting_doy"] == 110 and out["layer"] == "ir" and out["rainfed_fallback"] is False
    assert out["source"] == g.citation("SAGE")


def test_irrigated_falls_back_to_rf_when_ir_missing():
    doc = _doc({"mai_ir": _empty(), "mai_rf": _layer(_cell(120), _cell(250), _cell(130))})
    out = g.lookup("ZEAMX", 49.8, 0.2, irrigation="regadío", data=doc)
    assert out["planting_doy"] == 120 and out["layer"] == "rf" and out["rainfed_fallback"] is True
    assert "rainfed calendar used for irrigated parcel" in out["source"]


def test_irrigated_falls_back_to_rf_when_ir_layer_absent():
    doc = _doc({"mai_rf": _layer(_cell(120), _cell(250), _cell(130))})
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation="regadío", data=doc)["layer"] == "rf"


def test_irrigated_dropped_ir_with_allowed_rf_uses_rf():
    doc = _doc({"mai_ir": _layer(_cell(None), _cell(None), _cell(None), src=_cell(1)),
                "mai_rf": _layer(_cell(120), _cell(250), _cell(130))})
    out = g.lookup("ZEAMX", 49.8, 0.2, irrigation="regadío", data=doc)
    assert out["planting_doy"] == 120 and out["rainfed_fallback"] is True


@pytest.mark.parametrize("ir_src,rf_src", [(None, None), (1, None), (None, 1), (1, 1)])
def test_irrigated_none_when_neither_layer_has_an_allowed_value(ir_src, rf_src):
    doc = _doc({"mai_ir": _layer(_cell(None), _cell(None), _cell(None), src=_cell(ir_src)),
                "mai_rf": _layer(_cell(None), _cell(None), _cell(None), src=_cell(rf_src))})
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation="regadío", data=doc) is None


def test_rainfed_parcel_never_reads_ir():
    doc = _doc({"mai_ir": _layer(_cell(110), _cell(260), _cell(150)), "mai_rf": _empty()})
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation="secano", data=doc) is None
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation=None, data=doc) is None


def test_irrigated_wheat_order_wwh_ir_then_wwh_rf_then_swh():
    swh = {"swh_ir": _layer(_cell(70), _cell(190), _cell(120)),
           "swh_rf": _layer(_cell(80), _cell(200), _cell(120))}
    wwh_rf = _layer(_cell(300), _cell(190), _cell(255))
    # wwh ir missing, wwh rf present -> wwh rf
    doc = _doc({"wwh_ir": _empty(), "wwh_rf": wwh_rf, **swh})
    assert g.lookup("TRZAX", 49.8, 0.2, irrigation="regadío", data=doc)["planting_doy"] == 300
    # wwh has no calendar at all -> swh ir
    doc = _doc({"wwh_ir": _empty(), "wwh_rf": _empty(), **swh})
    out = g.lookup("TRZAX", 49.8, 0.2, irrigation="regadío", data=doc)
    assert out["planting_doy"] == 70 and out["layer"] == "ir"
    # wwh dropped in both layers -> None (no swh)
    dropped = _layer(_cell(None), _cell(None), _cell(None), src=_cell(1))
    doc = _doc({"wwh_ir": dropped, "wwh_rf": dropped, **swh})
    assert g.lookup("TRZAX", 49.8, 0.2, irrigation="regadío", data=doc) is None
    # wwh ir dropped, wwh rf missing -> None (dropped wheat is still wheat)
    doc = _doc({"wwh_ir": dropped, "wwh_rf": _empty(), **swh})
    assert g.lookup("TRZAX", 49.8, 0.2, irrigation="regadío", data=doc) is None


def test_mirca_cell_is_none():
    doc = _doc({"mai_rf": _layer(_cell(None), _cell(None), _cell(None), src=_cell(1))})
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation=None, data=doc) is None


def test_unlisted_source_index_is_none_even_with_values():
    # Defence in depth: a value whose source is not an allowed dataset is never served.
    doc = _doc({"mai_rf": _layer(_cell(120), _cell(250), _cell(130), src=_cell(9))})
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation=None, data=doc) is None


def test_allowlist_is_the_code_constant_not_the_file_map():
    # A file whose map lists MIRCA (1) still never serves a MIRCA cell.
    doc = _doc({"mai_rf": _layer(_cell(120), _cell(250), _cell(130), src=_cell(1))})
    doc["data_sources"] = {**doc["data_sources"], "1": "MIRCA2000"}
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation=None, data=doc) is None
    assert set(g.ALLOWED_SOURCES) == {2, 3, 4, 5}


def test_dropped_winter_wheat_cell_blocks_spring_wheat_fallback():
    doc = _doc({"wwh_rf": _layer(_cell(None), _cell(None), _cell(None), src=_cell(1)),
                "swh_rf": _layer(_cell(80), _cell(200), _cell(120))})
    assert g.lookup("TRZAX", 49.8, 0.2, irrigation=None, data=doc) is None


def test_source_names_dataset_of_the_cell():
    doc = _doc({"mai_rf": _layer(_cell(120), _cell(250), _cell(130), src=_cell(3))})
    assert g.lookup("ZEAMX", 49.8, 0.2, data=doc)["source"] == g.citation("Iizumi et al. 2019")


def test_winter_wheat_pixel_wins_over_spring_wheat():
    doc = _doc({"wwh_rf": _layer(_cell(300), _cell(190), _cell(255)),
                "swh_rf": _layer(_cell(80), _cell(200), _cell(120))})
    assert g.lookup("TRZAX", 49.8, 0.2, irrigation=None, data=doc)["planting_doy"] == 300


def test_cell_index_uses_cell_edges():
    doc = _doc({"mai_rf": _layer([[1, 2], [3, 4]], [[1, 2], [3, 4]], [[1, 2], [3, 4]])})
    pick = lambda lat, lon: g.lookup("ZEAMX", lat, lon, irrigation=None, data=doc)["planting_doy"]
    assert pick(50.0, 0.0) == 1
    assert pick(49.51, 0.49) == 1
    assert pick(49.49, 0.51) == 4
    assert g.lookup("ZEAMX", 49.0, 0.2, irrigation=None, data=doc) is None  # south edge is outside


def test_trimmed_rows_keep_column_positions():
    doc = _doc({"mai_rf": _layer([[None, 7], [None, None]], [[None, 8], [None, None]],
                                 [[None, 9], [None, None]])})
    assert doc["layers"]["mai_rf"]["planting_day"] == [[1, [7]], [0, []]]
    assert g.lookup("ZEAMX", 49.8, 0.7, irrigation=None, data=doc)["cycle_days"] == 9
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation=None, data=doc) is None
    assert g.lookup("ZEAMX", 49.2, 0.7, irrigation=None, data=doc) is None


def test_missing_layer_is_none():
    assert g.lookup("ZEAMX", 49.8, 0.2, irrigation=None, data=_doc({})) is None


@pytest.mark.parametrize("doy,season", [(1, "spring"), (151, "spring"), (152, "summer"), (243, "summer"),
                                        (244, "autumn"), (304, "autumn"), (365, "autumn"), (366, "autumn")])
def test_sowing_type_from_planting_month(doy, season):
    # Non-leap calendar: DOY 151 = 31 May, 152 = 1 Jun, 243 = 31 Aug, 244 = 1 Sep.
    assert g.sowing_type_from_doy(doy) == season


def test_load_reads_given_path(tmp_path):
    p = tmp_path / "c.json"
    p.write_text(json.dumps(_doc({})))
    assert g.load(p)["grid"]["nrows"] == 2
