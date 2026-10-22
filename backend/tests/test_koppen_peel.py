"""Köppen–Geiger after Peel et al. (2007) from monthly normals."""
import pytest

from app.services.chelsa_climate import koppen_peel

# Monthly CHELSA v2.1 1981-2010 normals read on 2026-10-02 (spec §3 check).
OCEANIC_TAS = [4.95, 5.6, 8.4, 10.3, 14.0, 17.9, 20.4, 20.4, 17.4, 13.6, 8.6, 5.9]
OCEANIC_PR = [77, 65, 66, 92, 81, 58, 39, 33, 48, 81, 93, 94]
MED_HOT_TAS = [10.7, 12.2, 15.0, 17.0, 20.6, 25.0, 27.8, 27.6, 24.6, 20.0, 14.7, 11.5]
MED_HOT_PR = [67, 50, 40, 54, 32, 10, 2, 5, 25, 70, 89, 103]
STEPPE_TAS = [5.3, 6.8, 10.0, 12.2, 16.3, 21.9, 25.9, 25.4, 20.8, 15.0, 9.2, 6.1]
STEPPE_PR = [32, 34, 25, 45, 46, 21, 10, 11, 24, 55, 54, 53]


@pytest.mark.parametrize("tas,pr,expected", [
    (OCEANIC_TAS, OCEANIC_PR, "Cfb"),
    (MED_HOT_TAS, MED_HOT_PR, "Csa"),
    (STEPPE_TAS, STEPPE_PR, "BSk"),
])
def test_reference_points(tas, pr, expected):
    assert koppen_peel(tas, pr, lat=40.0) == expected


def test_tropical_rainforest():
    assert koppen_peel([26.0] * 12, [200.0] * 12, lat=0.0) == "Af"


def test_tropical_savanna():
    pr = [5, 5, 10, 50, 150, 200, 250, 250, 200, 100, 20, 5]
    assert koppen_peel([25.0] * 12, pr, lat=10.0) == "Aw"


def test_hot_desert():
    assert koppen_peel([20, 22, 25, 28, 32, 35, 36, 36, 33, 28, 24, 20], [5] * 12, lat=25.0) == "BWh"


def test_tundra_and_ice():
    assert koppen_peel([-20, -18, -15, -10, -2, 3, 6, 5, 1, -5, -12, -18], [20] * 12, lat=70.0) == "ET"
    assert koppen_peel([-30] * 11 + [-1], [10] * 12, lat=80.0) == "EF"


def test_continental_dfb():
    tas = [-8, -6, -1, 6, 13, 17, 19, 18, 13, 7, 0, -5]
    assert koppen_peel(tas, [50, 45, 45, 50, 60, 70, 80, 75, 60, 55, 55, 50], lat=55.0) == "Dfb"


def test_southern_hemisphere_summer():
    # Dry Dec-Feb is SUMMER in the south → Csb, not Cwb.
    tas = [19, 19, 17, 15, 12, 10, 9, 10, 11, 13, 15, 17]
    pr = [10, 12, 20, 50, 90, 110, 120, 100, 70, 45, 25, 12]
    assert koppen_peel(tas, pr, lat=-34.0) == "Csb"


@pytest.mark.parametrize("tas,pr", [([None] * 12, [1] * 12), ([1] * 11, [1] * 12), ([1] * 12, [None] + [1] * 11)])
def test_invalid_input_is_none(tas, pr):
    assert koppen_peel(tas, pr, lat=40.0) is None
