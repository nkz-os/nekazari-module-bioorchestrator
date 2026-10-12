"""The sowing method vocabulary and its mulch (validated values, Anexo 14)."""
import pytest

from app.services.crop_simulation import DIRECT_SOWING_MULCH_PCT, check_sowing_method, mulch_for
from app.services.sim_errors import SimulationError


def test_direct_sowing_is_the_fao_minimum_conservation_cover():
    assert DIRECT_SOWING_MULCH_PCT == 30.0
    assert mulch_for("direct") == 30.0
    assert mulch_for("conventional") is None


@pytest.mark.parametrize("v", [None, "direct", "conventional"])
def test_valid_values_pass(v):
    assert check_sowing_method(v) == v


@pytest.mark.parametrize("v", ["Direct", "no-till", ""])
def test_invalid_value_is_a_422(v):
    with pytest.raises(SimulationError) as ei:
        check_sowing_method(v)
    assert ei.value.code == "invalid_sowing_method" and ei.value.status_code == 422


def test_engine_adapter_forwards_the_mulch():
    import asyncio
    from datetime import date
    from unittest.mock import patch

    from app.services import crop_simulation as cs
    seen = {}

    async def fake(*args, **kwargs):
        seen.update(kwargs)
        return {}

    with patch("app.services.crop_simulation.aq.simulate", fake):
        asyncio.run(cs.AquaCropEngine().simulate(
            observed=[], analogs={}, soil=[], crop="WheatGDD", planting=date(2020, 10, 1),
            irrigation="rainfed", sim_start=None, mulch_pct=30.0))
    assert seen["mulch_pct"] == 30.0
