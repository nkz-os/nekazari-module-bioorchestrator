import inspect
from app.graph.dao import GraphDAO


def test_get_variety_trials_returns_perennial_fields():
    src = inspect.getsource(GraphDAO.get_variety_trials)
    assert "rootstock" in src
    assert "trainingSystem" in src
    assert "plantingYear" in src
    assert "orchard_age_years" in src
