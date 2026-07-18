"""get_yield_projection debe declarar que su estrés hídrico es un supuesto fijo.

No valida el valor del rendimiento: valida que la respuesta no se presente como
si el cociente ETa/ETc procediera de dato observado.
"""
import inspect

from app.graph import dao


def test_response_declares_the_fixed_eta_etc_assumption():
    src = inspect.getsource(dao.GraphDAO.get_yield_projection)
    assert '"eta_etc_source": "fixed_assumption_0.85"' in src, (
        "data_quality debe declarar explícitamente que ETa/ETc es un supuesto fijo"
    )


def test_response_carries_a_top_level_warning():
    src = inspect.getsource(dao.GraphDAO.get_yield_projection)
    assert '"warning"' in src, (
        "la respuesta debe llevar un aviso al mismo nivel que methodology"
    )
