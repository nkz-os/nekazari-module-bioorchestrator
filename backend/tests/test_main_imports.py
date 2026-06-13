"""Guard against the latent NameError: main.py uses HTTPException at the
404/500 raise sites but must actually import it."""
from fastapi import HTTPException


def test_main_imports_httpexception():
    import app.main as m
    assert m.HTTPException is HTTPException
