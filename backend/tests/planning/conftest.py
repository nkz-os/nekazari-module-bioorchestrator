import pytest


class _Result:
    def __init__(self, rows):
        self._rows = rows

    async def data(self):
        return list(self._rows)


class _Session:
    def __init__(self, answers):
        self._answers = answers

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def run(self, query, **params):
        for marker, rows in self._answers:
            if marker in query:
                return _Result(rows)
        raise AssertionError(f"unexpected query: {query[:80]}")


class FakeDriver:
    """Answers each Cypher query with the rows of the first marker it contains."""

    def __init__(self, answers):
        self.answers = answers
        self.sessions = 0

    def session(self, **kwargs):
        self.sessions += 1
        return _Session(self.answers)


@pytest.fixture
def fake_driver_cls():
    return FakeDriver
