from app.planning import irrigation as irr


def _wheat_rows():
    kc = {"initial": 0.3, "development": 0.7, "mid-season": 1.15, "late-season": 0.4}
    ln = {"initial": (15, 45), "development": (45, 105), "mid-season": (105, 180), "late-season": (180, 230)}
    return [{"species": "wheat", "stage": s, "kc": kc[s], "d1": ln[s][0], "d2": ln[s][1], "source": "FAO-56"}
            for s in irr.STAGES]


async def test_stage_rows_query_filters_generic_and_caches(fake_driver_cls):
    from app.core.knowledge import KnowledgeSource
    irr.clear_stage_cache()
    drv = fake_driver_cls([("PhenologyParams", _wheat_rows())])
    src = KnowledgeSource("global", drv)
    got = await irr.load_stage_rows(src, now=0.0)
    assert set(got) == {"wheat"} and len(got["wheat"]) == 4
    assert await irr.load_stage_rows(src, now=10.0) is got and drv.sessions == 1
    q = irr._STAGES_QUERY
    assert "__generic__" in q and "__standard__" in q and "pending_review" in q
