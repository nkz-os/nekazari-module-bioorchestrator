from app.ingestion.base_ingester import BaseIngester


def test_perennial_fields_change_content_hash():
    """Two trials identical except rootstock get distinct unique keys."""
    base = {"mergeKey": "ifapa_almond|prndu|guara|las torres|2019",
            "cropEppo": "PRNDU", "variety": "Guara", "year": 2019,
            "yieldKgHa": 2100.0}
    a = {**base, "rootstock": "Garnem"}
    b = {**base, "rootstock": "GF-677"}
    ka = BaseIngester._variety_unique_key(a)
    kb = BaseIngester._variety_unique_key(b)
    assert ka is not None and kb is not None
    assert ka != kb  # different rootstock -> different node identity


def test_merge_variety_trials_sets_perennial_fields():
    """The MERGE Cypher must SET the new optional fields."""
    import inspect
    from app.ingestion.base_ingester import BaseIngester
    src = inspect.getsource(BaseIngester._merge_variety_trials)
    for field in ("rootstock", "scion", "trainingSystem",
                  "plantingYear", "plantingDensityTreesHa"):
        assert field in src, f"{field} not persisted in _merge_variety_trials"
