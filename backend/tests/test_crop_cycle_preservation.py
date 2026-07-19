"""El ciclo del cultivo (invierno / primavera) no debe perderse ni colapsarse.

Distinguir variedades de invierno de las de primavera es un requisito de producto:
sin él no se pueden planificar rotaciones ajustadas a la época del año, y las medias
por cultivo mezclan poblaciones agronómicamente incomparables.

La fuente BSL etiqueta el ciclo en alemán dentro de `crop_scientific`
(Winterweichweizen, Sommergerste, Winterraps...): 9.980 de 15.884 ensayos.
"""
from app.ingestion.base_ingester import BaseIngester
from app.ingestion.bsl_ingester import BslIngester


class TestEppoUnification:
    """La unificación de códigos EPPO no puede fusionar ciclos ni especies distintas."""

    def test_maize_split_code_is_unified(self):
        # ZEAMA y ZEAMX son el mismo maíz: unificar es correcto.
        assert BaseIngester._normalize_eppo("ZEAMA") == "ZEAMX"
        assert BaseIngester._normalize_eppo("eppo:ZEAMA") == "ZEAMX"

    def test_winter_wheat_code_is_NOT_merged_into_spring_wheat(self):
        # TRZAW es espelta/trigo de INVIERNO en la fuente; TRZAX en BSL es
        # Sommerweichweizen, trigo de PRIMAVERA. Fusionarlos destruye el ciclo
        # y además mezcla dos especies.
        assert BaseIngester._normalize_eppo("TRZAW") == "TRZAW"

    def test_winter_rapeseed_code_is_NOT_merged(self):
        # BRSNW es Winterraps (colza de invierno) en el 100 % de los casos.
        assert BaseIngester._normalize_eppo("BRSNW") == "BRSNW"

    def test_unknown_codes_pass_through_unchanged(self):
        assert BaseIngester._normalize_eppo("HORVX") == "HORVX"
        assert BaseIngester._normalize_eppo("bad") is None
        assert BaseIngester._normalize_eppo(None) is None


class TestCropCycleExtraction:
    """El ciclo se captura como campo propio, sin tocar `cropScientific`."""

    def test_winter_labels_yield_winter_cycle(self):
        for label in ("Winterweichweizen", "Wintergerste", "Winterraps",
                      "Winterroggen", "Winterspelz"):
            assert BslIngester._crop_cycle(label) == "winter", label

    def test_spring_labels_yield_spring_cycle(self):
        for label in ("Sommergerste", "Sommerweichweizen", "Sommerhafer",
                      "Sommerroggen"):
            assert BslIngester._crop_cycle(label) == "spring", label

    def test_unlabelled_crops_yield_none(self):
        # Maíz, soja o triticale no llevan prefijo estacional: no inventamos uno.
        for label in ("Körnermais", "Silomais", "Sojabohne", "Triticale",
                      "Hartweizen", None, ""):
            assert BslIngester._crop_cycle(label) is None, label

    def test_conversion_keeps_latin_name_and_adds_cycle(self):
        """`cropScientific` sigue siendo el nombre latino: el DAO empareja por él."""
        out = BslIngester()._convert_trial({
            "crop_eppo": "eppo:HORVX",
            "crop_scientific": "Wintergerste",
            "variety": "Adriana",
            "year": 2024,
            "trial_location": "BSL Deutschland Dfb",
        })
        assert out["cropScientific"] == "Hordeum vulgare"
        assert out["cropCycle"] == "winter"

    def test_conversion_without_seasonal_label_has_no_cycle(self):
        out = BslIngester()._convert_trial({
            "crop_eppo": "eppo:ZEAMX",
            "crop_scientific": "Körnermais",
            "variety": "Test",
            "year": 2024,
            "trial_location": "BSL Deutschland Dfb",
        })
        assert out["cropScientific"] == "Zea mays"
        assert out.get("cropCycle") is None

    def test_winter_and_spring_barley_get_different_merge_identity(self):
        """Dos ensayos idénticos salvo el ciclo no pueden colapsar en un nodo."""
        base = {
            "crop_eppo": "eppo:HORVX", "variety": "Adriana", "year": 2024,
            "trial_location": "BSL Deutschland Dfb",
        }
        ing = BslIngester()
        winter = ing._convert_trial({**base, "crop_scientific": "Wintergerste"})
        spring = ing._convert_trial({**base, "crop_scientific": "Sommergerste"})
        assert winter["cropCycle"] != spring["cropCycle"]
        assert winter["mergeKey"] != spring["mergeKey"]
