# Ingestión de datos — BioOrchestrator

Este directorio contiene los ingesters que transforman archivos JSON-LD
producidos por los scrapers (`nkz-*-scraper`) en nodos del grafo de
conocimiento (Neo4j).

## Regla de oro

**Los scrapers NO traducen, NO normalizan, NO unifican.**
Los ingesters (vía `BaseIngester.normalize_nodes()`) lo hacen automáticamente.

## Estructura

```
ingestion/
├── base_ingester.py          ← Clase base. NO MODIFICAR sin revisión.
├── normalization_registry.py ← Mapeos de traits, ubicaciones, escalas.
├── *_ingester.py             ← Un fichero por fuente (heredan de BaseIngester).
├── semantic_mappings.py      ← Mapeos ICASA, QUDT, AGROVOC.
├── validate_source.py        ← Script de validación para nuevas fuentes.
├── sync.py                   ← Sincronización Orion-LD → Neo4j.
├── builders.py               ← Constructores de entidades NGSI-LD.
├── uri.py                    ← Generación de URIs canónicas.
└── variety_ingester.py       ← Ingesta de variedades CPVO.
```

## Cómo añadir una nueva fuente

1. Crea el scraper en `nkz-nueva-fuente-scraper` → produce `data/trials.jsonld`
2. Crea `nueva_fuente_ingester.py` heredando de `BaseIngester`
3. Añade mapeos en `normalization_registry.py`
4. Valida: `python -m app.ingestion.validate_source NUEVA_FUENTE data/trials.jsonld`
5. Ejecuta: `python -m app.ingestion.nueva_fuente_ingester --jsonld data/trials.jsonld`

**Especificación completa:** `internal-docs-local/ESPECIFICACION_SCRAPER_INGESTA.md`
