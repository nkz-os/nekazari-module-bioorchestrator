---
title: BioOrchestrator
description: Multi-domain biodiversity ETL pipeline and knowledge graph for the Nekazari platform.
sidebar:
  order: 1
---

# BioOrchestrator

Crop planning for each parcel, on top of a multi-domain agronomic knowledge graph. The backend integrates scientific data sources into a Neo4j graph with JSON-LD/RDF semantics and serves it, with the crop engines, to the other modules.

The user guide for the planning page is in the [README](../README.md#user-guide-crop-planning).

## Architecture

- **IkerKeta** — ETL pipeline engine (fetch, deduplicate, cross-reference, export JSON-LD)
- **Neo4j + n10s** — Knowledge graph with RDF/JSON-LD semantics
- **FastAPI** — REST API with JWT auth, pipeline trigger, DAD-IS proxy, phenology params
- **React, Module Federation 2.0** — the crop planning page (farm matrix, parcel wizard, estimates, campaign simulation) and the *Parcel plan* panel of the 3D viewer

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness probe |
| GET | `/readyz` | Readiness probe (checks IkerKeta) |
| GET | `/api/graph/health` | Neo4j connectivity check |
| GET | `/api/graph/stats` | Node/relationship counts |
| GET | `/api/graph/phenology-params` | Phenology parameters (D1, D2, Kc, MDS_ref) for Crop Health |
| GET | `/api/graph/planning/farm` | Parcels × campaigns planning matrix |
| GET | `/api/graph/planning/parcels/{id}/fit` · `/varieties` · `/calendar` · `/estimates` | Planning wizard data for a parcel |
| GET | `/api/graph/planning/parcels/{id}/estimates/in-season` | In-season yield forecast of a sown parcel (read by Crop Health) |
| PUT | `/api/graph/planning/parcels/{id}/plan` | Write the planned cycles of a campaign |
| POST | `/api/graph/agriculture/crop-simulation/jobs` | Campaign simulation (AquaCrop), asynchronous |
| POST | `/api/dadis/breeds` | DAD-IS breed search |
| GET | `/api/dadis/breeds/{id}` | Breed details |
| GET | `/api/dadis/countries` | DAD-IS countries |
| GET | `/api/dadis/species` | DAD-IS species |
| * | `/api/v1/*` | IkerKeta native API (mounted) |

## Data ingestion

Scrapers (`nkz-*-scraper`) produce JSON-LD; ingesters in `backend/app/ingestion/` merge into Neo4j.

**Canonical schema:** [INGESTION_SCHEMA.md](./INGESTION_SCHEMA.md) — node types, required fields, yield honesty rules, perennial extensions, checklist for the legacy ingesters.

**Graph build:** [KG_PIPELINE.md](./KG_PIPELINE.md) — gated, reproducible build (adapter, contract, gate, loader, verify, export), CLI safety model, onboarding a new source and evidence semantics.

## Deployment

See [deployment.md](./deployment.md) for the full production runbook.
