---
title: BioOrchestrator
description: Multi-domain biodiversity ETL pipeline and knowledge graph for the Nekazari platform.
sidebar:
  order: 1
---

# BioOrchestrator

Multi-domain biodiversity ETL pipeline for regenerative agriculture intelligence. Integrates 19 data sources across agriculture, livestock, forestry, and agroforestry into a Neo4j knowledge graph with JSON-LD/RDF semantics.

## Architecture

- **IkerKeta** — ETL pipeline engine (fetch, deduplicate, cross-reference, export JSON-LD)
- **Neo4j + n10s** — Knowledge graph with RDF/JSON-LD semantics
- **FastAPI** — REST API with JWT auth, pipeline trigger, DAD-IS proxy, phenology params
- **React IIFE** — Frontend (Sources Dashboard, Pipeline Runner, DAD-IS Breed Explorer)

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness probe |
| GET | `/readyz` | Readiness probe (checks IkerKeta) |
| GET | `/api/graph/health` | Neo4j connectivity check |
| GET | `/api/graph/stats` | Node/relationship counts |
| GET | `/api/graph/phenology-params` | Phenology parameters (D1, D2, Kc, MDS_ref) for Crop Health |
| POST | `/api/pipeline/run` | Trigger IkerKeta ETL pipeline |
| POST | `/api/dadis/breeds` | DAD-IS breed search |
| GET | `/api/dadis/breeds/{id}` | Breed details |
| GET | `/api/dadis/countries` | DAD-IS countries |
| GET | `/api/dadis/species` | DAD-IS species |
| * | `/api/v1/*` | IkerKeta native API (mounted) |

## Deployment

See [deployment.md](./deployment.md) for the full production runbook.
