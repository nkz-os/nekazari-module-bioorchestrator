---
title: BioOrchestrator Deployment Runbook
description: Step-by-step deployment procedure for the BioOrchestrator module on Nekazari production cluster.
---

# BioOrchestrator — Deployment Runbook

## Prerequisites

- [ ] GitHub repo `nkz-os/nekazari-module-bioorchestrator` exists and is **public**
- [ ] Local git repo has `origin` pointing to the GitHub remote
- [ ] Docker is installed and authenticated to `ghcr.io` (`docker login ghcr.io`)
- [ ] `kubectl` access to the production K3s cluster (namespace `nekazari`)
- [ ] Neo4j custom image built (n10s plugin)

## 1. Push Code to GitHub

```bash
cd /home/g/Documents/nekazari/nkz-module-bioorchestrator
git remote add origin git@github.com:nkz-os/nekazari-module-bioorchestrator.git
git add .
git commit -m "v0.1.0 — Initial bioorchestrator module with Neo4j, IkerKeta, DAD-IS"
git push -u origin main
```

## 2. Build & Push Docker Images

### 2a. Neo4j Image (with n10s plugin)

```bash
cd /home/g/Documents/nekazari/nkz-module-bioorchestrator
docker build --network=host --no-cache \
  -t ghcr.io/nkz-os/bioorchestrator-neo4j:5.26 \
  -f backend/neo4j/Dockerfile backend/neo4j/
docker push ghcr.io/nkz-os/bioorchestrator-neo4j:5.26
```

**Verify:** Package is **public** on GHCR (Settings → Change visibility → Public).

### 2b. Backend Image

```bash
cd /home/g/Documents/nekazari/nkz-module-bioorchestrator
docker build --network=host --no-cache \
  -t ghcr.io/nkz-os/bioorchestrator-backend:0.1.0 \
  -f backend/Dockerfile backend/
docker push ghcr.io/nkz-os/bioorchestrator-backend:0.1.0
```

**Verify:** Package is **public** on GHCR.

## 3. Apply Secrets (MANUAL — before ArgoCD)

```bash
# Edit k8s/secret.yaml with real values first, then:
kubectl apply -f k8s/secret.yaml -n nekazari
```

Required secret values:
| Key | Status | Notes |
|-----|--------|-------|
| `neo4j-auth` | Set | `neo4j/<password>` |
| `EPPO_API_TOKEN` | Pending | EPPO API registration |
| `IUCN_API_TOKEN` | Pending | IUCN Red List API |
| `bioorchestrator-dadis-token` | Pending | FAO DAD-IS approval |
| `KEYCLOAK_JWKS_URL` | Set | `https://auth.robotika.cloud/auth/realms/nekazari/protocol/openid-connect/certs` |
| `KEYCLOAK_CLIENT_ID` | Set | `nekazari-frontend` |

## 4. Apply K8s Manifests

```bash
cd /home/g/Documents/nekazari/nkz-module-bioorchestrator

# Neo4j first (backend depends on it)
kubectl apply -f k8s/neo4j-statefulset.yaml -n nekazari

# Wait for Neo4j to be ready
kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=neo4j -n nekazari --timeout=120s

# Backend
kubectl apply -f k8s/deployment.yaml -n nekazari
kubectl apply -f k8s/service.yaml -n nekazari

# Wait for backend to be ready
kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=backend -n nekazari --timeout=60s
```

**Do NOT apply `k8s/ingress.yaml`** — routing goes through api-gateway. The ingress file is for local development only.

## 5. Verify Deployment

```bash
# Check pods
kubectl get pods -n nekazari | grep bioorchestrator

# Check Neo4j connectivity from backend
kubectl exec -n nekazari deploy/bioorchestrator-backend -- \
  curl -s http://localhost:8420/api/graph/health

# Check health endpoints
kubectl exec -n nekazari deploy/bioorchestrator-backend -- \
  curl -s http://localhost:8420/healthz

# Check phenology endpoint (requires graph data loaded)
kubectl exec -n nekazari deploy/bioorchestrator-backend -- \
  curl -s "http://localhost:8420/api/graph/phenology-params?species=olive&stage=vegetative"
```

## 6. Initialize Knowledge Graph

```bash
# Copy IkerKeta JSON-LD data to the Neo4j pod (if data exists)
# Then run the init script:
kubectl exec -n nekazari deploy/bioorchestrator-backend -- \
  python scripts/init_graph.py --neo4j-uri bolt://bioorchestrator-neo4j:7687
```

## 7. Register in Marketplace

```bash
# Connect to admin_platform PostgreSQL and run:
psql -h <db-host> -U <user> -d admin_platform -f k8s/registration.sql
```

## 8. Enable ArgoCD Sync

Once everything is stable, the ArgoCD Application at `nkz/gitops/modules/bioorchestrator.yaml` will auto-sync the deployment. Ensure:

- [ ] Secret is applied (ArgoCD excludes `secret.yaml`)
- [ ] GHCR images are public
- [ ] No ingress conflict with api-gateway

```bash
# Force ArgoCD to pick up the new app
kubectl apply -f nkz/gitops/modules/bioorchestrator.yaml
# Or push to nkz main — the root app auto-detects new modules
```

## 9. Verify Crop Health Integration

```bash
# From crop-health pod, test bioorchestrator connectivity
kubectl exec -n nekazari deploy/crop-health-backend -- \
  curl -s "http://bioorchestrator-api-service:8420/api/graph/phenology-params?species=olive&stage=vegetative"

# Check crop-health logs for phenology source
kubectl logs -n nekazari deploy/crop-health-backend --tail=50 | grep -i phenology
```

## Rollback

```bash
kubectl delete -f k8s/deployment.yaml -n nekazari
kubectl delete -f k8s/service.yaml -n nekazari
kubectl delete -f k8s/neo4j-statefulset.yaml -n nekazari
# PVC is retained — delete manually if needed:
# kubectl delete pvc -l app.kubernetes.io/name=bioorchestrator -n nekazari
```

## Notes

- **Neo4j Community Edition** — single node, no clustering. Backups via `neo4j-admin dump`.
- **IkerKeta** is installed from `git+https://github.com/nekazari/IkerKeta.git@main` during Docker build. If the repo is private, the build will fail.
- **DAD-IS token** is pending FAO approval — the DAD-IS tab will show errors until the token is set.
- **API Gateway routing**: The recommended production setup routes `/api/bioorchestrator` through the api-gateway, not via a separate Traefik Ingress. See `nkz/services/api-gateway/` for configuration.
