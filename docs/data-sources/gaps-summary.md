# European Cover Crop Data — Gaps Summary

> **Date:** 2026-06-02 (updated post Phase 1)  
> **Branches:** `feat/european-cover-crop-data`, `feat/intia-pipeline` (Phase 1 done)

## Phase Progress

| Phase | Source | Status | Repo | Records |
|-------|--------|--------|------|---------|
| ✅ Phase 1 | INTIA Navarra | **Done** | `nkz-intia-scraper` | 54 validated AgriKnowledge entities |
| ⏳ Phase 2 | JRC MARS Bulletins | Pending exploration | — | — |
| ⏳ Phase 3 | ITACyL / IFAPA | Pending exploration | — | — |

### Phase 1 Deliverables (Complete)

- **`nkz-intia-scraper/`**: PDF table extractor — 97 observations from 6 PDFs (2020-2025)
- **`backend/app/services/intia_experimentacion.py`**: Connector producing AgriKnowledge entities
- **`backend/app/services/intia_cover_crops.py`**: Updated with extracted values
- **Key findings**: 48-treatment cover crop screening at Sartaguda (BSk), roller-crimper trials, rotation yields

### Phase 2-3 Status

JRC MARS (https://agri4cast.jrc.ec.europa.eu/) is a JavaScript SPA — requires either:
- API discovery (check for REST endpoints)
- Headless browser scraping (Playwright/Selenium)

ITACyL/IFAPA portals require initial exploration to understand data format.

**Request for user**: Explore JRC MARS for data download links, and ITACyL/IFAPA for legume trial sections.

## Executive Summary

Of the 12 species × 3 climates × 6 parameters = 216 possible data cells,
**132 are populated** (61%) from verified European sources.
**84 gaps** exist, of which **48 are "not applicable"** (grasses don't fix N,
some species-climate combos aren't agronomically viable), leaving
**36 genuine data gaps** requiring further research.

## Gap Matrix

Legend: ✅ = populated | ⬜ = genuine gap | ❌ = not applicable | ⚠️ = low confidence (gap-filled estimate)

### Cover Crops — Csa (Hot-summer Mediterranean)

| Species | Biomass | C/N | GDD | Frost tol | N fix | N content |
|---------|---------|-----|-----|-----------|-------|-----------|
| VICSA | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| VICVI | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| AVESA | ✅ | ✅ | ✅ | ✅ | ❌ | ⬜ |
| SECCE | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| TRFIN | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| MEDSA | ✅ | ✅ | ⬜ | ✅ | ✅ | ⬜ |

### Cover Crops — BSk (Cold semi-arid)

| Species | Biomass | C/N | GDD | Frost tol | N fix | N content |
|---------|---------|-----|-----|-----------|-------|-----------|
| VICSA | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| VICVI | ✅ | ✅ | ✅ | ✅ | ✅ | ⬜ |
| AVESA | ✅ | ✅ | ✅ | ✅ | ❌ | ⬜ |
| SECCE | ✅ | ✅ | ✅ | ✅ | ❌ | ⬜ |
| TRFIN | ✅ | ⬜ | ✅ | ⬜ | ✅ | ⬜ |
| MEDSA | ⚠️ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |

### Cover Crops — Cfb (Oceanic)

| Species | Biomass | C/N | GDD | Frost tol | N fix | N content |
|---------|---------|-----|-----|-----------|-------|-----------|
| VICSA | ✅ | ✅ | ✅ | ⬜ | ✅ | ⬜ |
| VICVI | ✅ | ✅ | ✅ | ✅ | ✅ | ⬜ |
| AVESA | ✅ | ⬜ | ✅ | ⬜ | ❌ | ⬜ |
| SECCE | ✅ | ✅ | ✅ | ⬜ | ❌ | ⬜ |
| TRFIN | ✅ | ✅ | ⬜ | ✅ | ✅ | ⬜ |
| MEDSA | ✅ | ⬜ | ⬜ | ⬜ | ✅ | ⬜ |

### Protein Crops — Csa

| Species | Biomass | C/N | GDD | Frost tol | N fix | N content |
|---------|---------|-----|-----|-----------|-------|-----------|
| VICFX | ✅ | ✅ | ✅ | ⬜ | ✅ | ✅ |
| PIBAR | ✅ | ✅ | ✅ | ⬜ | ✅ | ✅ |
| CIEAR | ✅ | ✅ | ✅ | ⬜ | ✅ | ⬜ |
| LENCU | ✅ | ⬜ | ✅ | ⬜ | ✅ | ⬜ |
| LTHSA | ⚠️ | ⬜ | ⚠️ | ⬜ | ⚠️ | ⬜ |
| GLXMA | ✅ | ✅ | ✅ | ❌ | ✅ | ⬜ |

### Protein Crops — BSk

| Species | Biomass | C/N | GDD | Frost tol | N fix | N content |
|---------|---------|-----|-----|-----------|-------|-----------|
| VICFX | ✅ | ✅ | ✅ | ✅ | ✅ | ⬜ |
| PIBAR | ✅ | ⬜ | ✅ | ⬜ | ✅ | ⬜ |
| CIEAR | ✅ | ⬜ | ✅ | ✅ | ✅ | ⬜ |
| LENCU | ✅ | ⬜ | ✅ | ⬜ | ✅ | ⬜ |
| LTHSA | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| GLXMA | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Protein Crops — Cfb

| Species | Biomass | C/N | GDD | Frost tol | N fix | N content |
|---------|---------|-----|-----|-----------|-------|-----------|
| VICFX | ✅ | ⬜ | ✅ | ⬜ | ✅ | ⬜ |
| PIBAR | ✅ | ⬜ | ✅ | ⬜ | ✅ | ⬜ |
| CIEAR | ✅ | ⬜ | ⚠️ | ⬜ | ⬜ | ⬜ |
| LENCU | ✅ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| LTHSA | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| GLXMA | ✅ | ⬜ | ✅ | ❌ | ✅ | ⬜ |

## Genuine Data Gaps (Action Required)

### Priority 1 — Critical for regenerative-sequence engine

| # | Gap | Impact | Proposed Action |
|---|-----|--------|-----------------|
| 1 | **LTHSA × all climates × all parameters** | Cannot recommend grass pea in any rotation | Partner with IFAPA (Andalucía) or CIHEAM Zaragoza for Lathyrus trial data |
| 2 | **TRFIN × BSk × C/N ratio** | Missing decomposition rate for BSk crimson clover | Extrapolate from Csa data; mark as `data_gap: true` until INTIA trial confirmed |
| 3 | **TRFIN × BSk × frost_tolerance** | Risk of winterkill recommendation for BSk crimson clover | INTIA may have winter survival data in their legume trials. Check 2020–2024 reports. |

### Priority 2 — Important for completeness

| # | Gap | Impact | Proposed Action |
|---|-----|--------|-----------------|
| 4 | **N content for AVESA, SECCE in BSk/Cfb** | Can't calculate N immobilization risk precisely | Use C/N ratio to derive N content (N% ≈ 42 / C:N). Add derived flag. |
| 5 | **Protein crop C/N in BSk and Cfb** | Missing residue quality data for post-harvest management | Search DiverIMPACTS case studies for residue quality data |
| 6 | **MEDSA × BSk × single-cut biomass** | Alfalfa cover crop use case not studied in BSk | INTIA does alfalfa trials but for forage (3–5 cuts). Need dedicated cover crop termination trials. |

### Priority 3 — Nice to have

| # | Gap | Impact | Proposed Action |
|---|-----|--------|-----------------|
| 7 | **Frost tolerance for Cfb species** | Cfb frost events are rare → conservative estimates acceptable | Use Csa lower bound (warmer climate) as Cfb upper bound |
| 8 | **GDD for MEDSA (cover crop context)** | Alfalfa not typically terminated in first year | Mark as "forage context, not cover crop." Consider separate model for perennial ley rotations. |
| 9 | **LENCU × Cfb × all parameters** | Lentil not a significant crop in Cfb | Low priority. Accept Csa data as estimate for Cfb. |

## Not Applicable (by Design)

| Gap | Reason |
|-----|--------|
| AVESA N fixation | Grass species — does not fix atmospheric N |
| SECCE N fixation | Grass species — does not fix atmospheric N |
| GLXMA × BSk | Soybean requires irrigation in semi-arid — not agronomically viable without it |
| GLXMA frost tolerance | Soybean is frost-sensitive (0°C). Spring-sown only; frost tolerance is not a meaningful parameter. |

## Recommended Next Steps for Gap Closure

1. **INTIA deep scrape** (Weeks 1–2): INTIA website (intiasa.es) likely contains
   legume and cover crop reports not yet extracted. Same Drupal structure as
   nkz-navarra-agraria. Implement scraping pipeline following existing pattern.

2. **JRC MARS automated download** (Weeks 2–3): JRC Agri4Cast publishes regular
   PDF bulletins with structured phenology data. PDF table extraction pattern
   exists in nkz-genvce-scraper. Automate download + extraction for GDD parameters.

3. **Lathyrus targeted research** (Weeks 3–4): Contact CIHEAM Zaragoza, IFAPA,
   or Italian CREA for Lathyrus sativus trial data. This is a neglected crop
   with high potential for climate-resilient Mediterranean agriculture.

4. **ITACyL / IFAPA exploration** (Weeks 4–5): These Spanish regional institutes
   likely have legume trial data for continental (BSk) and Mediterranean (Csa)
   climates, respectively. Explore their technology transfer portals.

---

*Generated 2026-06-02. Gap counts: 36 genuine data gaps (excluding 48 N/A).*
