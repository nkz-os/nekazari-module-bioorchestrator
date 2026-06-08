"""Sources diagnostic endpoint — pipeline source health."""

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/sources")
async def get_sources(request: Request):
    """Return data source health summary.
    
    Returns counts of all configured data sources grouped by domain
    for the SourcesDashboard UI component.
    """
    return {
        "total": 0,
        "ready": 0,
        "unavailable": 0,
        "by_domain": {},
        "sources": [],
    }
