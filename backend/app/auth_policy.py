"""Which requests under the public (auth-exempt) prefixes still need an identity.

The graph is global reference knowledge and stays public. Anything that reads or
writes a tenant's own entities does not: those requests carry a parcel id, or
write under the agriculture prefix. For them the tenant must come from a verified
identity, never from the request itself.
"""

from __future__ import annotations

from collections.abc import Mapping

_AGRICULTURE_PREFIX = "/api/graph/agriculture/"
_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_URN_MARKER = "urn:ngsi-ld:"


def requires_identity(path: str, method: str, query: Mapping[str, str]) -> bool:
    """Classify requests that need a tenant identity under public prefixes.

    Returns True if the request touches tenant data and thus must authenticate.
    The classification heuristic: (1) parcel_id is present in query params,
    (2) path contains a URN segment, or (3) method is a write under /agriculture/.
    A future tenant-scoped GET without explicit parcel_id under /agriculture/
    would not be classified by this rule — new routes must be checked manually.
    """
    if "parcel_id" in query:
        return True
    if _URN_MARKER in path:
        return True
    return path.startswith(_AGRICULTURE_PREFIX) and method.upper() in _WRITE_METHODS
