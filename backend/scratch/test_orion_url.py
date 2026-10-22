import os
import sys

# Change to the correct directory so imports work
sys.path.insert(0, "/home/g/Documents/nekazari/nkz-module-bioorchestrator/backend")

from nkz_platform_sdk.orion import OrionClient
from app.core.config import settings

print("OS ENVS:", os.environ.get("ORION_LD_URL"))
print("Settings Orion URL:", settings.orion_ld_url)

client_no_base = OrionClient(tenant_id="test")
print("Client NO BASE URL:", client_no_base.base_url)

client_with_base = OrionClient(tenant_id="test", base_url=settings.orion_ld_url)
print("Client WITH BASE URL:", client_with_base.base_url)
