import asyncio
import math
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

app = FastAPI()

@app.get("/test")
def test_endpoint():
    return {"ph": math.nan}

client = TestClient(app)

try:
    response = client.get("/test")
    print("Status:", response.status_code)
    print("Body:", response.text)
except Exception as e:
    print("Exception raised:", str(e))
