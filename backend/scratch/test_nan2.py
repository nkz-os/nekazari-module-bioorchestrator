import math
from fastapi import FastAPI
import uvicorn
import threading
import requests
import time

app = FastAPI()

@app.get("/test")
def test_endpoint():
    return {"ph": math.nan}

def run_server():
    uvicorn.run(app, host="127.0.0.1", port=8421, log_level="error")

t = threading.Thread(target=run_server, daemon=True)
t.start()

time.sleep(1)

try:
    resp = requests.get("http://127.0.0.1:8421/test")
    print("Real HTTP status:", resp.status_code)
    print("Real HTTP body:", resp.text)
except Exception as e:
    print("Real HTTP Exception:", str(e))
