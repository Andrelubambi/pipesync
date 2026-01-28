# webhook_server.py
import os, json
from pathlib import Path
from datetime import datetime, timezone
from fastapi import FastAPI, Request, Header, HTTPException
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
EVENT_SECRET = os.getenv("EVENT_SECRET_TOKEN", "")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./data"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI()

def append_event(row: dict):
    out = OUTPUT_DIR / "events_webhook.csv"
    df = pd.DataFrame([row])
    header = not out.exists()
    df.to_csv(out, mode="a", header=header, index=False)

@app.get("/")
def home():
    return {"message": "PipeSync Omatapalo - Server is Running!"}


@app.post("/pipefy/webhook")
async def pipefy_webhook(request: Request, x_secret_token: str = Header(None)):
    # Validação simples
    if EVENT_SECRET and x_secret_token != EVENT_SECRET:
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = await request.json()
    now_utc = datetime.now(timezone.utc).isoformat()

    event = {
        "received_at_utc": now_utc,
        "action": payload.get("action") or (payload.get("event") or {}).get("action"),
        "pipe_id": (payload.get("data") or {}).get("pipe_id"),
        "card_id": (payload.get("data") or {}).get("card", {}).get("id"),
        "from_phase_id": (payload.get("data") or {}).get("from", {}).get("id"),
        "to_phase_id": (payload.get("data") or {}).get("to", {}).get("id"),
        "field_id": (payload.get("data") or {}).get("field", {}).get("id"),
        "raw": json.dumps(payload)
    }
    append_event(event)
    return {"ok": True}


# Rub server python -m uvicorn webhook_server:app --host 0.0.0.0 --port 8080
# python manage_webhook.py