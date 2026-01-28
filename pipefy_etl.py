# pipefy_etl.py
import os, json
from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("PIPEFY_TOKEN")
PIPE_ID = os.getenv("PIPE_ID")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./data2"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_URL = "https://api.pipefy.com/graphql"  # Endpoint GraphQL oficial [6](https://developers.pipefy.com/)
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

QUERY = """
query GetCardsWithDates($pipeId: ID!, $first: Int, $after: String) {
  cards(pipe_id: $pipeId, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        title
        createdAt
        updated_at
        due_date
        started_current_phase_at
        current_phase { id name }
        fields {
          name
          indexName
          date_value
          datetime_value
          filled_at
          value
        }
        phases_history {
          phase { id name }
          created_at
          firstTimeIn
          lastTimeIn
          lastTimeOut
          duration
        }
      }
    }
  }
}
"""  # Campos e objetos conforme Cards/PhaseDetail e Queries da API. [2](https://developers.pipefy.com/reference/cards)[3](https://api-docs.pipefy.com/reference/objects/PhaseDetail/)[5](https://api-docs.pipefy.com/reference/queries/)

def fetch_cards(pipe_id: str, page_size: int = 200):
    after = None
    cards = []
    while True:
        payload = {"query": QUERY, "variables": {"pipeId": pipe_id, "first": page_size, "after": after}}
        r = requests.post(API_URL, headers=HEADERS, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()["data"]["cards"]
        cards.extend([e["node"] for e in data["edges"]])
        if data["pageInfo"]["hasNextPage"]:
            after = data["pageInfo"]["endCursor"]
        else:
            break
    return cards

def to_ts_utc(x):
    if not x:
        return None
    return pd.to_datetime(x, utc=True, errors="coerce")

def normalize_card(card: dict):
    base = {
        "card_id": card["id"],
        "title": card.get("title"),
        "createdAt_utc": to_ts_utc(card.get("createdAt")),
        "updated_at_utc": to_ts_utc(card.get("updated_at")),
        "due_date_date": pd.to_datetime(card.get("due_date"), errors="coerce").date() if card.get("due_date") else None,
        "started_current_phase_at_utc": to_ts_utc(card.get("started_current_phase_at")),
        "current_phase_id": (card.get("current_phase") or {}).get("id"),
        "current_phase_name": (card.get("current_phase") or {}).get("name"),
    }

    fields_rows = []
    for f in card.get("fields", []):
        fields_rows.append({
            "card_id": card["id"],
            "field_name": f.get("name"),
            "indexName": f.get("indexName"),
            "date_value": pd.to_datetime(f.get("date_value"), errors="coerce").date() if f.get("date_value") else None,
            "datetime_value_utc": to_ts_utc(f.get("datetime_value")),
            "filled_at_utc": to_ts_utc(f.get("filled_at")),
            "raw_value": f.get("value"),
        })

    phases_rows = []
    for ph in card.get("phases_history", []):
        phases_rows.append({
            "card_id": card["id"],
            "phase_id": (ph.get("phase") or {}).get("id"),
            "phase_name": (ph.get("phase") or {}).get("name"),
            "created_at_utc": to_ts_utc(ph.get("created_at")),
            "firstTimeIn_utc": to_ts_utc(ph.get("firstTimeIn")),
            "lastTimeIn_utc": to_ts_utc(ph.get("lastTimeIn")),
            "lastTimeOut_utc": to_ts_utc(ph.get("lastTimeOut")),
            "duration_seconds": ph.get("duration"),
        })
    return base, fields_rows, phases_rows

def run():
    cards = fetch_cards(PIPE_ID)
    base_rows, field_rows, phase_rows = [], [], []
    for c in cards:
        base, fr, pr = normalize_card(c)
        base_rows.append(base)
        field_rows.extend(fr)
        phase_rows.extend(pr)

    df_cards = pd.DataFrame(base_rows)
    df_fields = pd.DataFrame(field_rows)
    df_phases = pd.DataFrame(phase_rows)

    # Guardar em CSV (UTC). O ajuste visual para GMT+1 faremos no Power BI.
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    df_cards.to_csv(OUTPUT_DIR / f"cards_{ts}.csv", index=False)
    df_fields.to_csv(OUTPUT_DIR / f"fields_{ts}.csv", index=False)
    df_phases.to_csv(OUTPUT_DIR / f"phases_{ts}.csv", index=False)
    print("Export concluído em", OUTPUT_DIR)

if __name__ == "__main__":
    run()