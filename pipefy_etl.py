import os, json, math
from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("PIPEFY_TOKEN")
PIPE_ID = os.getenv("PIPE_ID")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./data"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_URL = "https://api.pipefy.com/graphql"  # Pipefy GraphQL endpoint (docs) [9](https://developers.pipefy.com/)
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# 1) Query GraphQL com paginação
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
"""  # Estrutura conforme a referência de Queries/Objects (Cards/PhaseDetail). [6](https://api-docs.pipefy.com/reference/queries/)[2](https://developers.pipefy.com/reference/cards)[3](https://api-docs.pipefy.com/reference/objects/PhaseDetail/)

def fetch_cards(pipe_id: str, page_size: int = 200):
    after = None
    cards = []
    while True:
        payload = {"query": QUERY, "variables": {"pipeId": pipe_id, "first": page_size, "after": after}}
        r = requests.post(API_URL, headers=HEADERS, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()["data"]["cards"]
        for edge in data["edges"]:
            cards.append(edge["node"])
        if data["pageInfo"]["hasNextPage"]:
            after = data["pageInfo"]["endCursor"]
        else:
            break
    return cards

# 2) Normalização de datas/horas
def to_ts_utc(x):
    if not x:
        return None
    # Pandas parse ISO-8601; Pipefy usa ISO offsets (ex.: 2024-11-14T21:50:55+00:00) 
    # que devem ser tratados como timezone-aware.  (Pipefy datas em ISO-8601) [2](https://developers.pipefy.com/reference/cards)
    ts = pd.to_datetime(x, utc=True, errors="coerce")
    return ts

def normalize_card(card: dict):
    # Campos principais
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

    # Campos custom: achata lista de dicts -> linhas
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

    # Histórico de fases: uma linha por fase
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

    # Também gera colunas em GMT+1 (Luanda) para conveniência de relatório
    def to_gmt_plus_1(s):
        if s is None or s.isna().all():
            return s
        return s.dt.tz_convert("Etc/GMT-1")  # GMT-1 em Etc equivale a +01:00 (convenção do tzdb)
    for col in [c for c in df_cards.columns if c.endswith("_utc")]:
        local_col = col.replace("_utc", "_ao")
        df_cards[local_col] = to_gmt_plus_1(df_cards[col])
    for df in (df_fields, df_phases):
        for col in [c for c in df.columns if c.endswith("_utc")]:
            local_col = col.replace("_utc", "_ao")
            df[local_col] = to_gmt_plus_1(df[col])

    # Guardar CSV/Parquet
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    df_cards.to_parquet(OUTPUT_DIR / f"cards_{ts}.parquet", index=False)
    df_fields.to_parquet(OUTPUT_DIR / f"fields_{ts}.parquet", index=False)
    df_phases.to_parquet(OUTPUT_DIR / f"phases_{ts}.parquet", index=False)
    print("Export concluído:", OUTPUT_DIR)

if __name__ == "__main__":
    run()