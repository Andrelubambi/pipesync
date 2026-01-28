# pipefy_etl.py  — Apenas Title, Current phase, Creator, Created at
import os
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv

# Carregar .env mesmo se executares a partir de outra pasta
env_path = find_dotenv(usecwd=True)
load_dotenv(env_path)

# Usa o mesmo nome de variável do teste que funcionou (PIPEFY_TOKEN).
TOKEN = os.getenv("PIPEFY_TOKEN") or os.getenv("TOKEN")
PIPE_ID = os.getenv("PIPE_ID")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./data"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

if not TOKEN or not TOKEN.strip():
    raise RuntimeError("PIPEFY_TOKEN/TOKEN não encontrado no .env")
if not PIPE_ID:
    raise RuntimeError("PIPE_ID não encontrado no .env")

API_URL = "https://api.pipefy.com/graphql"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Query só com os campos necessários
QUERY = """
query GetCards($pipeId: ID!, $first: Int, $after: String) {
  cards(pipe_id: $pipeId, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        title
        createdAt
        createdBy { name }
        current_phase { name }
      }
    }
  }
}
"""

def fetch_cards(pipe_id: str, page_size: int = 200):
    after = None
    nodes = []
    while True:
        payload = {"query": QUERY, "variables": {"pipeId": pipe_id, "first": page_size, "after": after}}
        r = requests.post(API_URL, headers=HEADERS, json=payload, timeout=60)
        if r.status_code != 200:
            print("[DEBUG] Status:", r.status_code)
            print("[DEBUG] Body:", r.text)
            r.raise_for_status()
        data = r.json()["data"]["cards"]
        nodes.extend([e["node"] for e in data["edges"]])
        if data["pageInfo"]["hasNextPage"]:
            after = data["pageInfo"]["endCursor"]
        else:
            break
    return nodes

def run():
    cards = fetch_cards(PIPE_ID)
    # Monta exatamente as 4 colunas pedidas
    rows = [{
        "Title": c.get("title"),
        "Current phase": (c.get("current_phase") or {}).get("name"),
        "Creator": (c.get("createdBy") or {}).get("name"),
        "Created at": c.get("createdAt"),
    } for c in cards]

    df = pd.DataFrame(rows)

    # xlsx com ; para abrir bem no Excel em PT
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out = OUTPUT_DIR / f"cards_{ts}.xlsx"
    df.to_xlsx(out, index=False, sep=";")
    print(f"OK: gerado {out}")

if __name__ == "__main__":
    run()