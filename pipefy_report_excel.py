# pipefy_report_excel.py
# Gera Excel com: Title | Current phase | Creator | Created at (GMT+1, yyyy-mm-dd hh:mm:ss)
# Grava metadados do Pipe (Start Form e Fases) em JSON para descoberta dinâmica de campos.

import os
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv

# ---------- Config ----------
env_path = find_dotenv(usecwd=True)
load_dotenv(env_path)

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

# ---------- Queries ----------
# Metadados do Pipe: Start Form + Fases/Fields
PIPE_SCHEMA_QUERY = """
query ($pipeId: ID!){
  pipe(id: $pipeId){
    id
    name
    start_form_fields { id label type }
    phases {
      id
      name
      fields { id label type }
    }
  }
}
"""

# Cards (apenas campos necessários para o relatório simples)
CARDS_QUERY = """
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

# ---------- Helpers ----------
def gql(query: str, variables: dict):
    r = requests.post(API_URL, headers=HEADERS, json={"query": query, "variables": variables}, timeout=60)
    if r.status_code != 200:
        print("[DEBUG] Status:", r.status_code)
        print("[DEBUG] Body:", r.text)
        r.raise_for_status()
    j = r.json()
    if "errors" in j:
        raise RuntimeError(j["errors"])
    return j["data"]

def discover_fields(pipe_id: str):
    """Descobre campos do pipe (Start Form + Fases) para mapeamento dinâmico."""
    data = gql(PIPE_SCHEMA_QUERY, {"pipeId": pipe_id})
    pipe = data["pipe"] or {}
    meta = {
        "pipe_id": pipe.get("id"),
        "pipe_name": pipe.get("name"),
        "start_form_fields": pipe.get("start_form_fields", []),
        "phases": []
    }
    for ph in pipe.get("phases", []):
        meta["phases"].append({
            "phase_id": ph.get("id"),
            "phase_name": ph.get("name"),
            "fields": ph.get("fields", [])
        })
    return meta

def fetch_all_cards(pipe_id: str, page_size: int = 200):
    after = None
    nodes = []
    while True:
        data = gql(CARDS_QUERY, {"pipeId": pipe_id, "first": page_size, "after": after})
        conn = data["cards"]
        nodes.extend([e["node"] for e in conn["edges"]])
        if conn["pageInfo"]["hasNextPage"]:
            after = conn["pageInfo"]["endCursor"]
        else:
            break
    return nodes

def to_ao_dt(iso_text: str):
    """Converte ISO UTC -> datetime 'naive' em GMT+1 (Luanda) para Excel/Power BI."""
    if not iso_text:
        return None
    ts_utc = pd.to_datetime(iso_text, utc=True, errors="coerce")
    if pd.isna(ts_utc):
        return None
    # Angola (GMT+1, sem DST): soma +1 hora e remove tz
    ao = (ts_utc + pd.Timedelta(hours=1)).tz_localize(None)
    return ao

# ---------- Main ----------
def main():
    # 1) Metadados do Pipe (descoberta dinâmica)
    meta = discover_fields(PIPE_ID)
    (OUTPUT_DIR / "pipe_fields_meta.json").write_text(pd.Series(meta).to_json(orient="columns"), encoding="utf-8")
    print(f"Metadados gravados em: {OUTPUT_DIR / 'pipe_fields_meta.json'}")

    # 2) Extrai cards e prepara DataFrame final (4 colunas)
    cards = fetch_all_cards(PIPE_ID)
    rows = []
    for c in cards:
        rows.append({
            "Title": c.get("title"),
            "Current phase": (c.get("current_phase") or {}).get("name"),
            "Creator": (c.get("createdBy") or {}).get("name"),
            "Created at": to_ao_dt(c.get("createdAt")),  # datetime já em GMT+1
        })
    df = pd.DataFrame(rows)

    # Ordena por Created at
    if not df.empty:
        df = df.sort_values(by=["Created at"], ascending=True)

    # 3) Grava Excel (.xlsx) com formato adequado
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    xlsx_path = OUTPUT_DIR / f"cards_report_{ts}.xlsx"

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        # Aba principal
        df.to_excel(writer, sheet_name="Cards", index=False)

        # Ajustes de formatação (openpyxl)
        ws = writer.sheets["Cards"]

        # Formato de data/hora para Excel (yyyy-mm-dd hh:mm:ss)
        from openpyxl.styles import numbers
        # Descobre índice da coluna "Created at"
        created_col_idx = None
        for idx, cell in enumerate(ws[1], start=1):
            if cell.value == "Created at":
                created_col_idx = idx
                break
        if created_col_idx:
            for row in ws.iter_rows(min_row=2, min_col=created_col_idx, max_col=created_col_idx):
                for cell in row:
                    cell.number_format = "yyyy-mm-dd hh:mm:ss"

        # Congelar cabeçalho e filtro
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        # Larguras automáticas (aproximadas)
        for col in ws.columns:
            max_len = max((len(str(c.value)) if c.value is not None else 0) for c in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 60)

        # Aba de metadados (útil para auditoria e evolução do pipe)
        # Start Form
        sff = pd.DataFrame(meta.get("start_form_fields", []))
        sff.to_excel(writer, sheet_name="StartFormFields", index=False)
        # Fases/Fields (explode fase a fase)
        phases = []
        for ph in meta.get("phases", []):
            for f in ph.get("fields", []):
                phases.append({
                    "phase_id": ph.get("phase_id"),
                    "phase_name": ph.get("phase_name"),
                    "field_id": f.get("id"),
                    "field_label": f.get("label"),
                    "field_type": f.get("type")
                })
        pd.DataFrame(phases).to_excel(writer, sheet_name="PhaseFields", index=False)

    print(f"OK: gerado {xlsx_path}")

if __name__ == "__main__":
    main()
