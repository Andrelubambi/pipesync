from asyncio.log import logger
import os
import io
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from openpyxl.styles import numbers

# ---------- Configurações ----------
load_dotenv(find_dotenv(usecwd=True))

TOKEN = os.getenv("PIPEFY_TOKEN") or os.getenv("TOKEN")
API_URL = "https://api.pipefy.com/graphql"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./data"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Queries GraphQL ----------
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

def execute_gql(query: str, variables: dict, token: str):
    """Executa chamadas GraphQL usando o token fornecido dinamicamente."""
    headers = {
        "Authorization": f"Bearer {token}", 
        "Content-Type": "application/json"
    }
    response = requests.post(API_URL, headers=headers, json={"query": query, "variables": variables}, timeout=60)
    
    if response.status_code != 200:
        raise RuntimeError(f"Erro na API Pipefy: {response.status_code} - {response.text}")
    
    data = response.json()
    if "errors" in data:
        raise RuntimeError(f"Erro na Query: {data['errors']}")
    return data["data"]

def get_pipe_metadata(pipe_id: str, token: str):
    """Obtém estrutura do pipe com o token dinâmico."""
    data = execute_gql(PIPE_SCHEMA_QUERY, {"pipeId": pipe_id}, token)
    pipe = data["pipe"] or {}
    return {
        "id": pipe.get("id"),
        "name": pipe.get("name"),
        "start_fields": pipe.get("start_form_fields", []),
        "phases": pipe.get("phases", [])
    }

def fetch_all_cards(pipe_id: str, token: str, page_size: int = 200):
    """Busca todos os cards com o token dinâmico."""
    cursor = None
    all_nodes = []
    
    while True:
        data = execute_gql(CARDS_QUERY, {"pipeId": pipe_id, "first": page_size, "after": cursor}, token)
        cards_data = data["cards"]
        all_nodes.extend([edge["node"] for edge in cards_data["edges"]])
        
        if cards_data["pageInfo"]["hasNextPage"]:
            cursor = cards_data["pageInfo"]["endCursor"]
        else:
            break
    return all_nodes

def format_to_angola_time(iso_date: str):
    if not iso_date: return None
    ts_utc = pd.to_datetime(iso_date, utc=True, errors="coerce")
    if pd.isna(ts_utc): return None
    return (ts_utc + pd.Timedelta(hours=1)).tz_localize(None)

def generate_excel_stream(pipe_id: str, token: str):
    """Gera o Excel em memória usando o token recebido da API."""
    logger.info(f"Gerando stream para Pipe: {pipe_id}")
    
    # 1. Obter Dados
    meta = get_pipe_metadata(pipe_id, token)
    cards = fetch_all_cards(pipe_id, token)
    
    # 2. Processar DataFrame Principal
    rows = [{
        "Title": c.get("title"),
        "Current phase": (c.get("current_phase") or {}).get("name"),
        "Creator": (c.get("createdBy") or {}).get("name"),
        "Created at": format_to_angola_time(c.get("createdAt")),
    } for c in cards]
    
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(by=["Created at"], ascending=True)

    # 3. Gerar Ficheiro Excel
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = OUTPUT_DIR / f"report_{pipe_id}_{timestamp}.xlsx"

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Cards", index=False)
        ws = writer.sheets["Cards"]

        # Formatação de Data e Layout
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        
        for col in ws.columns:
            max_length = max((len(str(cell.value)) if cell.value else 0) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_length + 2, 50)
            
            if col[0].value == "Created at":
                for cell in col[1:]:
                    cell.number_format = "yyyy-mm-dd hh:mm:ss"

        # Aba de Metadados (Fases e Campos)
        phases_data = []
        for ph in meta["phases"]:
            for f in ph.get("fields", []):
                phases_data.append({
                    "Phase": ph.get("name"),
                    "Field Label": f.get("label"),
                    "Type": f.get("type")
                })
        pd.DataFrame(phases_data).to_excel(writer, sheet_name="Structure_Metadata", index=False)

    logger.info(f"[SUCESSO] Relatório gerado em: {file_path}")
    return file_path


def generate_excel_report_to_server(pipe_id: str, token: str):
    """Gera o Excel no disco usando o token dinâmico."""
    buffer = generate_excel_stream(pipe_id, token)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = OUTPUT_DIR / f"report_{pipe_id}_{timestamp}.xlsx"
    
    with open(file_path, "wb") as f:
        f.write(buffer.getbuffer())
        
    return str(file_path)

def generate_excel_stream(pipe_id: str, token: str):
    """Gera o relatório Excel em memória e retorna o buffer BytesIO."""
    logger.info(f"Gerando stream para Pipe: {pipe_id}")
    
    # 1. Obter Dados
    meta = get_pipe_metadata(pipe_id, token)
    cards = fetch_all_cards(pipe_id, token)
    
    # 2. Processar Dados
    rows = [{
        "Title": c.get("title"),
        "Current phase": (c.get("current_phase") or {}).get("name"),
        "Creator": (c.get("createdBy") or {}).get("name"),
        "Created at": format_to_angola_time(c.get("createdAt")),
    } for c in cards]
    
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(by=["Created at"], ascending=True)

    # 3. Criar Excel em Memória
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Aba principal
        df.to_excel(writer, sheet_name="Cards", index=False)
        ws = writer.sheets["Cards"]

        # Formatação profissional
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        
        for col in ws.columns:
            max_length = max((len(str(cell.value)) if cell.value else 0) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_length + 2, 50)
            
            if col[0].value == "Created at":
                for cell in col[1:]:
                    cell.number_format = "yyyy-mm-dd hh:mm:ss"

        # Aba de Metadados
        phases_data = []
        for ph in meta["phases"]:
            for f in ph.get("fields", []):
                phases_data.append({
                    "Phase": ph.get("name"),
                    "Field Label": f.get("label"),
                    "Type": f.get("type")
                })
        pd.DataFrame(phases_data).to_excel(writer, sheet_name="Structure_Metadata", index=False)

    # Move o ponteiro para o início do arquivo virtual
    output.seek(0)
    logger.info(f"[SUCESSO] Stream do Excel pronto para o Pipe: {pipe_id}")
    return output