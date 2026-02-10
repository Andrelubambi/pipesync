import os
import io
import logging
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv

# Configuração de Logging corrigida (usando logging padrão em vez de asyncio.log)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Configurações ----------
load_dotenv(find_dotenv(usecwd=True))

TOKEN = os.getenv("PIPEFY_TOKEN") or os.getenv("TOKEN")
API_URL = "https://api.pipefy.com/graphql"
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./data"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Queries GraphQL ----------
PIPE_SCHEMA_QUERY = """
query ($pipeId: ID!){
  pipe(id: $pipeId){
    id
    name
    phases {
      name
      fields { label type }
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
        id
        title
        createdAt
        updated_at
        createdBy { name }
        current_phase { name }
        phases_history {
          phase { name }
          firstTimeIn
          lastTimeOut
        }
      }
    }
  }
}
"""

# ---------- Funções de Apoio ----------

def format_to_angola_time(iso_date: str):
    if not iso_date: return None
    ts_utc = pd.to_datetime(iso_date, utc=True, errors="coerce")
    if pd.isna(ts_utc): return None
    # UTC+1 para Angola
    return (ts_utc + pd.Timedelta(hours=1)).tz_localize(None)

def format_duration_human(duration):
    """Equivalente à fnFormatarTempo do Power Query"""
    if pd.isna(duration) or duration.total_seconds() <= 0:
        return "Agora"
    
    days = duration.days
    years = days // 365
    remaining_days = days % 365
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    
    parts = []
    if years > 0: parts.append(f"{years} ano(s)")
    if remaining_days > 0: parts.append(f"{remaining_days}d")
    if hours > 0: parts.append(f"{hours}h")
    if minutes > 0 or not parts: parts.append(f"{minutes}min")
    
    return " ".join(parts)

# ---------- Processamento de Dados ----------

def execute_gql(query: str, variables: dict, token: str):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(API_URL, headers=headers, json={"query": query, "variables": variables}, timeout=60)
    data = response.json()
    if "errors" in data:
        raise RuntimeError(f"Erro na Query: {data['errors']}")
    return data["data"]

def fetch_all_cards(pipe_id: str, token: str):
    cursor = None
    all_nodes = []
    while True:
        data = execute_gql(CARDS_QUERY, {"pipeId": pipe_id, "first": 50, "after": cursor}, token)
        cards_data = data["cards"]
        all_nodes.extend([edge["node"] for edge in cards_data["edges"]])
        if cards_data["pageInfo"]["hasNextPage"]:
            cursor = cards_data["pageInfo"]["endCursor"]
        else:
            break
    return all_nodes

def process_cards_to_history_df(cards_nodes):
    v_agora = pd.Timestamp.now(tz='UTC')
    rows = []

    for card in cards_nodes:
        updated_at = pd.to_datetime(card.get("updated_at"), utc=True)
        tempo_desde_att = format_duration_human(v_agora - updated_at)

        for history in card.get("phases_history", []):
            time_in = pd.to_datetime(history.get("firstTimeIn"), utc=True)
            time_out = pd.to_datetime(history.get("lastTimeOut"), utc=True) if history.get("lastTimeOut") else None
            
            fim_periodo = time_out if time_out else v_agora
            duracao_fase = format_duration_human(fim_periodo - time_in)

            rows.append({
                "Card ID": card.get("id"),
                "Título": card.get("title"),
                "Criador": (card.get("createdBy") or {}).get("name"),
                "Fase Atual": (card.get("current_phase") or {}).get("name"),
                "Fase do Histórico": (history.get("phase") or {}).get("name"),
                "Entrada na Fase": format_to_angola_time(history.get("firstTimeIn")),
                "Saída da Fase": format_to_angola_time(history.get("lastTimeOut")) if time_out else "Ainda na fase",
                "Tempo Gasto na Fase": duracao_fase,
                "Última Atualização": format_to_angola_time(card.get("updated_at")),
                "Há quanto tempo atualizado": tempo_desde_att
            })
    
    return pd.DataFrame(rows)

# ---------- Geração do Excel ----------

def generate_excel_stream(pipe_id: str, token: str):
    """Gera o Excel em memória e retorna o buffer BytesIO."""
    logger.info(f"Iniciando processamento para o Pipe: {pipe_id}")
    
    # 1. Coleta
    cards = fetch_all_cards(pipe_id, token)
    meta = execute_gql(PIPE_SCHEMA_QUERY, {"pipeId": pipe_id}, token)["pipe"]
    
    # 2. Transformação
    df_history = process_cards_to_history_df(cards)
    
    # 3. Escrita em Buffer
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Aba 1: Histórico
        df_history.to_excel(writer, sheet_name="Histórico de Fases", index=False)
        ws = writer.sheets["Histórico de Fases"]
        
        # Auto-ajuste de colunas
        for col in ws.columns:
            max_length = max((len(str(cell.value)) if cell.value else 0) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_length + 2, 60)

        # Aba 2: Estrutura (Metadados)
        phases_data = []
        for ph in meta.get("phases", []):
            for f in ph.get("fields", []):
                phases_data.append({"Fase": ph["name"], "Campo": f["label"], "Tipo": f["type"]})
        pd.DataFrame(phases_data).to_excel(writer, sheet_name="Estrutura_Pipe", index=False)

    output.seek(0)
    return output

def generate_excel_report_to_server(pipe_id: str, token: str):
    """Salva no disco para uso local ou debug."""
    buffer = generate_excel_stream(pipe_id, token)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = OUTPUT_DIR / f"report_{pipe_id}_{timestamp}.xlsx"
    
    with open(file_path, "wb") as f:
        f.write(buffer.getbuffer())
    return str(file_path)