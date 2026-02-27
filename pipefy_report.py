import os
import io
import logging
from pathlib import Path
from datetime import datetime
import requests
import time
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
PHASE_ID_IGNORAR = "317368281"

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

PHASE_CARDS_QUERY = """
query GetPhaseCards($phaseId: ID!, $first: Int!, $after: String) {
  phase(id: $phaseId) {
    cards(first: $first, after: $after) {
      pageInfo {
        hasNextPage
        endCursor
      }
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
}
"""

REFINED_DATA_QUERY = """
query GetRefinedCards($phaseId: ID!, $first: Int!, $after: String) {
  phase(id: $phaseId) {
    cards(first: $first, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id
          title
          createdAt
          updated_at
          createdBy { name }
          current_phase { name }
          assignees { name }
          labels { name }
          fields { name value }
          phases_history {
            phase { name }
            firstTimeIn
            lastTimeOut
          }
        }
      }
    }
  }
}
"""

# ---------- Funções de Apoio ----------
def process_refined_data(cards_nodes):
    v_agora = pd.Timestamp.now(tz='UTC')
    rows = []

    for card in cards_nodes:
        # Extração de campos customizados do Pipefy
        #fields = {f["label"]: f["value"] for f in card.get("fields", [])}
        fields = {f["name"]: f["value"] for f in card.get("fields", [])}
        history = card.get("phases_history", [])

        # Função auxiliar para calcular tempo total numa fase específica
        def calc_tempo_fase(nome_fase):
            total = pd.Timedelta(0)
            entries = [h for h in history if h["phase"]["name"] == nome_fase]
            for e in entries:
                t_in = pd.to_datetime(e["firstTimeIn"], utc=True)
                t_out = pd.to_datetime(e["lastTimeOut"], utc=True) if e["lastTimeOut"] else v_agora
                total += (t_out - t_in)
            return total.days

        # Função auxiliar para pegar a primeira data de entrada numa fase
        def data_primeira_entrada(nome_fase):
            entries = [h for h in history if h["phase"]["name"] == nome_fase]
            if not entries: return None
            # Pega a menor data de entrada
            primeira = min([pd.to_datetime(e["firstTimeIn"], utc=True) for e in entries])
            # Formata para o tempo de Angola (UTC+1)
            return (primeira + pd.Timedelta(hours=1)).tz_localize(None)

        # Montagem do dicionário seguindo EXATAMENTE a ordem das colunas pedidas
        registro = {
            "Título": card.get("title"),
            "Fase atual": (card.get("current_phase") or {}).get("name"),
            "Criador": (card.get("createdBy") or {}).get("name"),
            "Responsáveis": ", ".join([a["name"] for a in card.get("assignees", [])]),
            "Criado em": format_to_angola_time(card.get("createdAt")),
            "DRH - Local da Admissão": fields.get("DRH - Local da Admissão"),
            "Tempo total na fase 2.13 Envio Infor. DTI | DT| RH (dias)": calc_tempo_fase("2.13 Envio Infor. DTI | DT| RH"),
            "Tempo total na fase 2.12 Candidatos Contratados (dias)": calc_tempo_fase("2.12 Candidatos Contratados"),
            "Tempo total na fase 2.11.1 Validação da Administração (dias)": calc_tempo_fase("2.11.1 Validação da Administração"),
            "Tempo total na fase 2.11 Apresentar Proposta (dias)": calc_tempo_fase("2.11 Apresentar Proposta"),
            "Tempo total na fase 2.10.1 Validação DCH (dias)": calc_tempo_fase("2.10.1 Validação DCH"),
            "Tempo total na fase 2.18 Candidatos em Standby (dias)": calc_tempo_fase("2.18 Candidatos em Standby"),
            "Tempo total na fase 2.3 Marcar Entrevista (dias)": calc_tempo_fase("2.3 Marcar Entrevista"),
            "Tempo total na fase 2.6 Marcar Entrevista Técnica (dias)": calc_tempo_fase("2.6 Marcar Entrevista Técnica"),
            "Tempo total na fase 2.5 Avaliação pelo Cliente (dias)": calc_tempo_fase("2.5 Avaliação pelo Cliente"),
            "Tempo total na fase 2.4 Relatório da Entrevista DRS (dias)": calc_tempo_fase("2.4 Relatório da Entrevista DRS"),
            "Tempo total na fase 2.10 Validação Cliente (dias)": calc_tempo_fase("2.10 Validação Cliente"),
            "Tempo total na fase 2.11.0 Resposta do candidato (dias)": calc_tempo_fase("2.11.0 Resposta do candidato"),
            "Código": card.get("id"),
            "Etiquetas": ", ".join([l["name"] for l in card.get("labels", [])]),
            "Atualizado em": format_to_angola_time(card.get("updated_at")),
            "Oportunidade de Emprego": fields.get("Oportunidade de Emprego"),
            "Nome": fields.get("Nome"),
            "Nacionalidade": fields.get("Nacionalidade"),
            "Empresa Actual": fields.get("Empresa Actual"),
            "Grau de Habilitação Académica": fields.get("Grau de Habilitação Académica"),
            "Área de Formação": fields.get("Área de Formação"),
            "AV- Motivação do Candidato": fields.get("AV- Motivação do Candidato"),
            "AV-Percurso Profissional": fields.get("AV-Percurso Profissional"),
            "AV-Percurso Académico": fields.get("AV-Percurso Académico"),
            "Parecer do Recrutador": fields.get("Parecer do Recrutador"),
            "Tipo de Entrevista": fields.get("Tipo de Entrevista"),
            "Parecer do DRH. DRS": fields.get("Parecer do DRH. DRS"),
            "Parecer Técnico": fields.get("Parecer Técnico"),
            "Tempo total na fase 2.8 Elaborar Proposta (dias)": calc_tempo_fase("2.8 Elaborar Proposta"),
            "Primeira vez que entrou na fase 2.12 Candidatos Contratados": data_primeira_entrada("2.12 Candidatos Contratados"),
            "Tempo total na fase 2.7 Avaliação pelo Cliente (dias)": calc_tempo_fase("2.7 Avaliação pelo Cliente"),
            "Primeira vez que entrou na fase 2.17 Candidatos Desistiram": data_primeira_entrada("2.17 Candidatos Desistiram"),
            "Tempo total na fase 2.2  Breve Avaliação do Candidato (dias)": calc_tempo_fase("2.2  Breve Avaliação do Candidato"),
            "Primeira vez que entrou na fase 2.14 Processos Concluídos": data_primeira_entrada("2.14 Processos Concluídos"),
            "Tempo total na fase 2.9 Validação DRS (dias)": calc_tempo_fase("2.9 Validação DRS"),
        }
        rows.append(registro)

    return pd.DataFrame(rows)


def generate_refined_excel(pipe_id: str, token: str):
    logger.info(f"Gerando relatório refinado para Pipe: {pipe_id}")
    # Nota: Reutiliza a lógica de fetch_all_cards mas passando a nova query REFINED_DATA_QUERY
    # Para brevidade, assumimos que fetch_all_cards foi adaptada ou usa esta query.
    cards = fetch_all_cards_refined(pipe_id, token) 
    df = process_refined_data(cards)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Dados Refinados", index=False)
    output.seek(0)
    return output

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



def execute_gql(query: str, variables: dict, token: str, retries=3):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    for i in range(retries):
        try:
            response = requests.post(API_URL, headers=headers, json={"query": query, "variables": variables}, timeout=60)
            data = response.json()
            
            if "errors" in data:
                # Se for erro de servidor, tentamos novamente após um breve delay
                if any(err.get('extensions', {}).get('code') == 'INTERNAL_SERVER_ERROR' for err in data['errors']):
                    logger.warning(f"Tentativa {i+1} falhou com erro de servidor. Tentando novamente...")
                    time.sleep(2 * (i + 1)) # Espera progressiva (2s, 4s...)
                    continue
                raise RuntimeError(f"Erro na Query: {data['errors']}")
            
            return data["data"]
        except (requests.exceptions.RequestException, Exception) as e:
            if i == retries - 1: raise e
            time.sleep(2)

def get_valid_phase_ids(pipe_id: str, token: str):
    phases = get_pipe_phases_details(pipe_id, token)

    if not isinstance(phases, list):
        raise RuntimeError("Erro ao obter fases do pipe")

    return [
        p["id"]
        for p in phases
        if p["id"] != PHASE_ID_IGNORAR
    ]            

def fetch_all_cards_refined(pipe_id: str, token: str):
    """
    Versão otimizada que busca cards de todas as fases usando a query refinada.
    """
    all_nodes = []
    start_time = time.time()

    logger.info("🔎 Obtendo fases para relatório refinado...")
    phases = get_pipe_phases_details(pipe_id, token)

    # Filtrar fases a ignorar
    valid_phases = [
        p for p in phases
        if p["id"] != PHASE_ID_IGNORAR
    ]

    for phase in valid_phases:
        logger.info(f"➡ Extraindo dados refinados da fase: {phase['name']}")
        cursor = None

        while True:
            variables = {
                "phaseId": phase["id"],
                "first": 50,
                "after": cursor
            }

            # Importante: Usa a REFINED_DATA_QUERY definida no topo do seu script
            data = execute_gql(REFINED_DATA_QUERY, variables, token)
            
            if not data or "phase" not in data:
                break

            cards_data = data["phase"]["cards"]
            batch_nodes = [edge["node"] for edge in cards_data["edges"]]
            all_nodes.extend(batch_nodes)

            if cards_data["pageInfo"]["hasNextPage"]:
                cursor = cards_data["pageInfo"]["endCursor"]
            else:
                break

    logger.info(f"✅ Extração concluída: {len(all_nodes)} cards processados.")
    logger.info(f"⏱ Tempo decorrido: {time.time() - start_time:.2f}s")

    return all_nodes

def fetch_all_cards(pipe_id: str, token: str):
    all_nodes = []
    start_time = time.time()

    logger.info("🔎 Obtendo fases...")
    phases = get_pipe_phases_details(pipe_id, token)

    valid_phases = [
        p for p in phases
        if p["id"] != "317368281"
    ]

    logger.info(f"📊 Fases consideradas: {len(valid_phases)}")

    for phase in valid_phases:
        logger.info(f"➡ Processando fase: {phase['name']}")

        cursor = None

        while True:
            variables = {
                "phaseId": phase["id"],
                "first": 50,
                "after": cursor
            }

            data = execute_gql(PHASE_CARDS_QUERY, variables, token)
            cards_data = data["phase"]["cards"]

            batch_nodes = [edge["node"] for edge in cards_data["edges"]]
            all_nodes.extend(batch_nodes)

            if cards_data["pageInfo"]["hasNextPage"]:
                cursor = cards_data["pageInfo"]["endCursor"]
            else:
                break

    logger.info(f"✅ Total final de cards: {len(all_nodes)}")
    logger.info(f"⏱ Tempo total: {time.time() - start_time:.2f}s")

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
                "Criado em": format_to_angola_time(card.get("createdAt")),
                "Fase Atual": (card.get("current_phase") or {}).get("name"),
                "Fase do Histórico": (history.get("phase") or {}).get("name"),
                "Entrada na Fase": format_to_angola_time(history.get("firstTimeIn")),
                "Saída da Fase": format_to_angola_time(history.get("lastTimeOut")) if time_out else "Ainda na fase",
                "Tempo Gasto na Fase": duracao_fase,
                "Última Atualização": format_to_angola_time(card.get("updated_at")),
                "Há quanto tempo atualizado": tempo_desde_att
            })
    
    return pd.DataFrame(rows)

def get_pipe_phases_details(pipe_id: str, token: str):
    """
    Retorna os nomes e IDs das fases do Pipe.
    """
    query = """
    query ($pipeId: ID!){
      pipe(id: $pipeId){
        phases {
          id
          name
        }
      }
    }
    """
    try:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        # Note que removi o ponto e vírgula conforme sua instrução
        response = requests.post(API_URL, headers=headers, json={"query": query, "variables": {"pipeId": pipe_id}}, timeout=10)
        data = response.json()
        
        if "errors" in data:
            return {"erro": data["errors"]}
            
        phases = data["data"]["pipe"]["phases"]
        # Retorna uma lista de objetos com ID e Nome
        return [{"id": p["id"], "name": p["name"]} for p in phases]
        
    except Exception as e:
        return {"erro": f"Falha na conexão: {str(e)}"}

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
   # file_path = OUTPUT_DIR / f"report_{pipe_id}_{timestamp}.xlsx"
    file_path = OUTPUT_DIR / f"report_rs_{timestamp}.xlsx"
    
    with open(file_path, "wb") as f:
        f.write(buffer.getbuffer())
    return str(file_path)

