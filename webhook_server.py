import logging
import os
import io
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Depends, Request
from fastapi.responses import FileResponse, StreamingResponse
from starlette.status import HTTP_401_UNAUTHORIZED

# Importação do motor de relatório
import pipefy_report as report_engine

# Configuração de Logging para facilitar o debug no terminal
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(
    title="PipeSync Omatapalo API",
    description="API para exportação dinâmica de dados do Pipefy"
)

# Configurações de Ambiente
DEFAULT_API_TOKEN = os.getenv("TOKEN") 
DEFAULT_PIPE_ID = os.getenv("PIPE_ID")
API_MASTER_SECRET = os.getenv("EVENT_SECRET_TOKEN")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./output"))
EXPIRE_HOURS = int(os.getenv("EXPIRE_HOURS", 24))
OUTPUT_DIR.mkdir(exist_ok=True)

# --- Segurança ---

def validate_api_access(x_api_key: str = Header(None, alias="x-api-key")):
    """
    Verifica se o cliente possui a chave mestra definida no .env.
    """
    if API_MASTER_SECRET and x_api_key != API_MASTER_SECRET:
        logger.error(f"Acesso negado! Chave recebida: {x_api_key}")
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED, 
            detail="Chave de API (x-api-key) inválida ou ausente."
        )
    return x_api_key

def cleanup_old_files():
    """Remove arquivos antigos do OUTPUT_DIR para não lotar o servidor."""
    now = datetime.now()
    for f in OUTPUT_DIR.glob("*.xlsx"):
        file_age = now - datetime.fromtimestamp(f.stat().st_mtime)
        if file_age > timedelta(hours=EXPIRE_HOURS):
            try:
                f.unlink()
                logger.info(f"Arquivo antigo removido: {f.name}")
            except Exception as e:
                logger.warning(f"Falha ao remover {f.name}: {e}")

# --- Endpoints ---

@app.get("/", tags=["Health"])
def health_check():
    return {
        "status": "online",
        "server_time": datetime.now().isoformat()
    }

@app.get("/export", tags=["Export"])
def export_report(
    pipe_id: str = Query(..., description="ID do Pipe"),
    pipefy_token: str = Header(..., description="Token JWT do Pipefy")
):
    """
    Gera o Excel no servidor e retorna um link para download.
    Arquivos antigos serão limpos automaticamente.
    """
    cleanup_old_files()  # Limpa arquivos antigos

    try:
        logger.info(f"Iniciando geração de relatório para Pipe: {pipe_id}")
        
        # 1️⃣ Gerar Excel no disco
        file_path = report_engine.generate_excel_report_to_server(pipe_id, pipefy_token)
        logger.info(f"Relatório gerado: {file_path}")

        # 2️⃣ Retornar o arquivo para download
        return FileResponse(
            path=file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=file_path.name
        )
    
    except Exception as e:
        logger.exception("Falha na geração do relatório")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data-json", tags=["Export"])
def get_data_json_simple():
    """
    Endpoint para visualização rápida de dados em formato JSON.
    """
    try:
        if not DEFAULT_API_TOKEN or not DEFAULT_PIPE_ID:
            raise HTTPException(status_code=500, detail="Configurações ausentes no servidor.")

        cards = report_engine.fetch_all_cards(DEFAULT_PIPE_ID, DEFAULT_API_TOKEN)
        df = report_engine.process_cards_to_history_df(cards)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Erro no processamento JSON: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 

@app.post("/pipefy/webhook", tags=["Webhooks"])
async def handle_pipefy_webhook(request: Request):
    try:
        payload = await request.json()
        logger.info(f"Webhook recebido: {payload.get('action')}")
        return {"status": "received"}
    except Exception:
        logger.error("Erro ao processar webhook", exc_info=True)
        raise HTTPException(status_code=400, detail="Payload inválido.")
    
@app.get("/debug-phases", tags=["Debug"])
def debug_phases(
    pipe_id: Optional[str] = Query(None),
    pipefy_token: str = Header(...)
):
    pid = pipe_id or DEFAULT_PIPE_ID
    # Agora recebemos a lista completa (ID + Nome)
    fases_detalhadas = report_engine.get_pipe_phases_details(pid, pipefy_token)
    
    return {
        "pipe_id": pid, 
        "total_fases": len(fases_detalhadas) if isinstance(fases_detalhadas, list) else 0,
        "fases": fases_detalhadas
    }       

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)