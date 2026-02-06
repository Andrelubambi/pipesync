import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Depends, BackgroundTasks
from fastapi.responses import FileResponse, StreamingResponse
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_400_BAD_REQUEST

# Importação do motor de relatório
import pipefy_report_excel as report_engine

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

# Configurações do ambiente
API_MASTER_SECRET = os.getenv("EVENT_SECRET_TOKEN", "")
DEFAULT_PIPE_ID = os.getenv("PIPE_ID")

# --- Segurança ---

def validate_api_access(x_api_key: str = Header(None, alias="x-api-key")):
    """
    Verifica se o cliente possui a chave mestra definida no .env.
    Usamos 'x-api-key' para evitar conflitos com o header 'Authorization' padrão.
    """
    if API_MASTER_SECRET and x_api_key != API_MASTER_SECRET:
        logger.error(f"Acesso negado! Chave recebida: {x_api_key} | Esperada: {API_MASTER_SECRET[:8]}...")
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED, 
            detail="Chave de API (x-api-key) inválida ou ausente."
        )
    return x_api_key

# --- Auxiliares ---

def remove_file(path: str):
    """Remove arquivos temporários do servidor após o envio."""
    try:
        Path(path).unlink(missing_ok=True)
        logger.info(f"Arquivo removido: {path}")
    except Exception as e:
        logger.error(f"Erro ao remover arquivo: {e}")

# --- Endpoints ---

@app.get("/", tags=["Health"])
def health_check():
    return {
        "status": "online",
        "server_time": datetime.now().isoformat()
    }

@app.get("/export", tags=["Export"])
def export_stream(
    pipe_id: Optional[str] = Query(None, description="ID do Pipe"),
    pipefy_token: str = Header(..., description="Token JWT do Pipefy"),
    _ : str = Depends(validate_api_access)
):
    """
    Gera e baixa o Excel via Stream (mais eficiente).
    Exige o header 'pipefy-token' e 'x-api-key'.
    """
    target_pipe = pipe_id or DEFAULT_PIPE_ID
    
    if not target_pipe:
        raise HTTPException(status_code=400, detail="PIPE_ID não fornecido.")

    try:
        logger.info(f"Iniciando stream para o Pipe {target_pipe}")
        
        # O motor agora recebe o token dinamicamente
        excel_buffer = report_engine.generate_excel_stream(
            pipe_id=target_pipe, 
            token=pipefy_token
        )
        
        filename = f"Report_{target_pipe}_{datetime.now().strftime('%Y%m%d')}.xlsx"
        
        return StreamingResponse(
            excel_buffer,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        logger.exception("Falha na exportação")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/export-to-server", tags=["Export"])
def export_to_server(
    background_tasks: BackgroundTasks,
    pipefy_token: str = Header(..., description="Token JWT do Pipefy"),
    pipe_id: Optional[str] = Query(None),
    _ : str = Depends(validate_api_access)
):
    """Gera o arquivo fisicamente, envia e depois apaga."""
    target_pipe = pipe_id or DEFAULT_PIPE_ID
    
    try:
        # Assume-se que o motor suporte receber o token aqui também
        xlsx_file_path = report_engine.generate_excel_report_to_server(
            pipe_id=target_pipe, 
            token=pipefy_token
        )
        
        background_tasks.add_task(remove_file, xlsx_file_path)

        return FileResponse(
            path=xlsx_file_path,
            filename=f"Export_{target_pipe}.xlsx",
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        logger.exception("Erro no servidor")
        raise HTTPException(status_code=500, detail="Erro interno ao processar arquivo.")

@app.post("/pipefy/webhook", tags=["Webhooks"])
async def handle_pipefy_webhook(payload: dict, _ : str = Depends(validate_api_access)):
    """Webhook protegido apenas pela chave mestre."""
    logger.info(f"Webhook: {payload.get('action')}")
    return {"status": "received"}