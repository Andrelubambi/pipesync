import logging
import os
import io
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Depends, BackgroundTasks, Request
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
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

# Configurações de Ambiente
DEFAULT_API_TOKEN = os.getenv("TOKEN") 
DEFAULT_PIPE_ID = os.getenv("PIPE_ID")
API_MASTER_SECRET = os.getenv("EVENT_SECRET_TOKEN")

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

# --- Auxiliares e Background Tasks ---

def remove_file(path: str):
    """Remove arquivos temporários do servidor após o envio."""
    try:
        Path(path).unlink(missing_ok=True)
        logger.info(f"Arquivo removido: {path}")
    except Exception as e:
        logger.error(f"Erro ao remover arquivo: {e}")

def task_gerar_relatorio_longo(pipe_id: str, token: str, report_id: str):
    """
    Executa a geração pesada (38k+ cards) fora da requisição HTTP.
    Isso evita o erro 502 Bad Gateway no Render.
    """
    try:
        logger.info(f"Iniciando processamento em segundo plano [ID: {report_id}] para o Pipe {pipe_id}")
        # Gera o arquivo no disco usando o seu motor
        path = report_engine.generate_excel_report_to_server(pipe_id=pipe_id, token=token)
        logger.info(f"Relatório [ID: {report_id}] finalizado com sucesso em: {path}")
        # Aqui você poderia adicionar um envio de email ou webhook de conclusão
    except Exception as e:
        logger.error(f"Erro no processamento em segundo plano [ID: {report_id}]: {str(e)}")

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
    Gera e baixa o Excel via Stream. 
    Atenção: Para 38k cards, este endpoint ainda pode sofrer timeout no Render.
    """
    PIPE_ID = pipe_id or DEFAULT_PIPE_ID
    if not PIPE_ID:
        raise HTTPException(status_code=400, detail="PIPE_ID não fornecido.")

    try:
        logger.info(f"Iniciando stream para o Pipe {PIPE_ID}")
        excel_buffer = report_engine.generate_excel_stream(pipe_id=PIPE_ID, token=pipefy_token)
        filename = f"Report_RS_{datetime.now().strftime('%Y%m%d')}.xlsx"
        
        return StreamingResponse(
            excel_buffer,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        logger.exception("Falha na exportação")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/export-to-server", tags=["Export"])
async def export_to_server(
    background_tasks: BackgroundTasks,
    pipefy_token: str = Header(..., description="Token JWT do Pipefy"),
    pipe_id: Optional[str] = Query(None),
    _ : str = Depends(validate_api_access)
):
    """
    Endpoint SEGURO para grandes volumes: Inicia a tarefa e responde 202 imediatamente.
    Evita o Erro 502 do Render.
    """
    target_pipe = pipe_id or DEFAULT_PIPE_ID
    report_id = str(uuid.uuid4())[:8]

    # Agenda a tarefa pesada para rodar em background
    background_tasks.add_task(task_gerar_relatorio_longo, target_pipe, pipefy_token, report_id)

    return {
        "status": "processing",
        "message": "Extração de grande volume iniciada.",
        "report_id": report_id,
        "instruction": "Acompanhe os logs do Render. O arquivo será salvo na pasta /data do servidor."
    }

@app.get("/data-json", tags=["Export"])
def get_data_json_simple():
    """
    Endpoint para Excel/Power Query.
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)