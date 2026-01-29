import os
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from dotenv import load_dotenv

# Importação do motor de relatório
import pipefy_report_excel as report_engine

load_dotenv()

app = FastAPI(title="PipeSync Omatapalo API")

EVENT_SECRET = os.getenv("EVENT_SECRET_TOKEN", "")
DEFAULT_PIPE_ID = os.getenv("PIPE_ID")

@app.get("/")
async def health_check():
    """Verifica se o servidor está online."""
    return {
        "status": "online",
        "service": "PipeSync Omatapalo",
        "server_time": datetime.now().isoformat()
    }

@app.get("/export-to-server")
async def export_pipe_data(
    pipe_id: str = Query(None, description="ID do Pipe no Pipefy"),
    x_secret_token: str = Header(None)
):
    """Endpoint para descarregar o relatório Excel customizado."""
    
    # Validação de Segurança (opcional via URL ou Header)
    if EVENT_SECRET and x_secret_token != EVENT_SECRET:
        print("[AVISO] Tentativa de acesso não autorizada.")
        # Se quiser facilitar para a equipa via link direto, pode comentar a linha abaixo
        # raise HTTPException(status_code=401, detail="Não autorizado")

    target_pipe = pipe_id or DEFAULT_PIPE_ID
    
    if not target_pipe:
        raise HTTPException(status_code=400, detail="PIPE_ID não fornecido.")

    try:
        # Gera o arquivo XLSX
        xlsx_file_path = report_engine.generate_excel_report_to_server(target_pipe)
        
        # Retorna o arquivo para o browser
        return FileResponse(
            path=xlsx_file_path,
            filename=f"Export_Pipe_{target_pipe}.xlsx",
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        print(f"[ERRO CRÍTICO] Falha ao gerar exportação: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro interno ao processar Excel.")
    
@app.get("/export")
async def export_pipe_data(
    pipe_id: str = Query(None, description="ID do Pipe no Pipefy"),
    x_secret_token: str = Header(None)
):
    """Gera e envia o Excel diretamente para o navegador via stream."""
    
    # Segurança básica
    if EVENT_SECRET and x_secret_token != EVENT_SECRET:
        print("[AVISO] Tentativa de acesso não autorizada.")
        # Se desejar acesso direto via browser, pode relaxar esta condição

    target_pipe = pipe_id or DEFAULT_PIPE_ID
    
    if not target_pipe:
        raise HTTPException(status_code=400, detail="PIPE_ID não fornecido.")

    try:
        # Gera o stream do Excel (sem salvar no disco)
        excel_buffer = report_engine.generate_excel_stream(target_pipe)
        
        filename = f"Report_Pipe_{target_pipe}_{datetime.now().strftime('%Y%m%d')}.xlsx"
        
        # Retorna o stream como um download direto
        return StreamingResponse(
            excel_buffer,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except Exception as e:
        print(f"[ERRO CRÍTICO] Falha ao processar exportação: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")    

@app.post("/pipefy/webhook")
async def handle_pipefy_webhook(payload: dict, x_secret_token: str = Header(None)):
    """Recebe eventos do Pipefy (Apenas para logs ou futuras automações)."""
    if EVENT_SECRET and x_secret_token != EVENT_SECRET:
        raise HTTPException(status_code=401)

    # Aqui você pode adicionar lógica para processar mudanças em tempo real
    # Por agora, apenas confirmamos a receção para o Pipefy não dar erro
    print(f"[WEBHOOK] Evento recebido: {payload.get('action')}")
    return {"status": "received"}

# Comando para rodar:
# uvicorn webhook_server:app --host 0.0.0.0 --port 8080