# manage_webhook.py
import os, json, requests
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("PIPEFY_TOKEN")
PIPE_ID = os.getenv("PIPE_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # ex.: https://tua-api.com/pipefy/webhook
API_URL = "https://api.pipefy.com/graphql"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def create_webhook():
    mutation = """
    mutation CreateHook($input: CreateWebhookInput!) {
      createWebhook(input: $input) {
        webhook { id actions url }
      }
    }
    """
    input_obj = {
      "actions": ["card.move", "card.field_update"],
      "name": "ETL_PowerBI",
      "pipe_id": int(PIPE_ID),
      "url": WEBHOOK_URL,
      # Exemplo de filtros: só movimentos para a fase X
      # "filters": "{\"card.move\": {\"to_phase_id\": [123456]}}"
    }
    r = requests.post(API_URL, headers=HEADERS, json={"query": mutation, "variables": {"input": input_obj}})
    r.raise_for_status()
    print(r.json())

if __name__ == "__main__":
    create_webhook()