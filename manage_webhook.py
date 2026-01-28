# manage_webhook.py
import os, requests
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("PIPEFY_TOKEN")
PIPE_ID = int(os.getenv("PIPE_ID"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
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
      "actions": ["card.move", "card.field_update", "card.create"],
      "name": "ETL_PowerBI",
      "pipe_id": PIPE_ID,
      "url": WEBHOOK_URL,
      # Exemplos de filtros (opcionais):
      # "filters": "{\"card.move\": {\"to_phase_id\": [123456]}}"
      # "overridePrevious": True
    }
    r = requests.post(API_URL, headers=HEADERS, json={"query": mutation, "variables": {"input": input_obj}})
    r.raise_for_status()
    print(r.json())

if __name__ == "__main__":
    create_webhook()