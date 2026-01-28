import requests
import time
from datetime import datetime

# --- CONFIGURAÇÕES ---
TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJQaXBlZnkiLCJpYXQiOjE3Njk1MzI1MTQsImp0aSI6IjdiMGMwNzM2LTYxMDItNDc0Yy04OTE5LTEwOWIwMDlhMGE1ZCIsInN1YiI6MzA3MzA0ODMzLCJ1c2VyIjp7ImlkIjozMDczMDQ4MzMsImVtYWlsIjoiYW5kcmVsdWJhbWJpMzZAZ21haWwuY29tIn0sInVzZXJfdHlwZSI6ImF1dGhlbnRpY2F0ZWQifQ.nxAgxrj8g7Yjhtfpnq9yDHbWFVGbqn_EyEIu2ahNYqRGMe-JXKirU4fkF3lRpsUHwJTcaduWygq1zFszichYbg"
PIPE_ID = "306939241"
PHASE_ID = "341842076" # Inbox
URL = "https://api.pipefy.com/graphql"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
META_TOTAL = 200

def get_current_count():
    query = "{ pipe(id: \"%s\") { cards_count } }" % PIPE_ID
    res = requests.post(URL, json={'query': query}, headers=HEADERS).json()
    return res['data']['pipe']['cards_count']

def create_cards(quantidade, starting_number):
    print(f"🚀 Criando {quantidade} cards usando campos reais (primeiro_teste e date)...")
    
    for i in range(1, quantidade + 1):
        num = starting_number + i
        agora = datetime.now()
        data_iso = agora.strftime("%Y-%m-%d") # Formato para o campo 'date'
        hora_str = agora.strftime("%H:%M:%S")
        
        mutation = {
            "query": """
            mutation($input: CreateCardInput!) {
              createCard(input: $input) { card { id } }
            }
            """,
            "variables": {
                "input": {
                    "pipe_id": PIPE_ID,
                    "phase_id": PHASE_ID,
                    "title": f"Card Automático #{num}",
                    "fields_attributes": [
                        {"field_id": "primeiro_teste", "field_value": f"Automação André - Hora: {hora_str}"},
                        {"field_id": "date", "field_value": data_iso}
                    ]
                }
            }
        }

        response = requests.post(URL, json=mutation, headers=HEADERS)
        res_data = response.json()
        
        if "errors" in res_data:
            print(f"❌ Erro fatal no card {num}: {res_data['errors'][0]['message']}")
            break

        if i % 10 == 0:
            print(f"✅ Sucesso: {i}/{quantidade} cards criados...")
        
        time.sleep(0.05)

if __name__ == "__main__":
    count = get_current_count()
    print(f"📊 Cards atuais no Pipe: {count}")
    
    if count < META_TOTAL:
        faltam = META_TOTAL - count
        create_cards(faltam, count)
        print("🏁 Processo concluído! Verifique seu Inbox.")
    else:
        print("✅ Meta de 1000 cards já atingida.")