import requests

TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJQaXBlZnkiLCJpYXQiOjE3Njk1MzI1MTQsImp0aSI6IjdiMGMwNzM2LTYxMDItNDc0Yy04OTE5LTEwOWIwMDlhMGE1ZCIsInN1YiI6MzA3MzA0ODMzLCJ1c2VyIjp7ImlkIjozMDczMDQ4MzMsImVtYWlsIjoiYW5kcmVsdWJhbWJpMzZAZ21haWwuY29tIn0sInVzZXJfdHlwZSI6ImF1dGhlbnRpY2F0ZWQifQ.nxAgxrj8g7Yjhtfpnq9yDHbWFVGbqn_EyEIu2ahNYqRGMe-JXKirU4fkF3lRpsUHwJTcaduWygq1zFszichYbg"
PIPE_ID = "306939241"
URL = "https://api.pipefy.com/graphql"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

query = """
{
  allCards(pipeId: "%s") {
    edges {
      node {
        id
        title
        current_phase { name }
      }
    }
  }
}
""" % PIPE_ID

res = requests.post(URL, json={'query': query}, headers=HEADERS).json()

print("\n--- CARDS ENCONTRADOS NO SISTEMA ---")
cards = res['data']['allCards']['edges']
if not cards:
    print("Nenhum card encontrado via API.")
for card in cards:
    c = card['node']
    print(f"ID: {c['id']} | Título: {c['title']} | Fase: {c['current_phase']['name']}")