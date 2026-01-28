import requests

TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJQaXBlZnkiLCJpYXQiOjE3Njk1MzI1MTQsImp0aSI6IjdiMGMwNzM2LTYxMDItNDc0Yy04OTE5LTEwOWIwMDlhMGE1ZCIsInN1YiI6MzA3MzA0ODMzLCJ1c2VyIjp7ImlkIjozMDczMDQ4MzMsImVtYWlsIjoiYW5kcmVsdWJhbWJpMzZAZ21haWwuY29tIn0sInVzZXJfdHlwZSI6ImF1dGhlbnRpY2F0ZWQifQ.nxAgxrj8g7Yjhtfpnq9yDHbWFVGbqn_EyEIu2ahNYqRGMe-JXKirU4fkF3lRpsUHwJTcaduWygq1zFszichYbg"
PIPE_ID = "306939241"
PHASE_ID = "341842076" # ID da sua fase Inbox
URL = "https://api.pipefy.com/graphql"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Vamos tentar criar o card mais simples possível, sem campos extras primeiro
mutation = {
    "query": """
    mutation {
      createCard(input: {
        pipe_id: "%s",
        phase_id: "%s",
        title: "Teste Unitário André",
      }) { card { id title } }
    }
    """ % (PIPE_ID, PHASE_ID)
}

response = requests.post(URL, json=mutation, headers=HEADERS)
res_json = response.json()

if "errors" in res_json:
    print("❌ ERRO REAL DA API:")
    print(res_json["errors"][0]["message"])
else:
    print("✅ SUCESSO!")
    print(f"Card criado com ID: {res_json['data']['createCard']['card']['id']}")