import requests

TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJQaXBlZnkiLCJpYXQiOjE3Njk1MzI1MTQsImp0aSI6IjdiMGMwNzM2LTYxMDItNDc0Yy04OTE5LTEwOWIwMDlhMGE1ZCIsInN1YiI6MzA3MzA0ODMzLCJ1c2VyIjp7ImlkIjozMDczMDQ4MzMsImVtYWlsIjoiYW5kcmVsdWJhbWJpMzZAZ21haWwuY29tIn0sInVzZXJfdHlwZSI6ImF1dGhlbnRpY2F0ZWQifQ.nxAgxrj8g7Yjhtfpnq9yDHbWFVGbqn_EyEIu2ahNYqRGMe-JXKirU4fkF3lRpsUHwJTcaduWygq1zFszichYbg"
PIPE_ID = "306939241"
URL = "https://api.pipefy.com/graphql"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

query = "{ pipe(id: \"%s\") { start_form_fields { id label } } }" % PIPE_ID
res = requests.post(URL, json={'query': query}, headers=HEADERS).json()

print("\n--- LISTA DE CAMPOS ENCONTRADOS ---")
for field in res['data']['pipe']['start_form_fields']:
    print(f"ID: {field['id']}  |  Nome: {field['label']}")
print("-----------------------------------\n")