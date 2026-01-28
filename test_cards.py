# test_cards.py
import os, requests
from dotenv import load_dotenv
load_dotenv()

TOKEN = os.getenv("TOKEN")
PIPE_ID = 306939241

url = "https://api.pipefy.com/graphql"
headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

query = f"""
{{
  cards(pipe_id: {PIPE_ID}, first: 5) {{
    edges {{
      node {{
        id
        title
        createdAt
      }}
    }}
  }}
}}
"""

r = requests.post(url, headers=headers, json={"query": query})
print("STATUS:", r.status_code)
print(r.json())