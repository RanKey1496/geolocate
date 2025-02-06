import json
import requests

def generate_query(direccion, nombre, ciudad):
    data = {
    "query": {
        "function_score": {
        "query": {
            "bool": {
            "must": [
                { "match": { "direccion": { "query": direccion } } },
                { "match": { "nombre": { "query": nombre, "boost": 2 } } }
            ],
            "filter": [
                { "term": { "ciudad": ciudad } }
            ]
            }
        },
        "functions": [
            {
            "filter": { "exists": { "field": "id_padre" } },
            "weight": 2
            }
        ],
        "score_mode": "multiply",
        "boost_mode": "multiply"
        }
    },
    "sort": [
        { "_score": { "order": "desc" } },
        { "registro": { "order": "desc" } },
        { "id_padre": { "order": "desc", "missing": "_last" } },
        { "lat": { "order": "desc" } }
    ]
    }
    return json.dumps(data)

def get_hits(url, key, data):
    url = f"{url}?size=5"
    
    headers = {
      'Authorization': f'ApiKey {key}',
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=data)
    json_response = response.json()
    hits = json_response["hits"]["hits"]
    return hits