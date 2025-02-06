import json
import time
import requests
from sentence_transformers import SentenceTransformer, util

start_time = time.time()
model = SentenceTransformer('all-MiniLM-L6-v2')

url = "http://:9200/localizaciones/_search?size=4"

def get_query(direccion, nombre, ciudad):
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

def get_loca(hits, query_direccion, query_name):
    if len(hits) > 0:
        for loca in hits:
            if (loca["_source"]["id_padre"] != None):
                print('Tiene loca padre')
                return [loca["_source"]["id_padre"]]
            
        data = list(
                map(
                    lambda x: { "id": x["_source"]["id"], "nombre": x["_source"]["nombre"], "direccion": x["_source"]["direccion"] },
                    hits
                )
            )
        combined_text = f"{query_direccion} {query_name}"
        results = []
        for d in data:
            result_text = f"{d['direccion']} {d['nombre']}"
            similarity = util.cos_sim(
                model.encode(combined_text),
                model.encode(result_text)
            ).item()
            
            if similarity >= 0.8:
                results.append({ "id": d["id"], "similarity": similarity })
        return results
    else:
        print('No se encontraron localizaciones')
        return []

direccion = 'AUTOPISTA MEDELLÍN KM 12 - VÍA SIBERIA - BOGOTÁ'
nombre = '1- GIOVANY PUENTES - 8014'
ciudad = 528
payload = get_query(direccion, nombre, ciudad)

headers = {
  'Authorization': 'ApiKey',
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)
json_response = response.json()
hits = json_response["hits"]["hits"]
print(json.dumps(hits))

final = get_loca(hits, direccion, nombre)
print("Final", final)

print("--- %s seconds ---" % (time.time() - start_time))