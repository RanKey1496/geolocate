import requests
import json
import g4f
import Levenshtein
import time

start_time = time.time()


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

def generate_nombre_prompt(nombre, direccion, hits):
    prompt = f"""
    Necesito que compares los campos de direccion y nombre que te proporciono para ver que tan similar son con los datos.
    Debes retornar solo el ratio de similitud con respecto al id analizado en caso de tu resultado en un json.
    Aquí un ejemplo de lo que retornarás:
    "id": "1", "ratio": "0.8"
    NO DEBES ESCRIBIR NADA MAS QUE EL VALOR RETORNADO
    NO DEBES INCLUIR NINGUNA MARCA O FORMATO EN EL TEXTO
    SOLO RETORNA EL CONTENIDO CRUDO DE TU RESPUESTA. NO INCLUYAS "VOICEOVER", "NARRATOR" o INDIRACORES SIMILARES DE QUE DEBERIA DECIRSE AL INICIO DE LA RESPUESA.
    DEBES RETORNAR POR TODOS LOS DATOS UN OBJECTO, SIN IMPORTAR SU RESULTADO, AL FINAL LA RESPUESTA SIEMPRE SERÁ UN ARRAY JSON
    
    Nombre: {nombre}
    Direccion: {direccion}
    
    Datos a comparar:
    
    """
    
    for hit in hits:
        prompt += f"""
        Id: {hit["id"]} - Nombre: {hit["nombre"]} - Direccion: {hit["direccion"]}
        """
    print(prompt)
    
    return prompt

def generate_llm_response(prompt: str) -> str:
    return g4f.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {
                'role': 'user',
                'content': prompt
            }
        ]
    )

def get_loca(hits, query_direccion, query_name):
    if len(hits) > 0:
        for loca in hits:
            if (loca["_source"]["id_padre"] != None):
                print('Tiene loca padre')
                return loca["_source"]["id_padre"]
            else:
                print('No tiene loca padre')
                ratio = Levenshtein.ratio(query_name, loca["_source"]["nombre"])
                print(f"Ratio: {ratio}")
                if (ratio > 0.7):
                    return loca["_source"]["id"]

        prompt = generate_nombre_prompt(query_name, query_direccion,
            list(
                map(
                    lambda x: { "id": x["_source"]["id"], "nombre": x["_source"]["nombre"], "direccion": x["_source"]["direccion"] },
                    hits
                )
            )
        )
        llm_response = generate_llm_response(prompt)
        print("Response", llm_response)
        if (len(llm_response) > 0):
            max_ratio = max(llm_response, key=lambda x: x["ratio"])
            return max_ratio["id"]
        return None
            
    else:
        print('No se encontraron localizaciones')
        return None

direccion = 'CL 80 7 00 IN D CELTA TRADE PARK BD 4 LT 41 COSTADO SUR PARQUE'
nombre = 'AGRIFOL SAS'
ciudad = 170
payload = get_query(direccion, nombre, ciudad)

headers = {
  'Authorization': 'ApiKey',
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)
print(response.json()["hits"]["hits"])

json_response = response.json()
hits = json_response["hits"]["hits"]

final = get_loca(hits, direccion, nombre)
print("Final", final)

print("--- %s seconds ---" % (time.time() - start_time))