from fastapi import FastAPI
from sentence_transformers import SentenceTransformer, util
from result_request import ResultRequest
from elastic import get_hits, generate_query
from config import get_elastic_url, get_elastic_key

app = FastAPI()
model = None

@app.on_event("startup")
async def startup_event():
    global model
    print("Cargando modelo...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    print("Modelo cargado")
    

@app.post("/results")
async def get_results(request: ResultRequest):
    if model is None:
        return {"error": "Modelo no cargado. Por favor, reinicia la aplicación."}
    
    url = get_elastic_url()
    key = get_elastic_key()
    data = generate_query(request.direccion, request.name, request.ciudad)
    hits = get_hits(url, key, data)
    
    if (len(hits) == 0):
        print("No se encontraron localizaciones")
        return []
    
    for hit in hits:
        if (hit["_source"]["id_padre"] != None):
            print('Tiene loca padre')
            return [{ "id": hit["_source"]["id_padre"], "direccion": hit["_source"]["direccion"], "nombre": hit["_source"]["nombre"], "similarity": "N/A" }]
        
    data = list(
                map(
                    lambda x: { "id": x["_source"]["id"], "nombre": x["_source"]["nombre"], "direccion": x["_source"]["direccion"] },
                    hits
                )
            )
    combined_text = f"{request.direccion} {request.name}"
    results = []
    
    for d in data:
        result_text = f"{d['direccion']} {d['nombre']}"
        print(result_text)
        similarity = util.cos_sim(
            model.encode(combined_text),
            model.encode(result_text)
        ).item()
        print(similarity)
            
        if similarity >= 0.7:
            results.append({ "id": d["id"], "direccion": d["direccion"], "nombre": d["nombre"], "similarity": similarity })
    return results