from fastapi import FastAPI, HTTPException
from sentence_transformers import SentenceTransformer, util
from result_request import ResultRequest
from elastic import get_hits, generate_query, generate_similarity_address, get_address_hits, convert_hits_to_list, generate_similarity_with_slop_address, get_address_hits_by_slop, get_address_hits_by_percent
from config import get_elastic_url, get_elastic_key
from llm import generate_address_references
from geo import process_records
import json

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
    
    url = f"{get_elastic_url()}/_search"
    key = get_elastic_key()
    data = generate_query(request.direccion, request.name, request.ciudad)
    hits = get_hits(url, key, data)
    
    if (len(hits) == 0):
        print("No se encontraron localizaciones")
        return []
    
    data = list(
                map(
                    lambda x: { "id": x["_source"]["id"], "id_padre": x["_source"]["id_padre"], "nombre": x["_source"]["nombre"], "direccion": x["_source"]["direccion"], "elastic_score": x["_score"] },
                    hits
                )
            )
    combined_text = f"{request.direccion} {request.name}"
    results = []
    
    for d in data:
        result_text = f"{d['direccion']} {d['nombre']}"
        
        embedding_input = model.encode(combined_text)
        embedding_result = model.encode(result_text)
        similarity = util.cos_sim(embedding_input, embedding_result).item()
        score = similarity * 100
        
        if d["id_padre"] is not None:
            score *= 1.5
        
        mix_score = (score + d["elastic_score"])/2
        
        results.append({ "id": d["id"], "id_padre": d["id_padre"], "direccion": d["direccion"], "nombre": d["nombre"], "score": score, "elastic_score": d["elastic_score"], "mix_score": mix_score })
        
    results.sort(key=lambda x: x["mix_score"], reverse=True)
    return results

@app.post("/similarityByPercent")
async def get_result_by_percent(request: ResultRequest):
    if model is None:
        return {"error": "Modelo no cargado. Por favor, reinicia la aplicación."}
    
    url = f"{get_elastic_url()}/_search"
    key = get_elastic_key()
    
    percent = 80
    minimum_percent = 50
    added_record = {}
    results = []
    
    while (percent > minimum_percent):
        print("Getting similarity by percent: ", percent)
        data = generate_similarity_address(request.direccion, request.ciudad, percent)
        hits = get_address_hits(url, key, data)
        
        records = convert_hits_to_list(hits)
        
        for r in records:
            if r["id"] not in added_record:
                embedding_input = model.encode(request.direccion)
                embedding_result = model.encode(r['direccion'])
                similarity = util.cos_sim(embedding_input, embedding_result).item()
                print(f"Input: {request.direccion}, Result: {r['direccion']}, Similarity: {similarity}")
                
                if similarity >= 0.4:
                    results.append({ "id": r["id"], "direccion": r["direccion"], "score": similarity * 100 })
                    added_record[r["id"]] = True
            else:
                print(f"Ya se incluyo el id {r['id']}")
        percent -= 5
        
    return results

@app.post("/address")
async def get_results_by_address(request: ResultRequest):
    if model is None:
        return {"error": "Modelo no cargado. Por favor, reinicia la aplicación."}
    
    url = f"{get_elastic_url()}/_search"
    key = get_elastic_key()
    
    data = generate_similarity_address(request.direccion, request.ciudad)
    hits = get_address_hits(url, key, data)
    
    print(len(hits))
    
    data = convert_hits_to_list(hits)
    
    results = []
    
    for d in data:       
        embedding_input = model.encode(request.direccion)
        embedding_result = model.encode(d['direccion'])
        similarity = util.cos_sim(embedding_input, embedding_result).item()
        score = similarity * 100 #Convertimos a porcentaje
        
        mix_score = (score + d["elastic_score"])/2
        
        results.append({ "id": d["id"], "direccion": d["direccion"], "score": score, "elastic_score": d["elastic_score"], "mix_score": mix_score })
        
    results.sort(key=lambda x: x["mix_score"], reverse=True)
    return results

@app.post("/similarityByReference")
async def get_results_by_reference(request: ResultRequest):
    if model is None:
        raise HTTPException(status_code=400, detail="Modelo no cargado. Por favor, reinicia la aplicación.")
    
    url = f"{get_elastic_url()}/"
    key = get_elastic_key()
    
    references = [request.direccion]
    references.extend(generate_address_references(request.direccion))
    
    added_record = {}
    results = []
    slop = 4
    percent = 70
    
    for index, ref in enumerate(references):
        print("Getting similarity by slop: ", slop, " of reference: ", ref)
        data = generate_similarity_with_slop_address(ref, request.ciudad, slop)
        hits = get_address_hits_by_slop(url, key, data)
        records = convert_hits_to_list(hits)
        
        result, duplicated = process_records(model, records, added_record, request.direccion, index, 0.8)
        added_record.update(duplicated)
        results.extend(result)
        
        print("Getting similarity percent: ", percent, " of reference: ", ref)
        data = generate_similarity_address(ref, request.ciudad, percent)
        hits = get_address_hits_by_percent(url, key, data)
        records = convert_hits_to_list(hits)
        
        result, duplicated = process_records(model, records, added_record, request.direccion, index, 0.8)
        added_record.update(duplicated)
        results.extend(result)
            
    return { "referencias": references, "total": len(results), "results": results }

@app.post("/similarityByExactAddress")
async def get_results_by_exact_address(request: ResultRequest):
    if model is None:
        raise HTTPException(status_code=400, detail="Modelo no cargado. Por favor, reinicia la aplicación.")
    
    url = f"{get_elastic_url()}/_search"
    key = get_elastic_key()
    
    data = generate_similarity_address(request.direccion, request.ciudad)
    hits = get_address_hits_by_exact_address(url, key, data)
    
    results = convert_hits_to_list(hits)
    
    return results