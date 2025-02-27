import Levenshtein
from sentence_transformers import util

def porcentaje_coincidencia(principal, resultado):
    busqueda = set(resultado.split())
    
    coincidencias = busqueda.intersection(principal)
    porcentaje = len(coincidencias) / len(busqueda)
    
    return porcentaje

def porcentaje_coincidencia_levenstein(direccion, resultado):
    distancia = Levenshtein.distance(direccion, resultado)
    longitud_maxima = max(len(direccion), len(resultado))
    similitud = (1 - distancia / longitud_maxima)
    return similitud
    
def porcentaje_LLM(model, direccion, resultado):
    embedding_input = model.encode(direccion)
    embedding_result = model.encode(resultado)
    similitud = util.cos_sim(embedding_input, embedding_result).item()
    return similitud

def process_records(model, records, added_record, direccion, index, umbral = 0.7):
    results = []
    temp = added_record
    principal = set(direccion.split())
    
    for r in records:
        if r["id"] not in temp:
            coincidencia = porcentaje_coincidencia(principal, r['direccion'])
            if (coincidencia >= umbral):
                results.append({ "id": r["id"], "ref": index, "calculo": "sencillo", "direccion": r["direccion"], "score": coincidencia * 100 })
                temp[r["id"]] = True
                continue
            
            coincidencia_levenstein = porcentaje_coincidencia_levenstein(direccion, r['direccion'])
            if (coincidencia_levenstein >= umbral):
                results.append({ "id": r["id"], "ref": index, "calculo": "levenshtein", "direccion": r["direccion"], "score": coincidencia_levenstein * 100 })
                temp[r["id"]] = True
                continue
            
            coincidencia_llm = porcentaje_LLM(model, direccion, r['direccion'])
            if coincidencia_llm >= umbral:
                results.append({ "id": r["id"], "ref": index, "calculo": "llm", "direccion": r["direccion"], "score": coincidencia_llm * 100 })
                temp[r["id"]] = True
                continue
            
    return results, temp