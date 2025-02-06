import os
import time
import pandas as pd
import json
from elasticsearch import Elasticsearch, helpers, TransportError, ApiError
from elasticsearch.helpers import BulkIndexError
from multiprocessing import Pool, cpu_count

index_name = "localizaciones"

def get_es_client():
    return Elasticsearch("http://:9200/",
                        max_retries=10,
                        retry_on_timeout=True,
                        api_key = '')
    
def create_index(index_name):
    es = get_es_client()
    if not es.indices.exists(index=index_name):
        es.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "id_int_padre": {"type": "text"},
                        "id_padre": {"type": "keyword"},
                        "ruta": {"type": "text"},
                        "lat": {"type": "float"},
                        "lon": {"type": "float"},
                        "direccion": {"type": "text"},
                        "ciudad": {"type": "keyword"},
                        "nombre": {"type": "text"},
                        "activacion": {
                            "type": "date",
                            "format": "dd/MM/yy||epoch_millis"
                        },
                        "modificacion": {
                            "type": "date",
                            "format": "dd/MM/yy||epoch_millis"
                        },
                        "registro": {"type": "integer"},
                    }
                }
            }
        )
        print(f"Índice '{index_name}' creado con éxito.")
    else:
        print(f"Índice '{index_name}' ya existe.")
    
def process_chunk(index_name, chunk):
    actions = []
    
    for idx, row in chunk.iterrows():
        loca_id_int = str(row["LOCA_ID_INT"])
        loca_id = str(row["LOCA_ID"])
        loca_id_int_padre = (str(int(row["LOCA_ID_INT_PADRE"])) if pd.notnull(row["LOCA_ID_INT_PADRE"]) else None)
        loca_id_padre = str(row["LOCA_ID_PADRE"]) if pd.notnull(row["LOCA_ID_PADRE"]) else None
        loca_cod_ruta = str(row["LOCA_COD_RUTA"]) if pd.notnull(row["LOCA_COD_RUTA"]) else None
        
        lat = (
            float(str(row["LOCA_LATITUD"]))
            if pd.notnull(row["LOCA_LATITUD"])
            else 0
        )
        lon = (
            float(str(row["LOCA_LONGITUD"]))
            if pd.notnull(row["LOCA_LONGITUD"])
            else 0
        )
        
        loca_ciudad = int(row["LOCA_CIUDAD"]) if pd.notnull(row["LOCA_CIUDAD"]) else None
        
        activacion = str(row["LOCA_ACTIVACION"]) if pd.notnull(row["LOCA_ACTIVACION"]) else None
        modificacion = str(row["LOCA_MODIFICACION"]) if pd.notnull(row["LOCA_MODIFICACION"]) else None
        
        loca_id_registro = int(row["LOCA_ID_REGISTRO"]) if pd.notnull(row["LOCA_ID_REGISTRO"]) else 0
        
        document = {
                "id": loca_id,
                "id_int_padre": loca_id_int_padre,
                "id_padre": loca_id_padre,
                "ruta": loca_cod_ruta,
                "lat": lat,
                "lon": lon,
                "direccion": str(row["LOCA_DIRECCION"]),
                "ciudad": loca_ciudad,
                "nombre": str(row["LOCA_NOMBRE_CLIENTE"]),
                "activacion": activacion,
                "modificacion": modificacion,
                "registro": loca_id_registro
            }
        
        action = {
            "_index": index_name,
            "_id": loca_id_int,
            "_source": document
        }
        
        actions.append(action)
    return actions

def disable_index_refresh(es, index_name):
    print(f"Desactivando la actualización del índice '{index_name}'...")
    es.indices.put_settings(index=index_name, body={"refresh_interval": "-1"})
    print("Actualización del índice desactivada.")
    
def enable_index_refresh(es, index_name):
    print(f"Reactivando la actualización del índice '{index_name}'...")
    es.indices.put_settings(index=index_name, body={"refresh_interval": "1s"})
    print("Actualización del índice reactivada.")

def save_errors_to_file(errors, file_path="errores_indexacion.json"):
    if not errors:
        return 

    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            existing_errors = json.load(f)
        errors.extend(existing_errors)

    with open(file_path, "w") as f:
        json.dump(errors, f, indent=4)
    print(f"Se han registrado {len(errors)} errores en '{file_path}'.")
    
def process_csv_and_index(file_path, index_name, chunk_size=100000, max_retries=3):
    es = get_es_client()  # Obtener la conexión a Elasticsearch
    all_errors = []  # Lista para almacenar todos los errores

    try:
        # Desactivar la actualización del índice para optimizar la indexación
        disable_index_refresh(es, index_name)

        # Leer el archivo CSV en chunks
        reader = pd.read_json(file_path, chunksize=chunk_size, dtype=str)
        chunk_number = 0

        for chunk in reader:
            chunk_number += 1
            print(f"\nProcesando chunk {chunk_number}...")

            # Procesar el chunk y obtener las acciones bulk
            actions = process_chunk(index_name, chunk)
            total_documents = len(actions)
            print(f"Chunk {chunk_number}: {total_documents} documentos para procesar.")
            
            # Procesar los documentos válidos con reintentos
            remaining_actions = actions  # Documentos pendientes por procesar
            attempt = 0

            while remaining_actions and attempt < max_retries:
                attempt += 1
                print(f"Chunk {chunk_number}: Intento {attempt} con {len(remaining_actions)} documentos pendientes...")

                processed_ok = []
                processed_errors = []

                try:
                    # Usar streaming_bulk para procesar los documentos
                    for ok, result in helpers.streaming_bulk(es, remaining_actions):
                        if ok:
                            processed_ok.append(result['index']['_id'])
                        else:
                            failed_id = result['index']['_id']
                            failed_error = result['index']['error']
                            processed_errors.append({
                                "chunk": chunk_number,
                                "id": failed_id,
                                "error": failed_error
                            })
                            print(f"Chunk {chunk_number}: Documento fallido: ID={failed_id}, Error={failed_error}")

                except ApiError as e:
                    print(f"3 Chunk {chunk_number}: Error de Elasticsearch durante el intento {attempt}: {e}")
                    processed_errors.extend([{
                        "chunk": chunk_number,
                        "id": action["_id"],
                        "error": f"Error de Elasticsearch: {str(e)}"
                    } for action in remaining_actions])
                except BulkIndexError as e:
                    # Capturar errores específicos de Elasticsearch
                    print(f"2 Chunk {chunk_number}: Error de Elasticsearch durante el intento {attempt}: {e}")
                    processed_errors.extend([{
                        "chunk": chunk_number,
                        "id": action["_id"],
                        "error": f"Error de Elasticsearch: {str(e)}"
                    } for action in remaining_actions])

                except Exception as e:
                    # Capturar otros errores generales
                    print(f"1 Chunk {chunk_number}: Error crítico durante el intento {attempt}: {e.message}")
                    processed_errors.extend([{
                        "chunk": chunk_number,
                        "id": action["_id"],
                        "error": f"Error crítico: {str(e)}"
                    } for action in remaining_actions])

                finally:
                    # Actualizar la lista de documentos pendientes
                    remaining_actions = [
                        action for action in remaining_actions if action["_id"] not in processed_ok
                    ]
                    all_errors.extend(processed_errors)

                    # Mostrar estadísticas del intento
                    print(f"Chunk {chunk_number}: Intento {attempt}: OK={len(processed_ok)}, Fallidos={len(processed_errors)}")

            # Registrar los documentos que no se pudieron procesar después de todos los intentos
            for action in remaining_actions:
                all_errors.append({
                    "chunk": chunk_number,
                    "id": action["_id"],
                    "error": "Fallo después de todos los intentos"
                })

            # Mostrar estadísticas finales del chunk
            print(f"Chunk {chunk_number}: Finalizado. Procesados OK={total_documents - len(remaining_actions)}, Fallidos={len(remaining_actions)}")

    finally:
        # Reactivar la actualización del índice al finalizar
        enable_index_refresh(es, index_name)

    # Guardar todos los errores al finalizar
    save_errors_to_file(all_errors, "errores_indexacion.json")
    print("\nProceso completado.")

if __name__ == "__main__":
    file_path = './result_loca_2021.json'
    create_index(index_name)
    process_csv_and_index(file_path, index_name, chunk_size=100000)