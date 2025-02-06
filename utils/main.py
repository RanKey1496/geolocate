import csv
from elasticsearch import Elasticsearch, helpers

# Configuración de Elasticsearch
es = Elasticsearch("",
                   api_key = '')

if es.ping():
    print("Conexión exitosa a Elasticsearch usando API Key.")
else:
    print("Error al conectar a Elasticsearch.")

# Nombre del índice
index_name = "localizaciones"

# Crear el índice con el mapeo necesario
def create_index():
    if not es.indices.exists(index=index_name):
        es.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "id_padre": {"type": "keyword"},
                        "ruta": {"type": "keyword"},
                        "ubicacion": {"type": "geo_point"},
                        "direccion": {"type": "text"},
                        "ciudad": {"type": "keyword"},
                        "nombre": {"type": "text"},
                        "activacion": {"type": "date"},
                        "modificacion": {"type": "date"},
                        "registro": {"type": "integer"},
                    }
                }
            }
        )
        print(f"Índice '{index_name}' creado con éxito.")
    else:
        print(f"Índice '{index_name}' ya existe.")

# Leer el archivo CSV e insertar los datos en Elasticsearch
def insert_data_from_csv(file_path):
    actions = []
    failed_actions = []
    with open(file_path, mode="r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        
        next(reader)
        for row in reader:
            print(row)
            
            if len(row) != 9:
                continue
            
            if (not row[2] and not row[3]) or (row[2] == '0' and row[3] == '0'):
                latitud = '0.0'
                longitud = '0.0'
            else:
                latitud = row[2] + '.' + row[3]
                longitud = row[4] + '.' + row[5]
            
            
            # Parsear los datos del CSV (ajusta según tu formato)
            #LOCA_ID_INT, LOCA_ID, LOCA_DIRECCION, LOCA_NOMBRE_CLIENTE, LOCA_CIUDAD = row[:5]
            LOCA_ID_INT, LOCA_ID = row[:2]
            LOCA_DIRECCION, LOCA_CIUDAD, LOCA_NOMBRE_CLIENTE = row[-3:]
            
            # Preparar el documento para Elasticsearch
            action = {
                "_index": index_name,
                "_id": LOCA_ID_INT,  # Usar el identificador único como ID del documento
                "_source": {
                    "id": LOCA_ID,
                    "id_padre": LOCA_ID_PADRE,
                    "ruta": LOCA_COD_RUTA,
                    "ubicacion": {
                        "lat": float(latitud),
                        "lon": float(longitud)
                    },
                    "direccion": LOCA_DIRECCION,
                    "ciudad": LOCA_CIUDAD,
                    "nombre": LOCA_NOMBRE_CLIENTE,
                    "activacion": LOCA_ACTIVACION,
                    "modificacion": LOCA_MODIFICACION,
                    "registro": LOCA_REGISTRO
                }
            }
            
            print(action)
            actions.append(action)

    try:
        print(f"Insertando {len(actions)} registros en Elasticsearch.")
        success, failed = helpers.bulk(es, actions, raise_on_error=False)
        
        if failed:
            failed_actions.extend(failed)
            
        print(f"{success}")
        print(f"Se han insertado {len(actions)} registros en Elasticsearch.")
    except BulkIndexError as e:
        print(f"Error al realizar la operación bulk: {e}")
        
    if failed_actions:
        print(f"Acciones fallidas: {failed_actions}")
        
    

# Script principal
if __name__ == "__main__":
    # Crea el índice (si no existe)
    create_index()

    # Ruta al archivo CSV
    csv_file_path = "./result3.csv"  # Cambia por la ruta real de tu archivo

    # Insertar datos
    insert_data_from_csv(csv_file_path)