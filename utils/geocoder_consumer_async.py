import pandas as pd
from zeep import Client
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurar el cliente SOAP con una sesión optimizada
from zeep.transports import Transport
from requests import Session

wsdl_url = ""

session = Session()
session.verify = False  # Deshabilita SSL si es necesario
transport = Transport(session=session)
client = Client(wsdl=wsdl_url, transport=transport)

# Leer el archivo CSV
df = pd.read_csv('./luis2.csv', index_col=False)

# Función para procesar una fila individual
def georeferenciar_row(row):
    direccion_data = {
        "address": row['LOCA_DIRECCION'],
        "city": row['CIUD_CODIGO'],
        "name": row['LOCA_NOMBRE_CLIENTE']
    }
    
    try:
        response = client.service.georeferenciar(direccion=direccion_data)
        return {
            "index": row.name,
            "message": response['message'],
            "direcciontcc": response['direcciontcc'],
            "fuente": response['data']['fuente'],
            "dirtrad": response['data']['dirtrad'],
            "latitude": response['data']['latitude'],
            "longitude": response['data']['longitude'],
            "zonapostal": response['data']['zonapostal'],
            "estado": response['data']['estado'],
            "tokenizedAddress": response['data']['tokenizedAddress'],
            "locaIdInt": response['data']['locaIdInt']
        }
    except Exception as e:
        print('Error')
        return {"index": row.name, "message": f"Error: {str(e)}"}

# Número de hilos a usar (depende de tu CPU y el servicio)
MAX_THREADS = 10  

# Procesamiento en paralelo
results = []
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    futures = {executor.submit(georeferenciar_row, row): row for _, row in df.iterrows()}
    
    for future in as_completed(futures):
        results.append(future.result())

# Convertir resultados a DataFrame
for res in results:
    index = res["index"]
    for key in res:
        if key != "index":
            df.loc[index, key] = res[key]

# Guardar los resultados en un nuevo CSV
df.to_csv('./luis_result.csv', index=False)

print("✅ Proceso completado con paralelización.")
