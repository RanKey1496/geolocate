import pandas as pd
import requests
import json

filepath = 'LOCA_EVALUACION_20250122_2120 1.xlsx'

df = pd.read_excel(filepath)

print(df.head())

def make_request(nombre, direccion, ciudad):
    url = 'http://localhost:8000/results'
    data = {
        "name": nombre,
        "direccion": direccion,
        "ciudad": ciudad
    }
    
    response = requests.post(url, json=data)
    return response.json()

"""
for index, row in df.iterrows():
    print("Index: ", index)
    try:
        response = make_request(row['LOCA_NOMBRE_CLIENTE_REM'], row['LOCA_DIRECCION_ORI'], row['LOCA_CIUDAD_ORI'])
    
        if (len(response) > 0):
            if (response[0]["id_padre"]):
                df.at[index, "ELASTIC_LOCA_ID"] = response[0]["id_padre"]
                df.at[index, "ELASTIC_SCORE"] = response[0]["score"]
                df.at[index, "ELASTIC_MIX_SCORE"] = response[0]["mix_score"]
            else:
                df.at[index, "ELASTIC_LOCA_ID"] = response[0]["id"]
                df.at[index, "ELASTIC_SCORE"] = response[0]["score"]
                df.at[index, "ELASTIC_MIX_SCORE"] = response[0]["mix_score"]
    except Exception as e:
        print("Ocurrió un error: ", e)
"""
            
#df.to_excel('result_loca_evaluacion.xlsx', index=False)

result = df[df["LOCA_ID_REM"] == 'REM000456762667']

print(result.head())

for index, row in result.iterrows():
    print(index)
    print(row['LOCA_NOMBRE_CLIENTE_REM'], row['LOCA_DIRECCION_ORI'], row['LOCA_CIUDAD_ORI'])
    response = make_request(row['LOCA_NOMBRE_CLIENTE_REM'], row['LOCA_DIRECCION_ORI'], row['LOCA_CIUDAD_ORI'])
    print(json.dumps(response))
    break