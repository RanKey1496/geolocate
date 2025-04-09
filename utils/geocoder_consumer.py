import pandas as pd
from zeep import Client

wsdl_url = ""
client = Client(wsdl=wsdl_url)

df = pd.read_csv('./luis2.csv', index_col=False)

for index, row in df.iterrows():
    direccion_data = {
        "address": row['LOCA_DIRECCION'],
        "city": row['CIUD_CODIGO'],
        "name": row['LOCA_NOMBRE_CLIENTE']
    }

    try:
        response = client.service.georeferenciar(direccion=direccion_data)
        df.loc[index, 'message'] = response['message']
        df.loc[index, 'direcciontcc'] = response['direcciontcc']
        df.loc[index, 'fuente'] = response['data']['fuente']
        df.loc[index, 'dirtrad'] = response['data']['dirtrad']
        df.loc[index, 'latitude'] = response['data']['latitude']
        df.loc[index, 'longitude'] = response['data']['longitude']
        df.loc[index, 'zonapostal'] = response['data']['zonapostal']
        df.loc[index, 'estado'] = response['data']['estado']
        df.loc[index, 'tokenizedAddress'] = response['data']['tokenizedAddress']
        df.loc[index, 'locaIdInt'] = response['data']['locaIdInt']
    except Exception as e:
        print(f'Error {index}: {str(e)}')
    

df.to_csv('./luis_result.csv', index=False)