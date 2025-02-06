from elasticsearch import Elasticsearch

# Conectar a Elasticsearch usando la API Key
es = Elasticsearch("",
                   api_key = '=')

# Nombre del índice
index_name = "localizaciones"

# Eliminar todos los documentos del índice
response = es.delete_by_query(index=index_name, body={
    "query": {
        "match_all": {}
    }
})

print(f"Respuesta de eliminación: {response}")