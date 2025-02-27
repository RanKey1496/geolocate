import json
import requests

def convert_hits_to_list(hits):
    data = list(
                map(
                    lambda x: { "id": x["_source"]["id"], "direccion": x["_source"]["direccion"], "elastic_score": x["_score"] },
                    hits
                )
            )
    return data

def generate_query(direccion, nombre, ciudad):
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
            },
            {
                "filter": {
                    "bool": {
                        "must": [
                            { "exists": { "field": "lat" } },
                            { "exists": { "field": "lon" } },
                            {
                                "script": {
                                    "script": "doc['lat'].value != 0 && doc['lon'].value != 0"
                                }
                            }
                        ]
                    }
                },
                "weight": 2
            },
            {
                "script_score": {
                    "script": {
                    "source": "double score = _score; return score / params.maxScore;",
                    "params": { "maxScore": 10 }
                    }
                }
            }
        ],
        "score_mode": "multiply",
        "boost_mode": "multiply"
        }
    },
    "sort": [
        { "_score": { "order": "desc" } },
        { "id_padre": { "order": "desc", "missing": "_last" } },
        { "registro": { "order": "desc" } },
        { "lat": { "order": "desc" } },
        { "modificacion": { "order": "desc" } }
    ]
    }
    return json.dumps(data)

def generate_similarity_with_slop_address(address, city, slop = 2):
    data = {
        "query": {
            "function_score": {
            "query": {
                "bool": {
                "must": [
                    {
                        "term": {
                            "ciudad": {
                                "value": city
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "direccion": {
                                "query": address,
                                "slop": slop
                            }
                        }
                    }
                ],
                "filter": [
                    { "term": { "ciudad": city } }
                ]
                }
            },
            "functions": [],
            "score_mode": "multiply",
            "boost_mode": "multiply"
            }
        }
    }
    
    return json.dumps(data)

def generate_similarity_address(address, city, percent = 60):
    data = {
        "query": {
            "function_score": {
            "query": {
                "bool": {
                "must": [
                    {
                        "term": {
                            "ciudad": {
                                "value": city
                            }
                        }
                    },
                    {
                        "match": {
                            "direccion": {
                                "query": address,
                                "minimum_should_match": f"{percent}%"
                            }
                        }
                    }
                ],
                "filter": [
                    { "term": { "ciudad": city } }
                ]
                }
            },
            "score_mode": "multiply",
            "boost_mode": "multiply"
            }
        }
    }
    
    return json.dumps(data)
  
def get_hits(url, key, data):
    url = f"{url}?size=5"
    
    headers = {
      'Authorization': f'ApiKey {key}',
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=data)
    json_response = response.json()
    
    hits = json_response["hits"]["hits"]
    return hits

def get_address_hits(url, key, data):
    url = f"{url}"
    
    headers = {
      'Authorization': f'ApiKey {key}',
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=data)
    json_response = response.json()
    hits = json_response["hits"]["hits"]
    return hits

def get_address_hits_by_slop(url, key, data):
    headers = {
      'Authorization': f'ApiKey {key}',
      'Content-Type': 'application/json'
    }
    
    count_url = f"{url}/_count"
    try_number = 0
    while try_number < 3:
        print("Intento de obtener address por slop ", try_number)
        try:
            
            response_count = requests.request("POST", count_url, headers=headers, data=data)
            json_response_count = response_count.json()
            size = json_response_count["count"]
            
            if size > 1000:
                size = 1000
                
            print("Size: ",size)
            search_url = f"{url}/_search?size={size}"
            response_search = requests.request("POST", search_url, headers=headers, data=data)
            json_response_search = response_search.json()    
            hits = json_response_search["hits"]["hits"]
            return hits
        except:
            try_number += 1
            
    return []

def get_address_hits_by_percent(url, key, data):
    headers = {
      'Authorization': f'ApiKey {key}',
      'Content-Type': 'application/json'
    }
    
    count_url = f"{url}/_count"
    
    try_number = 0
    while try_number < 3:
        print("Intento de obtener address por porcentaje ", try_number)
        try:
            response_count = requests.request("POST", count_url, headers=headers, data=data)
            json_response_count = response_count.json()
            size = json_response_count["count"]
            
            if size > 1000:
                size = 1000
                
            print("Size: ",size)
            search_url = f"{url}/_search?size={size}"
            response_search = requests.request("POST", search_url, headers=headers, data=data)
            json_response_search = response_search.json()    
            hits = json_response_search["hits"]["hits"]
            return hits
        except:
            try_number += 1
            
    return []

def get_address_hits_by_exact(url, key, data):
    return None