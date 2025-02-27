import g4f
from fastapi import HTTPException
import json

def generate_llm_response(prompt: str) -> str:
    return g4f.ChatCompletion.create(
        model = "gpt-4",
        messages=[
            {
                'role': 'user',
                'content': prompt
            }
        ]
    )

def divide_address(address):
    prompt = f"""
    Dada la dirección que te envíe, genera un array de strings con las referencias más importantes, priorizando zonas industriales, veredas o ubicaciones rurales fuera de la ciudad.

    - Extrae únicamente los elementos clave que ayuden a ubicar el destino.
    - No incluyas números de bodega, oficina, manzana ni kilómetros, a menos que sean esenciales.
    - Mantén nombres de parques industriales, zonas francas, veredas, sectores y corredores viales principales.
    - Si la dirección contiene una vía, combínala con otro elemento significativo (ej. una vereda, sector o zona franca). No retornes una vía sola como referencia.
    - Genera al menos tres combinaciones de referencia, priorizando los elementos más relevantes.
    - No agregues palabras que no estén en la dirección original.
    - No incluyas marcas, formato especial ni explicaciones en la respuesta. Solo devuelve el array con las referencias en español.
    - No agregues ninguna referencia al tipo de la respuesta, ni digas que json, ni array ni nada de eso.
    
    Ejemplo 1:
    Entrada: "OFI PRINCIPAL PQUE INDUSTRIAL SAN JOSE BODEGA A5 - KILOMETRO 3.5 VIA FUNZA SIBERIA MANZANA A BODEGA 5"
    Salida: "[{{ "referencia": "PARQUE INDUSTRIAL SAN JOSE" }}, {{ "referencia": "PARQUE INDUSTRIAL SAN JOSE VIA FUNZA SIBERIA" }}]"
    
    Ejemplo 2:
    Entrada: "BODEGA 24 ZN FRANCA TOCANCIPÁ, KILÓMETRO 1.5 VÍA BRICEÑO – ZIPAQUIRÁ, VEREDA VERGANZO, SECTOR TIBITOC, TOCANCIPÁ FABIO"
    Salida: ["ZONA FRANCA TOCANCIPÁ VEREDA VERGANZO", ""ZN FRANCA TOCANCIPÁ VEREDA VERGANZO"", "ZONA FRANCA TOCANCIPA KILOMETRO 1 5 VIA BRICEÑO ZIPAQUIRÁ", "VEREDA VERGANZO SECTOR TIBITOC VIA BRICEÑO ZIPAQUIRÁ"]
    
    Ejemplo 3:
    Entrada: "PC INDUSTRIAL ZOL OPERACIONES LOGISTIC BG 38Y39 ZN DE KM VIA FUNZHE COSTADO ORIENTAL VIA FUNZA - COTA"
    Salida: ["PARQUE INDUSTRIAL ZOL", "PARQUE INDUSTRIAL ZOL VIA FUNZHE", "PARQUE INDUSTRIAL ZOL VIA FUNZA"]
    
    Dirección: {address}
    """
    
    try_number = 0
    while try_number < 3:
        print("Intentando generar la respuesta de LLM: ", try_number + 1)
        try:
            completion = generate_llm_response(prompt)
        
            if not completion:
                try_number += 1
                continue
            
            if len(completion) == 0:
                try_number += 1
                continue
            return completion
        except Exception as e:
            try_number += 1
    raise Exception('No se pudo generar el texto luego de 3 intentos')

def generate_address_references(address):
    try:
        response = divide_address(address)
        return json.loads(response)
    except Exception as e:
        print(e)
        return json.loads('[]')