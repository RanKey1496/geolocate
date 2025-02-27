import re

def parse_address(address):
    # Patrones para identificar elementos
    via_pattern = r'\b(CL|CR|AV|TV|CA|AUT|KM)\b[\s\dC]*'
    number_pattern = r'\b\d+[\s-]*\d*\b'
    building_pattern = r'\b(BG|ED|MZ|LC|AP|CONJ|TO|P)\b[\s\w]*'
    neighborhood_pattern = r'\b(BRR|CENTRO|SAN|DEL|EL|LA)\b[\s\w]*'
    
    # Extraer información
    via = re.search(via_pattern, address)
    number = re.search(number_pattern, address)
    building = re.search(building_pattern, address)
    neighborhood = re.search(neighborhood_pattern, address)
    
    # Formatear resultados
    result = {
        'Via': via.group(0) if via else None,
        'Numero': number.group(0) if number else None,
        'Edificio': building.group(0) if building else None,
        'Barrio/Sitio': neighborhood.group(0) if neighborhood else None
    }
    
    return result

# Ejemplo de prueba
direccion = "PARQUE INDUSTRIAL CELTA TRADE PARK UNO KILOMETRO 7 AUTOPISTA MEDELLIN - BODEGA NR 131 - 16"
print(parse_address(direccion))
