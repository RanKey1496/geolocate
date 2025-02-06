import pandas as pd
from multiprocessing import Pool, cpu_count
import os

# Función para procesar una línea del archivo
def process_line(line):
    # Dividir la línea en columnas
    parts = line.strip().split(',')
    
    # Reemplazar comas por puntos en las columnas de latitud y longitud
    if (parts[5] == '0' and parts[6] == '0'):
        lat = None
        lon = None
        del parts[2]
        del parts[3]
        del parts[4]
    elif (parts[5] == '0' and parts[6].isdigit() and parts[7] == '0' and parts[8].isdigit()):
        print('Prueba 1: ', parts)
        lat = ".".join([parts[5], parts[6]])
        lat = ".".join([parts[7], parts[8]])
        print('Prueba 2: ', parts)
    elif (parts[5] != '0' and parts[7] != '0'):
        for (i, item) in enumerate(parts, start=1):
            print(i, item)
        print('Prueba 1: ', parts)
        lat = ".".join([parts[5], parts[6]])
        lon = ".".join([parts[7], parts[8]])
        del parts[6]
        del parts[7]
        del parts[8]
        del parts[9]
        print('Prueba 2: ', parts)
        print('Lat lon: ', lat, lon)
    else:
        lat = None
        lon = None
    
    parts.append(lat)
    parts.append(lon)
    
    print(parts)
    
    # Devolver la línea procesada
    return parts

# Función para procesar un chunk del archivo
def process_chunk(chunk):
    # Procesar cada línea en el chunk
    processed_lines = [process_line(line) for line in chunk]
    
    # Convertir las líneas procesadas en un DataFrame
    df = pd.DataFrame(processed_lines, columns=[
        "LOCA_ID_INT", "LOCA_ID", "LOCA_ID_INT_PADRE", "LOCA_ID_PADRE", "LOCA_COD_RUTA",
        "LOCA_DIRECCION", "LOCA_CIUDAD", "LOCA_NOMBRE_CLIENTE",
        "LOCA_ACTIVACION", "LOCA_MODIFICACION", "LOCA_ID_REGISTRO",
        "LOCA_LATITUD", "LOCA_LONGITUD"
    ])
    
    return df

# Función para dividir el archivo en chunks y procesarlos en paralelo
def process_file(file_path, output_path, chunksize=10000):
    # Leer el archivo línea por línea
    with open(file_path, 'r', encoding="utf8") as file:
        # Leer la cabecera
        header = file.readline().strip().split(',')
        
        # Leer el resto del archivo en chunks
        chunks = []
        chunk = []
        for i, line in enumerate(file):
            chunk.append(line)
            if len(chunk) >= chunksize:
                chunks.append(chunk)
                chunk = []
        if chunk:  # Añadir el último chunk si no está vacío
            print(f"Chunks: {len(chunks)}")
            chunks.append(chunk)
    
    # Crear un pool de procesos
    #with Pool(processes=cpu_count()) as pool:
        # Procesar cada chunk en paralelo
    processed_chunks = process_chunk(chunk)
    
    # Combinar todos los chunks procesados en un solo DataFrame
    df = pd.concat(processed_chunks)
    
    # Guardar el resultado en un nuevo archivo CSV
    df.to_csv(output_path, index=False)
    print(f"Archivo procesado guardado en: {output_path}")

# Ruta del archivo CSV de entrada y salida
input_file = 'result_loca_2023.csv'
output_file = 'archivo_procesado.csv'

# Procesar el archivo
process_file(input_file, output_file)