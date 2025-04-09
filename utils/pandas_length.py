# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 22:32:52 2024

@author: Jhon
"""

import pandas as pd

# Ruta al archivo CSV
file_path = "./LOCA_QA_2020.csv"

# Contador de registros
num_registros = 0

# Leer el archivo en chunks
for chunk in pd.read_csv(file_path, chunksize=10000):  # Tamaño del chunk ajustable
    num_registros += len(chunk)

print(f"El archivo contiene {num_registros} registros.")