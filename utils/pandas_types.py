# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 22:32:52 2024

@author: Jhon
"""

import pandas as pd

# Ruta al archivo CSV
file_path = "./result_loca_2023.csv"

df = pd.read_csv(file_path, nrows=10,
    na_values=[""],
    delimiter=",",
    index_col=False)
print(df.dtypes)

def combine_lat(row):
    return ".".join([str(row['LOCA_LATITUD']), str(row['LOCA_LONGITUD'])])

def combine_lon(row):
    return ".".join([str(row['LOCA_DIRECCION']), str(row['LOCA_CIUDAD'])])

#df['latitud'] = str(df["LOCA_LATITUD"]).replace(",", ".")
#df['longitud'] = str(df["LOCA_LONGITUD"]).replace(",", ".")
print(df.head())
df['latitud'] = df.apply(combine_lat, axis=1)
df['longitud'] = df.apply(combine_lon, axis=1)
df['LOCA_DIRECCION'] = df['LOCA_NOMBRE_CLIENTE']
df['LOCA_CIUDAD'] = df['LOCA_ACTIVACION']
df['LOCA_NOMBRE_CLIENTE'] = df['LOCA_MODIFICACION']
df['LOCA_ACTIVACION'] = df['LOCA_ID_REGISTRO']

print("\n\n")
print(df.head())