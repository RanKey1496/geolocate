import pandas as pd

df = pd.read_csv("./PRL LOCA.csv", index_col=False, delimiter=';')

print(df.dtypes)
print(df.head())

mask = df['LOCA_NOMBRE_CLIENTE'].str.contains(r"[^a-zA-Z0-9. &_/()*\-#]", regex=True, na=False)
filtered_df = df[mask]

mask_2 = df['LOCA_DIRECCION'].str.contains(r"[^a-zA-Z0-9. &_/()*\-#]", regex=True, na=False)
filtered_df_2 = df[mask_2]

print(f'Cantidad total: {df.shape[0]} - Cantidad filtrada: {filtered_df.shape[0]} - Cantidad direcciones: {filtered_df_2.shape[0]}')


#filtered_df.to_csv("./filtered_char.csv")

df_clean = df[~mask]
df_clean = df_clean[~mask_2]

print(df_clean.head())

df_clean.to_csv("./PRL LOCA clean.csv", index=False)