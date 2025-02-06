import pandas as pd

df = pd.read_csv("./result_loca_2020.csv", index_col=False, nrows=10)

print(df.dtypes)
print(df.head())