import pandas as pd

df = pd.read_csv("./luis2.csv", index_col=False, delimiter=',')

print(df.dtypes)
print(df.head())