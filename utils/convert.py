import pandas as pd

with open('./result_loca_2021.json', encoding='utf-8') as inputfile:
    df = pd.read_json(inputfile)

df.to_csv('./result_loca_2021.csv', encoding='utf-8', index=False)