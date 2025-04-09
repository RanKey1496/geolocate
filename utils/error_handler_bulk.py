import pandas as pd
import json

def read_json_file(file_path):
    with open(file_path, "r") as f:
        return json.load(f)
    
def read_csv_file(file_path):
    return pd.read_csv(file_path)

def get_data_from_csv_by_json(csv_file_path, json_file_path):
    json_data = read_json_file(json_file_path)    
    csv_data = read_csv_file(csv_file_path)
    
    data = list(map(lambda x: x["id"], json_data))
    print(data)
    
    filtered_data = csv_data[csv_data["LOCA_ID_INT"].isin(data)]
    
    return filtered_data

if __name__ == "__main__":
    csv_file_path = "./LOCA_QA_2018.csv"
    json_file_path = "./errores_indexacion.json"
    filtered_data = get_data_from_csv_by_json(csv_file_path, json_file_path)
    
    print(filtered_data)
    result_path = "./LOCA_QA_2018_errors.csv"
    filtered_data.to_csv(result_path, index=False)
    
    print(f"Datos filtrados guardados en '{result_path}'")