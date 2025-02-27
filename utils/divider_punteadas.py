import pandas as pd
import os

def split_csv_by_size(file_path, max_size_mb=100, output_dir=os.path.join("data", "output_chunks")):
    # Crear el directorio de salida si no existe
    os.makedirs(output_dir, exist_ok=True)

    # Leer el archivo CSV completo para estimar el tamaño de las filas
    df = pd.read_csv(file_path, dtype=str, sep=';',encoding='mbcs')
    
    total_rows = len(df)
    
    print(total_rows)
    
    estimated_row_size = df.memory_usage(deep=True).sum() / total_rows  # Tamaño promedio de una fila en bytes
    rows_per_chunk = int((max_size_mb * 1024 * 1024 * 0.2 * 0.05) / estimated_row_size)  # Filas por chunk

    print(f"Filas totales: {total_rows}")
    print(f"Tamaño promedio por fila: {estimated_row_size:.2f} bytes")
    print(f"Filas por chunk: {rows_per_chunk}")

    # Leer el archivo en chunks y escribir cada parte en un archivo separado
    reader = pd.read_csv(file_path, dtype=str, chunksize=rows_per_chunk, sep=';',encoding='mbcs')
    for i, chunk in enumerate(reader):
        output_file = os.path.join(output_dir, f"direcciones_{i + 1}.csv")
        header = ["AGRUPADOR", "DIRECCIONES"]
        chunk.to_csv(output_file, columns = header, index=False)
        print(f"Parte {i + 1} guardada en '{output_file}' ({len(chunk)} filas)")

def group_by_agrupador(file_pah, output_dir: str = os.path.join("data", "output_chunks")):
    os.makedirs(output_dir, exist_ok=True)
    
    df = pd.read_csv(file_pah, dtype=str, sep=';',encoding='mbcs')
    
    grouped = df.groupby("AGRUPADOR").first()
    
    output_file = os.path.join(output_dir, f"agrupados.csv")
    
    header = ["DIRECCIONES"]
    grouped.to_csv(output_file, index=False, columns = header)

# Ejemplo de uso
if __name__ == "__main__":
    file_path = "data/DIRECCIONES_PUNTEADAS.csv"
    #split_csv_by_size(file_path, max_size_mb=100)
    group_by_agrupador("data/DIRECCIONES_PUNTEADAS.csv")