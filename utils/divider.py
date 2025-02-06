import pandas as pd
import os

def split_csv_by_size(file_path, max_size_mb=100, output_dir="output_chunks_2"):
    # Crear el directorio de salida si no existe
    os.makedirs(output_dir, exist_ok=True)

    # Leer el archivo CSV completo para estimar el tamaño de las filas
    df = pd.read_csv(file_path, dtype=str)
    total_rows = len(df)
    estimated_row_size = df.memory_usage(deep=True).sum() / total_rows  # Tamaño promedio de una fila en bytes
    rows_per_chunk = int((max_size_mb * 1024 * 1024) / estimated_row_size)  # Filas por chunk

    print(f"Filas totales: {total_rows}")
    print(f"Tamaño promedio por fila: {estimated_row_size:.2f} bytes")
    print(f"Filas por chunk: {rows_per_chunk}")

    # Leer el archivo en chunks y escribir cada parte en un archivo separado
    reader = pd.read_csv(file_path, dtype=str, chunksize=rows_per_chunk)
    for i, chunk in enumerate(reader):
        output_file = os.path.join(output_dir, f"part_{i + 1}.csv")
        chunk.to_csv(output_file, index=False)
        print(f"Parte {i + 1} guardada en '{output_file}' ({len(chunk)} filas)")

# Ejemplo de uso
if __name__ == "__main__":
    file_path = "output_chunks/part_1.csv"  # Ruta al archivo CSV grande
    split_csv_by_size(file_path, max_size_mb=100)