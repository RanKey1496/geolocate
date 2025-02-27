import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans, DBSCAN
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import SnowballStemmer
import re
import nltk

nltk.download('punkt')

# Paso 1: Cargar el archivo CSV
def cargar_datos(archivo):
    return pd.read_csv(archivo)

# Paso 2: Preprocesamiento avanzado
def preprocesar_texto(texto):
    # Convertir a mayúsculas
    texto = texto.upper()
    
    # Eliminar caracteres especiales
    texto = re.sub(r'[^\w\s]', '', texto)
    
    # Tokenización
    tokens = word_tokenize(texto, language='spanish')
    
    # Eliminar stop words
    stop_words = set(stopwords.words('spanish'))
    tokens = [token for token in tokens if token not in stop_words]
    
    # Lematización
    stemmer = SnowballStemmer('spanish')
    tokens = [stemmer.stem(token) for token in tokens]
    
    # Unir tokens en una cadena
    return " ".join(tokens)

# Paso 3: Representación vectorial con TF-IDF
def vectorizar_direcciones(direcciones):
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(direcciones)
    return X, vectorizer

# Paso 4: Agrupamiento semántico
def agrupar_direcciones(X, n_clusters=10):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    clusters = kmeans.fit_predict(X)
    return clusters

# Paso 5: Validación con "AGRUPADOR"
def validar_con_agrupador(datos, clusters):
    datos['Cluster'] = clusters
    validaciones = {}
    for agrupador, grupo in datos.groupby('AGRUPADOR'):
        validaciones[agrupador] = grupo['Cluster'].unique().tolist()
    return validaciones

# Paso 6: Ejecutar el sistema
def main(archivo):
    # Cargar datos
    datos = cargar_datos(archivo)
    
    # Preprocesar direcciones
    datos['DIRECCIONES_PROCESADAS'] = datos['DIRECCIONES'].apply(preprocesar_texto)
    
    # Vectorizar direcciones
    X, vectorizer = vectorizar_direcciones(datos['DIRECCIONES_PROCESADAS'])
    
    # Agrupar direcciones
    clusters = agrupar_direcciones(X, n_clusters=20)
    
    # Validar con "AGRUPADOR"
    validaciones = validar_con_agrupador(datos, clusters)
    
    # Mostrar resultados
    print("Validaciones por Agrupador:")
    for agrupador, cluster_ids in validaciones.items():
        print(f"Agrupador: {agrupador}")
        print(f"Clusters: {cluster_ids}")
        print("-" * 50)

# Ejecutar el sistema
archivo = "data/output_chunks/direcciones_1.csv"
main(archivo)