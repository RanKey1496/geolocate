import tensorflow_hub as hub
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# Cargar el modelo USE
model_url = "https://tfhub.dev/google/universal-sentence-encoder/4"
embed = hub.load(model_url)

# Textos a comparar

text1 = "CENCOSUD COLOMBIA SA/EASY AMERICAS"
text2 = "CENCOSUD COLOMBIA S.A. // EASY AMERICAS"

# Obtener embeddings para los textos
embeddings = embed([text1, text2])

# Calcular similitud coseno
similarity_score = cosine_similarity([embeddings[0].numpy()], [embeddings[1].numpy()])[0][0]

print(f"Similitud con USE: {similarity_score:.2f}")