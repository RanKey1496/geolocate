from sklearn.feature_extraction.text import TfidfVectorizer
from keybert import KeyBERT
import numpy as np

# Modelo KeyBERT para extracción de palabras clave
kw_model = KeyBERT()

def extract_keywords(text: str, corpus: list) -> list:
    # TF-IDF para filtrar palabras irrelevantes
    vectorizer = TfidfVectorizer(stop_words='english')
    X = vectorizer.fit_transform(corpus + [text])
    feature_names = np.array(vectorizer.get_feature_names_out())
    tfidf_scores = np.ravel(X[-1].toarray())
    
    # Seleccionar palabras más relevantes según TF-IDF
    top_tfidf = feature_names[np.argsort(tfidf_scores)[-5:]].tolist()
    
    # Usar KeyBERT para refinar las palabras clave
    keywords = kw_model.extract_keywords(text, keyphrase_ngram_range=(1,2), stop_words='english', top_n=5)
    keybert_keywords = [kw[0] for kw in keywords]
    
    return list(set(top_tfidf + keybert_keywords))

# Prueba con un texto de ejemplo
sample_corpus = ["ZONA FRANCA INTEXZONA BODEGA 11 A", "ZN INDUSTRIAL CANAVITA COMPLEJO NAVCAR KM 22 AUTONORTE"]
sample_text = "ZONA FRANCA INTEXZONA BODEGA 11 A"
keywords = extract_keywords(sample_text, sample_corpus)
print("Palabras clave extraídas:", keywords)
