import spacy

nlp = spacy.load("es_core_news_lg")

text = "km 15 vía Medellín Bogotá, parque Versalles bodega 3"

doc = nlp(text)

for ent in doc.ents:
    print(ent.text, ent.label_)