import json
from transformers import pipeline

ner_pipeline = pipeline("ner", model="dccuchile/bert-base-spanish-wwm-cased", tokenizer="dccuchile/bert-base-spanish-wwm-cased")
text = "CR 7 13 95 ALM 24 HORAS DEL DIA"
entities = ner_pipeline(text)
print(entities)