import csv
import os
from elasticsearch import Elasticsearch, helpers

# Credenciales que vienen de los secretos de GitHub Actions
CLOUD_ID = os.environ["ELASTIC_CLOUD_ID"]
API_KEY = os.environ["ELASTIC_API_KEY"]

# Conectar a Elasticsearch
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    api_key=API_KEY,
)

INDEX_NAME = "inventario"

# Creamos el índice con un mapeo básico si no existe
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                "properties": {
                    "producto": {"type": "keyword"},
                    "categoria": {"type": "keyword"},
                    "stock": {"type": "integer"},
                    "precio_unitario": {"type": "float"}
                }
            }
        }
    )

docs = []
with open("data/inventario.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        docs.append({
            "_index": INDEX_NAME,
            "_source": {
                "producto": row["producto"],
                "categoria": row["categoria"],
                "stock": int(row["stock"]),
                "precio_unitario": float(row["precio_unitario"])
            }
        })

if docs:
    helpers.bulk(es, docs)
    print(f"Se cargaron {len(docs)} filas al índice '{INDEX_NAME}'")
else:
    print("El CSV estaba vacío o no se leyó nada.")

