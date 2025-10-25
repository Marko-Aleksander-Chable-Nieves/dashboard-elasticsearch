import csv
import os
from elasticsearch import Elasticsearch, helpers

# Credenciales que vienen de los secrets de GitHub Actions
CLOUD_ID = os.environ["ELASTIC_CLOUD_ID"]
API_KEY = os.environ["ELASTIC_API_KEY"]

# Conectar a Elasticsearch Cloud
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    api_key=API_KEY,
)

INDEX_NAME = "inventario"

# Creamos el índice con el mapeo correcto si no existe
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                "properties": {
                    "Fecha": {"type": "date"},            # formato tipo 2023-01-01
                    "Material": {"type": "keyword"},      # nombre del material
                    "StockInicial": {"type": "integer"},   # cantidad al inicio del día
                    "Entradas": {"type": "integer"},       # lo que entró
                    "Consumo": {"type": "integer"},        # lo que se usó / salió
                    "StockFinal": {"type": "integer"}      # existencias al final del día
                }
            }
        }
    )

docs = []
# OJO: tu CSV estaba en latin-1 cuando lo leímos, así que uso encoding="latin1"
with open("data/inventario.csv", newline="", encoding="latin1") as f:
    reader = csv.DictReader(f)

    for row in reader:
        docs.append({
            "_index": INDEX_NAME,
            "_source": {
                "Fecha": row["Fecha"],
                "Material": row["Material"],
                "StockInicial": int(row["StockInicial"]),
                "Entradas": int(row["Entradas"]),
                "Consumo": int(row["Consumo"]),
                "StockFinal": int(row["StockFinal"])
            }
        })

# Enviamos los documentos a Elasticsearch en bulk
if docs:
    helpers.bulk(es, docs)
    print(f"Se cargaron {len(docs)} filas al índice '{INDEX_NAME}'")
else:
    print("El CSV estaba vacío o no se leyó nada.")


