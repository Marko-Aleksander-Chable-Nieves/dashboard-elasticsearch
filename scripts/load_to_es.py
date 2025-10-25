import csv
import os
from elasticsearch import Elasticsearch, helpers

# Tomamos los valores desde los secrets de GitHub Actions
ENDPOINT = os.environ["ELASTIC_ENDPOINT"]
API_KEY = os.environ["ELASTIC_API_KEY"]

# Conectar a Elasticsearch usando el endpoint directo
es = Elasticsearch(
    ENDPOINT,
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
                    "Fecha": {"type": "date"},            # Ej: 2024-10-25
                    "Material": {"type": "keyword"},      # Nombre / código del material
                    "StockInicial": {"type": "integer"},  # Stock al inicio
                    "Entradas": {"type": "integer"},      # Lo que entró
                    "Consumo": {"type": "integer"},       # Lo que salió
                    "StockFinal": {"type": "integer"}     # Stock final del día
                }
            }
        }
    )

docs = []
# Usamos latin1 porque tu CSV venía así
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

if docs:
    helpers.bulk(es, docs)
    print(f"Se cargaron {len(docs)} filas al índice '{INDEX_NAME}'")
else:
    print("El CSV estaba vacío o no se leyó nada.")
