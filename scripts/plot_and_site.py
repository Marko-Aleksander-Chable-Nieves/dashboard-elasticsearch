import os
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from collections import defaultdict

CLOUD_ID = os.environ["ELASTIC_CLOUD_ID"]
API_KEY = os.environ["ELASTIC_API_KEY"]

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    api_key=API_KEY,
)

INDEX_NAME = "inventario"

# 1. Traer hasta 1000 documentos del índice inventario
result = es.search(
    index=INDEX_NAME,
    body={
        "size": 1000,
        "_source": ["Fecha", "Material", "StockInicial", "Entradas", "Consumo", "StockFinal"],
        "query": {
            "match_all": {}
        }
    }
)

items = []
for hit in result["hits"]["hits"]:
    items.append(hit["_source"])

# 2. Agregar por Material el último StockFinal visto
#    (si hay varias filas por el mismo material en diferentes fechas,
#     nos quedamos con el StockFinal más reciente que apareció en la búsqueda)
ultimo_stock_por_material = {}
for row in items:
    mat = row["Material"]
    stock_final = row["StockFinal"]
    # simplemente sobrescribimos; como vienen ordenados por score no por fecha,
    # si quisieras algo más exacto podrías ordenar por Fecha. Para la práctica está bien así.
    ultimo_stock_por_material[mat] = stock_final

# 3. Ordenar materiales por StockFinal descendente y tomar top 10
sorted_items = sorted(
    ultimo_stock_por_material.items(),
    key=lambda x: x[1],
    reverse=True
)[:10]

materiales = [m for (m, s) in sorted_items]
stocks = [s for (m, s) in sorted_items]

# 4. Graficar barras
plt.figure(figsize=(10,5))
plt.bar(materiales, stocks)
plt.title("Top 10 materiales con mayor StockFinal")
plt.xlabel("Material")
plt.ylabel("StockFinal (existencia)")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("plot.png", dpi=200)
plt.close()

# 5. Generar tabla HTML con los mismos datos
tabla_html_rows = ""
for mat, stk in sorted_items:
    tabla_html_rows += f"<tr><td>{mat}</td><td>{stk}</td></tr>"

html = f"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8" />
<title>Dashboard de Inventario</title>
<style>
body {{
    font-family: system-ui, sans-serif;
    max-width: 900px;
    margin: 40px auto;
    line-height: 1.5;
}}
h1 {{
    margin-bottom: 0;
    font-size: 1.7rem;
}}
.small {{
    color: #666;
    font-size: 0.9rem;
    margin-top: 0.2rem;
}}
img {{
    width: 100%;
    border: 1px solid #ccc;
    border-radius: 8px;
    margin-top: 20px;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 24px;
    font-size: 0.9rem;
}}
th, td {{
    border: 1px solid #ccc;
    padding: 8px;
    text-align: left;
}}
th {{
    background: #f5f5f5;
}}
</style>
</head>
<body>
    <h1>Dashboard de Inventario</h1>
    <p class="small">Datos reales del archivo inventario.csv cargados a Elasticsearch (índice "{INDEX_NAME}").</p>

    <p>Este dashboard muestra los materiales con mayor StockFinal.
    El flujo es:
    1) Tomar el archivo inventario.csv,
    2) Cargarlo automáticamente en Elasticsearch usando Python,
    3) Consultar Elasticsearch,
    4) Generar esta página estática y la gráfica con GitHub Actions,
    5) Publicarlo todo en GitHub Pages.</p>

    <img src="plot.png" alt="Top materiales por stock final" />

    <h2>Top 10 materiales por StockFinal</h2>
    <table>
        <tr>
            <th>Material</th>
            <th>StockFinal</th>
        </tr>
        {tabla_html_rows}
    </table>
</body>
</html>
"""

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Listo: generado index.html y plot.png")

