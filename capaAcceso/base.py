import os
from sodapy import Socrata
import pandas as pd
import requests

DOMAIN = "www.datos.gov.co"
DATASET_ID = "jbjy-vk9h"  # SECOP II – Contratos Electrónicos

APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None)
client = Socrata(DOMAIN, APP_TOKEN, timeout=60)

def get_primeros_5_registros(client, DATASET_ID):
    # Traer 5 registros (usa SoQL: $limit)
    rows = client.get(DATASET_ID, limit=5)

    # Convertir a DataFrame
    df = pd.DataFrame.from_records(rows)

    print(f"Filas recibidas: {len(df)}")
    print("Columnas:", list(df.columns)[:10], "…")
    print(df.head())

def test_url_accessibility():    
    url = "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.NTC.6087250&isFromPublicArea=True&isModal=true&asPopupView=true"
    resp = requests.head(url, allow_redirects=True, timeout=20)
    print(resp.status_code, resp.headers.get("Content-Type"))

def test_filtros_y_documentos(client, DATASET_ID):
    # SoQL: $select, $where, $order, $limit
    select_cols = [
        "nombre_entidad",
        "proceso_de_compra",
        "id_contrato",
        "estado_contrato",
        "valor_del_contrato",
        "fecha_de_firma"
    ]

    where_expr = "valor_del_contrato > 100000000"  # > 100 millones
    order_expr = "valor_del_contrato DESC"

    rows = client.get(
        DATASET_ID,
        select=",".join(select_cols),
        where=where_expr,
        order=order_expr,
        limit=10
    )

    df = pd.DataFrame.from_records(rows)
    print(df.head())

def main():
    get_primeros_5_registros(client, DATASET_ID)
    test_url_accessibility()
    test_filtros_y_documentos(client, DATASET_ID)
    
if __name__ == "__main__":    
    main()