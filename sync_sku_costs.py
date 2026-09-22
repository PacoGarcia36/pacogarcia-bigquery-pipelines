"""
Completa vtex_data.sku_cost_snapshot con los SKUs activos que no tienen costo.

La tabla es append-only: cada corrida agrega una foto nueva con la fecha del dia,
nunca pisa lo anterior. Lee el costo de la API de Pricing de VTEX.

Uso:
    python load_sku_costs.py                # solo los SKUs activos con stock sin costo
    python load_sku_costs.py --todos        # refresca TODOS los SKUs activos con stock
    python load_sku_costs.py --limite 500   # corta despues de N (para probar)

Guarda en BigQuery cada 500 registros, asi una interrupcion no pierde el avance.
No imprime credenciales.
"""
import argparse
import datetime
import os
import sys
import time

from dotenv import load_dotenv

RAIZ = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(RAIZ, ".env"))

import requests
from google.cloud import bigquery
from google.oauth2 import service_account

PROYECTO = "e-coomerce-484513"
TABLA = f"{PROYECTO}.vtex_data.sku_cost_snapshot"
LOTE_BQ = 500
PAUSA = 0.2


def cliente_bq():
    sa = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", "")
    if sa and not os.path.isabs(sa):
        sa = os.path.join(RAIZ, sa)
    return bigquery.Client(
        credentials=service_account.Credentials.from_service_account_file(
            sa, scopes=["https://www.googleapis.com/auth/bigquery"]),
        project=PROYECTO)


def skus_objetivo(bq, todos):
    filtro = "" if todos else "AND c.sku_id IS NULL"
    sql = f"""
    WITH ult AS (SELECT MAX(snapshot_date) d FROM `{PROYECTO}.vtex_data.stock_snapshot`),
    sk AS (SELECT s.sku_id, SUM(s.available_quantity) stock
           FROM `{PROYECTO}.vtex_data.stock_snapshot` s, ult
           WHERE s.snapshot_date = ult.d AND s.warehouse_id='1_1' AND s.is_active
           GROUP BY s.sku_id HAVING SUM(s.available_quantity) > 0),
    c AS (SELECT DISTINCT sku_id FROM `{PROYECTO}.vtex_data.sku_cost_snapshot`)
    SELECT sk.sku_id FROM sk LEFT JOIN c USING(sku_id) WHERE TRUE {filtro}
    ORDER BY sk.stock DESC
    """
    return [r["sku_id"] for r in bq.query(sql).result()]


def guardar(bq, filas):
    if not filas:
        return
    cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("sku_id", "STRING"),
            bigquery.SchemaField("list_price", "NUMERIC"),
            bigquery.SchemaField("cost_price", "NUMERIC"),
            bigquery.SchemaField("markup", "NUMERIC"),
            bigquery.SchemaField("base_price", "NUMERIC"),
            bigquery.SchemaField("snapshot_date", "DATE"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP"),
        ])
    bq.load_table_from_json(filas, TABLA, job_config=cfg).result()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--todos", action="store_true", help="Refresca todos, no solo los faltantes")
    ap.add_argument("--limite", type=int, default=0, help="Cortar despues de N SKUs")
    args = ap.parse_args()

    cuenta = os.environ["VTEX_ACCOUNT"]
    cab = {"X-VTEX-API-AppKey": os.environ["VTEX_APP_KEY"],
           "X-VTEX-API-AppToken": os.environ["VTEX_APP_TOKEN"],
           "Accept": "application/json"}

    bq = cliente_bq()
    print("Buscando SKUs a cargar...", flush=True)
    skus = skus_objetivo(bq, args.todos)
    if args.limite:
        skus = skus[:args.limite]
    print(f"  {len(skus)} SKUs objetivo\n", flush=True)
    if not skus:
        print("Nada para hacer.")
        return 0

    hoy = datetime.date.today().isoformat()
    ahora = datetime.datetime.now(datetime.timezone.utc).isoformat()
    buffer, total, sin_costo, errores = [], 0, 0, 0
    t0 = time.time()
    ses = requests.Session()
    ses.headers.update(cab)

    for i, sku in enumerate(skus, 1):
        url = f"https://api.vtex.com/{cuenta}/pricing/prices/{sku}"
        datos = None
        for intento in range(3):
            try:
                r = ses.get(url, timeout=25)
                if r.status_code == 429:
                    time.sleep(3 * (intento + 1))
                    continue
                if r.status_code == 404:
                    break
                if r.ok:
                    datos = r.json()
                break
            except Exception:
                if intento == 2:
                    errores += 1
                else:
                    time.sleep(2 ** intento)

        if datos and datos.get("costPrice") is not None:
            def num(v):
                # NUMERIC de BigQuery admite 9 decimales; la API devuelve mas en markup
                return None if v is None else round(float(v), 6)
            buffer.append({
                "sku_id": str(sku),
                "list_price": num(datos.get("listPrice")),
                "cost_price": num(datos.get("costPrice")),
                "markup": num(datos.get("markup")),
                "base_price": num(datos.get("basePrice")),
                "snapshot_date": hoy,
                "loaded_at": ahora,
            })
            total += 1
        else:
            sin_costo += 1

        if len(buffer) >= LOTE_BQ:
            guardar(bq, buffer)
            buffer = []
            vel = i / max(time.time() - t0, 1)
            resta = (len(skus) - i) / max(vel, 0.01) / 60
            print(f"  {i:>5}/{len(skus)}  cargados {total:>5}  sin costo {sin_costo:>4}  "
                  f"errores {errores:>3}  |  faltan ~{resta:.0f} min", flush=True)

        time.sleep(PAUSA)

    guardar(bq, buffer)
    print(f"\n--- Listo en {(time.time()-t0)/60:.1f} min ---", flush=True)
    print(f"  SKUs con costo cargado : {total}")
    print(f"  Sin costo en VTEX      : {sin_costo}")
    print(f"  Errores de API         : {errores}")
    print(f"  Foto guardada con fecha: {hoy}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
