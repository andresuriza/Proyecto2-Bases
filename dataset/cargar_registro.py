"""Carga los JSONL generados por generar_registro.py en MongoDB (db `plataforma`).

Idempotente: por cada colección hace drop de los documentos con los mismos _id (reemplazo
por upsert en bloques), de modo que ejecutarlo dos veces deja el mismo estado. Con --limpiar
vacía las colecciones antes de insertar (estado inicial exacto: todas las tareas 'disponible').
Escribe con w=majority (MONGO_W). Los índices los crea registro al arrancar.
Es una herramienta de carga permitida por el enunciado (§11.1 dataset/), no el simulador.

Dentro de la red:
  docker compose --profile tools run --rm herramientas python dataset/cargar_registro.py --dir dataset/out --limpiar
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient, ReplaceOne, WriteConcern

COLECCIONES = ["clientes", "proyectos", "batches", "tareas", "anotadores"]
CAMPOS_FECHA = {"creado_en", "cerrado_en", "creada_en", "actualizada_en"}


def convertir(d):
    for k in CAMPOS_FECHA & d.keys():
        if isinstance(d[k], str):
            d[k] = datetime.strptime(d[k], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="dataset/out")
    ap.add_argument("--uri", default=os.environ.get(
        "MONGO_URI", "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"))
    ap.add_argument("--w", default=os.environ.get("MONGO_W", "majority"))
    ap.add_argument("--limpiar", action="store_true", help="vaciar colecciones antes de cargar")
    ap.add_argument("--bloque", type=int, default=2000)
    a = ap.parse_args()
    w = int(a.w) if a.w.isdigit() else a.w
    db = MongoClient(a.uri, serverSelectionTimeoutMS=10000).get_database(
        "plataforma", write_concern=WriteConcern(w=w, wtimeout=30000))
    for nombre in COLECCIONES:
        ruta = os.path.join(a.dir, f"{nombre}.jsonl")
        if not os.path.exists(ruta):
            sys.exit(f"falta {ruta}: ejecute generar_registro.py primero")
        col = db[nombre]
        if a.limpiar:
            col.delete_many({})
        ops, n = [], 0
        with open(ruta, encoding="utf8") as fh:
            for linea in fh:
                doc = convertir(json.loads(linea))
                ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
                if len(ops) >= a.bloque:
                    col.bulk_write(ops, ordered=False)
                    n += len(ops)
                    ops = []
        if ops:
            col.bulk_write(ops, ordered=False)
            n += len(ops)
        print(f"{nombre}: {n} documentos cargados")
    # los índices obligatorios los crea el servicio registro al arrancar (con nombre propio)
    print("conteos finales:", {c: db[c].count_documents({}) for c in COLECCIONES})
    print("tareas por estado:", {x["_id"]: x["n"] for x in db.tareas.aggregate(
        [{"$group": {"_id": "$estado", "n": {"$sum": 1}}}])})


if __name__ == "__main__":
    main()
