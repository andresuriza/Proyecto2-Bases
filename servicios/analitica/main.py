"""Servicio analítica: FastAPI + SparkSession caliente sobre el datalake Parquet (sólo lectura).

Rutas (<= 6): GET /q/{q1..q5}?conjunto=principal|grande|validacion&explain=0|1, GET /salud.
Este servicio NO importa pymongo ni llama a registro/historial. Sólo habla con Valkey
para publicar mensajes de salud (salud:analitica y salud:spark), como exige el enunciado.
Compatible con Python 3.8 (imagen apache/spark:3.5.3), por eso no usa `common/`.
"""
import json
import logging
import os
import socket
import threading
import time
import urllib.request
import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request

import consultas

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s analitica %(message)s")
log = logging.getLogger("analitica")

CONJUNTOS = {
    "principal": os.environ.get("DATALAKE_DIR", "/datalake"),
    "grande": os.environ.get("DATALAKE_GRANDE_DIR", "/datalake_grande"),
    "validacion": os.environ.get("DATALAKE_VALIDACION_DIR", "/datalake_validacion"),
}
VALKEY_URL = os.environ.get("VALKEY_URL", "redis://valkey:6379/0")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_UI = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080")

estado = {"spark": None, "error": None, "ejecuciones": 0, "ultima": None, "creada_en": None}
_lock_sesion = threading.Lock()


def obtener_spark():
    """Sesión caliente (singleton). Se crea al arrancar; si falló, se reintenta aquí."""
    with _lock_sesion:
        if estado["spark"] is None:
            t0 = time.perf_counter()
            estado["spark"] = consultas.crear_spark(SPARK_MASTER, "analitica")
            estado["creada_en"] = round(time.perf_counter() - t0, 2)
            estado["error"] = None
            if os.environ.get("SPARK_WARMUP", "0") == "1":
                estado["spark"].range(100).count()
            log.info("SparkSession lista en %.1f s (master=%s)", estado["creada_en"], SPARK_MASTER)
        return estado["spark"]


def estado_spark_cluster():
    """(estado, detalle) del clúster Spark consultando la API JSON del master."""
    if SPARK_MASTER.startswith("local"):
        return ("ok" if estado["spark"] else "degradado"), f"modo local ({SPARK_MASTER})"
    try:
        with urllib.request.urlopen(SPARK_UI + "/json/", timeout=2) as r:
            j = json.loads(r.read())
        vivos = [w for w in j.get("workers", []) if w.get("state") == "ALIVE"]
        if j.get("status") != "ALIVE":
            return "caido", f"master {j.get('status')}"
        if not vivos:
            return "degradado", "master vivo sin workers"
        return "ok", f"{len(vivos)} worker(es) vivos, {sum(w['cores'] for w in vivos)} cores"
    except Exception as e:
        return "caido", f"master inaccesible: {type(e).__name__}"


def estado_analitica():
    if estado["error"]:
        return "degradado", f"SparkSession: {estado['error']}"
    if estado["spark"] is None:
        return "degradado", "SparkSession iniciando"
    return "ok", f"ejecuciones={estado['ejecuciones']}"


def latidos():
    """Hilo: publica salud:analitica y salud:spark en Valkey cada 5 s (EX 15). Nunca lanza."""
    import redis
    r = None
    while True:
        try:
            if r is None:
                r = redis.Redis.from_url(VALKEY_URL, socket_timeout=3, socket_connect_timeout=3)
            ts = int(time.time() * 1000)
            for capa, (est, det) in (("analitica", estado_analitica()), ("spark", estado_spark_cluster())):
                r.set("salud:" + capa, json.dumps({"estado": est, "detalle": det, "ts": ts}), ex=15)
        except Exception as e:
            log.warning("latido falló: %s", e)
            r = None
        time.sleep(5)


def arrancar_spark():
    try:
        obtener_spark()
    except Exception as e:
        estado["error"] = f"{type(e).__name__}: {e}"
        log.error("no se pudo crear SparkSession: %s", e)


app = FastAPI(title="analitica")


@app.on_event("startup")
def _inicio():
    threading.Thread(target=arrancar_spark, daemon=True).start()
    threading.Thread(target=latidos, daemon=True).start()


@app.middleware("http")
async def request_id(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex
    resp = await call_next(request)
    resp.headers["x-request-id"] = rid
    return resp


@app.get("/salud")
def salud():
    est, det = estado_analitica()
    est_sp, det_sp = estado_spark_cluster()
    if est == "ok" and est_sp != "ok":
        est = "degradado"
    return {"estado": est, "detalle": det, "spark": {"estado": est_sp, "detalle": det_sp},
            "master": SPARK_MASTER, "host": socket.gethostname(),
            "creada_en_s": estado["creada_en"], "ejecuciones": estado["ejecuciones"]}


@app.get("/evento/{event_id}")
def evento(event_id: str, conjunto: str = Query("principal")):
    """Ruta de comprobación (P4): ¿ya es visible este event_id para Spark? (3.ª ruta de 6 permitidas)."""
    if conjunto not in CONJUNTOS:
        raise HTTPException(400, f"conjunto inválido; use {sorted(CONJUNTOS)}")
    ruta = os.path.join(CONJUNTOS[conjunto], "eventos")
    try:
        return consultas.buscar_evento(obtener_spark(), ruta, event_id)
    except Exception as e:
        raise HTTPException(503, f"fallo de Spark: {type(e).__name__}: {str(e)[:300]}")


@app.get("/q/{q}")
def consulta(q: str, conjunto: str = Query("principal"), explain: int = Query(0, ge=0, le=1),
             contar: int = Query(0, ge=0, le=1)):
    q = q.lower()
    if q not in consultas.CONSULTAS:
        raise HTTPException(404, f"consulta desconocida {q}; use q1..q5")
    if conjunto not in CONJUNTOS:
        raise HTTPException(400, f"conjunto inválido; use {sorted(CONJUNTOS)}")
    ruta = os.path.join(CONJUNTOS[conjunto], "eventos")
    if not os.path.isdir(ruta) or not any(d.startswith("mes=") for d in os.listdir(ruta)):
        raise HTTPException(404, f"conjunto '{conjunto}' sin datos en {ruta}")
    try:
        spark = obtener_spark()
    except Exception as e:
        raise HTTPException(503, f"Spark no disponible: {type(e).__name__}: {e}")
    try:
        r = consultas.ejecutar(spark, q, ruta, explain=bool(explain), contar=bool(contar))
    except Exception as e:
        log.exception("fallo ejecutando %s", q)
        raise HTTPException(503, f"fallo de Spark: {type(e).__name__}: {str(e)[:500]}")
    estado["ejecuciones"] += 1
    estado["ultima"] = {"q": q, "conjunto": conjunto, "tiempo_s": r["tiempo_s"]}
    out = {"consulta": q, "conjunto": conjunto, "filas": r["filas"], "n_filas": r["n_filas"],
           "tiempo_s": r["tiempo_s"], "n_ejecucion": estado["ejecuciones"]}
    for k in ("eventos_leidos", "plan"):
        if k in r:
            out[k] = r[k]
    return out
