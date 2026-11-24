"""Servicio historial: consumidor Valkey Stream -> micro-lotes -> datalake Parquet.

Rutas (<= 6): GET /salud, GET /estado, POST /compactar?mes=, POST /reprocesar.
Garantías: at-least-once (XACK sólo después de publicar el archivo); la publicación
es idempotente por nombre de archivo y Spark deduplica por event_id.
"""
import asyncio
import json
import logging
import os
import socket
import time
from collections import deque
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query

from common.config import DATALAKE_DIR, VALKEY_URL, env_int
from common.heartbeat import bucle_latidos
from common.logs import RequestIDMiddleware, setup_logging

from . import lago

log = setup_logging("historial")
STREAM = os.environ.get("STREAM", "eventos")
GRUPO = os.environ.get("GRUPO", "historial")
CONSUMIDOR = os.environ.get("CONSUMIDOR", "historial-1")  # fijo: permite recuperar pendientes
LOTE_N = env_int("LOTE_N", 500)
LOTE_T_S = float(os.environ.get("LOTE_T_S", "2"))
RECLAMAR_MS = env_int("RECLAMAR_MS", 30000)

stats = {
    "inicio": time.time(), "lotes": 0, "eventos_escritos": 0, "archivos_escritos": 0,
    "invalidos": 0, "ultimo_error": None, "ultimo_error_ts": 0.0, "recuperados": 0,
    "ultimos_lotes": deque(maxlen=20),
}
valkey: aioredis.Redis | None = None
lock_compactar = asyncio.Lock()


def _registrar_error(e: Exception) -> None:
    stats["ultimo_error"] = f"{type(e).__name__}: {e}"
    stats["ultimo_error_ts"] = time.time()


def _procesar_sync(entradas: list) -> dict:
    """Bloqueante (se ejecuta en un hilo): valida, guarda replay y publica los Parquet.
    entradas = [(stream_id, json_str)]. Devuelve resumen; los inválidos se descartan."""
    filas, invalidos = [], 0
    for sid, js in entradas:
        try:
            filas.append(lago.validar(json.loads(js)))
        except Exception as e:  # evento corrupto: no debe bloquear el consumidor
            invalidos += 1
            log.error("evento inválido %s descartado: %s", sid, e)
    batch_id = lago.id_lote(entradas[0][0], entradas[-1][0])
    rutas = []
    if filas:
        lago.guardar_replay(DATALAKE_DIR, batch_id, entradas)
        rutas = lago.publicar_lote(DATALAKE_DIR, batch_id, filas)
    return {"batch_id": batch_id, "eventos": len(filas), "invalidos": invalidos,
            "archivos": [os.path.relpath(r, DATALAKE_DIR) for r in rutas]}


def _extraer(resp) -> list:
    """XREADGROUP/XAUTOCLAIM -> [(id, json_str)]"""
    out = []
    for _stream, msgs in resp or []:
        for sid, campos in msgs:
            if not campos:  # entrada borrada/trimmeada
                out.append((sid, None))
                continue
            out.append((sid, campos.get("evento")))
    return out


async def publicar_y_ack(entradas: list) -> None:
    if not entradas:
        return
    validas = [(s, j) for s, j in entradas if j is not None]
    ids = [s for s, _ in entradas]
    t0 = time.perf_counter()
    resumen = {"batch_id": None, "eventos": 0, "invalidos": 0, "archivos": []}
    if validas:
        resumen = await asyncio.to_thread(_procesar_sync, validas)
    await valkey.xack(STREAM, GRUPO, *ids)  # sólo tras publicar
    stats["lotes"] += 1
    stats["eventos_escritos"] += resumen["eventos"]
    stats["archivos_escritos"] += len(resumen["archivos"])
    stats["invalidos"] += resumen["invalidos"]
    resumen.update(ms=round((time.perf_counter() - t0) * 1000, 1), ts=time.time())
    stats["ultimos_lotes"].append(resumen)
    log.info("lote %s: %d eventos, %d archivos, %.0f ms", resumen["batch_id"], resumen["eventos"],
             len(resumen["archivos"]), resumen["ms"])


async def asegurar_grupo() -> None:
    try:
        await valkey.xgroup_create(STREAM, GRUPO, id="0", mkstream=True)
        log.info("grupo %s creado en %s", GRUPO, STREAM)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def recuperar_pendientes() -> None:
    """Recuperación tras caída: (1) entradas ya entregadas a este consumidor y sin ACK
    (leer con id '0'); (2) entradas de otros consumidores inactivas > RECLAMAR_MS (XAUTOCLAIM)."""
    while True:
        resp = await valkey.xreadgroup(GRUPO, CONSUMIDOR, {STREAM: "0"}, count=LOTE_N)
        entradas = _extraer(resp)
        if not entradas:
            break
        log.warning("recuperando %d pendientes propios", len(entradas))
        await publicar_y_ack(entradas)
        stats["recuperados"] += len(entradas)
    cursor = "0-0"
    while True:
        res = await valkey.xautoclaim(STREAM, GRUPO, CONSUMIDOR, min_idle_time=RECLAMAR_MS,
                                      start_id=cursor, count=LOTE_N)
        cursor, msgs = res[0], res[1]
        entradas = [(sid, (c or {}).get("evento")) for sid, c in msgs]
        if entradas:
            log.warning("reclamadas %d entradas de otros consumidores", len(entradas))
            await publicar_y_ack(entradas)
            stats["recuperados"] += len(entradas)
        if cursor in ("0-0", b"0-0") or not msgs:
            break


async def consumidor() -> None:
    """Bucle principal: micro-lote por tamaño (LOTE_N) o tiempo (LOTE_T_S)."""
    buf: list = []
    inicio_lote = None
    iniciado = False
    while True:
        try:
            if not iniciado:
                await asegurar_grupo()
                await recuperar_pendientes()
                iniciado = True
            if buf:
                resto_ms = max(1, int((inicio_lote + LOTE_T_S - time.monotonic()) * 1000))
            else:
                resto_ms = 1000
            if len(buf) < LOTE_N and resto_ms > 0:
                resp = await valkey.xreadgroup(GRUPO, CONSUMIDOR, {STREAM: ">"},
                                               count=LOTE_N - len(buf), block=resto_ms)
                nuevos = _extraer(resp)
                if nuevos and not buf:
                    inicio_lote = time.monotonic()
                buf.extend(nuevos)
            if buf and (len(buf) >= LOTE_N or time.monotonic() - inicio_lote >= LOTE_T_S):
                await publicar_y_ack(buf)
                buf = []
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _registrar_error(e)
            log.error("consumidor: %s; reintento en 2 s (buf=%d sin ACK, se conserva)", e, len(buf))
            if "NOGROUP" in str(e):
                iniciado = False
            await asyncio.sleep(2)
            if buf:
                inicio_lote = time.monotonic() - LOTE_T_S  # reintentar publicar enseguida


async def sonda() -> tuple:
    if time.time() - stats["ultimo_error_ts"] < 15:
        return "degradado", f"error reciente: {stats['ultimo_error']}"
    return "ok", f"lotes={stats['lotes']} eventos={stats['eventos_escritos']}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    global valkey
    valkey = aioredis.from_url(VALKEY_URL, decode_responses=True)
    os.makedirs(os.path.join(DATALAKE_DIR, "eventos"), exist_ok=True)
    tareas = [asyncio.create_task(consumidor()),
              asyncio.create_task(bucle_latidos(valkey, "historial", sonda))]
    yield
    for t in tareas:
        t.cancel()
    await valkey.aclose()


app = FastAPI(title="historial", lifespan=lifespan)
app.add_middleware(RequestIDMiddleware)


@app.get("/salud")
async def salud():
    estado, detalle = await sonda()
    return {"estado": estado, "detalle": detalle, "host": socket.gethostname()}


@app.get("/estado")
async def estado():
    info = {"stream": STREAM, "grupo": GRUPO, "consumidor": CONSUMIDOR,
            "lote_n": LOTE_N, "lote_t_s": LOTE_T_S}
    try:
        info["xlen"] = await valkey.xlen(STREAM)
        for g in await valkey.xinfo_groups(STREAM):
            if g["name"] == GRUPO:
                info["lag"] = g.get("lag")
                info["pendientes"] = g.get("pending")
                info["ultimo_entregado"] = g.get("last-delivered-id")
        resumen = await valkey.xpending(STREAM, GRUPO)
        info["pendientes"] = resumen.get("pending", info.get("pendientes"))
    except Exception as e:
        info["valkey_error"] = f"{type(e).__name__}: {e}"
    info.update(
        lotes=stats["lotes"], eventos_escritos=stats["eventos_escritos"],
        archivos_escritos=stats["archivos_escritos"], invalidos=stats["invalidos"],
        recuperados=stats["recuperados"], ultimo_error=stats["ultimo_error"],
        ultimos_lotes=list(stats["ultimos_lotes"]),
        archivos_por_mes=await asyncio.to_thread(lago.contar_archivos, DATALAKE_DIR))
    return info


@app.post("/compactar")
async def compactar(mes: str = Query(..., pattern=r"^\d{4}-\d{2}$")):
    """Fusiona los archivos pequeños de un mes (dedup por event_id, publicación atómica)."""
    async with lock_compactar:
        return await asyncio.to_thread(lago.compactar_mes, DATALAKE_DIR, mes)


@app.post("/reprocesar")
async def reprocesar(batch_id: str | None = None, duplicar: bool = False):
    """Gancho de pruebas (P4). Re-publica un micro-lote ya escrito, tomando sus eventos de
    la copia `_replay/` (o del stream con XRANGE si no existe).
    - duplicar=false: mismo nombre de archivo => sobrescritura idempotente (sin duplicados).
    - duplicar=true : archivo adicional con sufijo => las filas quedan duplicadas en el lago,
      para demostrar que Spark deduplica por event_id.
    Sin batch_id usa el último lote publicado."""
    if batch_id is None:
        if not stats["ultimos_lotes"]:
            raise HTTPException(404, "no hay lotes recientes")
        batch_id = next(l["batch_id"] for l in reversed(stats["ultimos_lotes"]) if l["batch_id"])
    try:
        entradas = await asyncio.to_thread(lago.leer_replay, DATALAKE_DIR, batch_id)
    except FileNotFoundError:
        try:
            primero, ultimo = batch_id.split("_")
        except ValueError:
            raise HTTPException(400, "batch_id inválido")
        msgs = await valkey.xrange(STREAM, primero, ultimo)
        entradas = [(s, c.get("evento")) for s, c in msgs if c and c.get("evento")]
        if not entradas:
            raise HTTPException(404, "lote no encontrado en replay ni en el stream")

    def _rep():
        filas = []
        for _, js in entradas:
            try:
                filas.append(lago.validar(json.loads(js)))
            except Exception:  # los inválidos ya se descartaron en el consumo original
                pass
        sufijo = f"-dup{int(time.time() * 1000)}" if duplicar else ""
        return lago.publicar_lote(DATALAKE_DIR, batch_id, filas, sufijo)

    rutas = await asyncio.to_thread(_rep)
    return {"batch_id": batch_id, "eventos": len(entradas), "duplicar": duplicar,
            "archivos": [os.path.relpath(r, DATALAKE_DIR) for r in rutas]}
