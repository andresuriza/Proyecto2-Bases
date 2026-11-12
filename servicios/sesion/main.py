"""Servicio de sesión: locks, sesiones, borradores y latidos en Valkey.

Regla: no usa pymongo ni decide el estado definitivo de una tarea.
"""
import asyncio
import json
import time
from contextlib import asynccontextmanager
from typing import Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from redis import exceptions as re_

from common import config
from common.heartbeat import bucle_latidos
from common.logs import RequestIDMiddleware, setup_logging

log = setup_logging("sesion")
r = aioredis.from_url(config.VALKEY_URL, decode_responses=True,
                      socket_timeout=2, socket_connect_timeout=2, health_check_interval=15)

# Compare-and-pexpire / compare-and-del (atómicos en el servidor)
LUA_RENOVAR = r.register_script(
    "if redis.call('GET', KEYS[1]) == ARGV[1] then "
    "return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end")
LUA_LIBERAR = r.register_script(
    "if redis.call('GET', KEYS[1]) == ARGV[1] then "
    "return redis.call('DEL', KEYS[1]) else return 0 end")

BORRADOR_TTL_S = 7 * 24 * 3600
PENDIENTE_TTL_S = 3600


def now_ms() -> int:
    return int(time.time() * 1000)


async def sonda():
    await r.ping()
    return "ok", "valkey responde"


@asynccontextmanager
async def lifespan(app: FastAPI):
    t = asyncio.create_task(bucle_latidos(r, "sesion", sonda))
    yield
    t.cancel()
    await r.aclose()


app = FastAPI(title="sesion", lifespan=lifespan)
app.add_middleware(RequestIDMiddleware)


@app.exception_handler(re_.RedisError)
async def _redis_error(request: Request, exc: re_.RedisError):
    log.error("Valkey no disponible: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "valkey_no_disponible"})


class AbrirIn(BaseModel):
    anotador_id: str
    tarea_id: str
    proyecto_id: str
    token: str
    etapa: str = "anotacion"


class BorradorIn(BaseModel):
    anotador_id: str
    token: str
    contenido: dict
    version_local: int = 0
    pendiente: bool = False  # True: guarda el envío pendiente (no toca el borrador)


class RecuperarIn(BaseModel):
    anotador_id: str


class LiberarIn(BaseModel):
    token: str
    anotador_id: Optional[str] = None
    proyecto_id: Optional[str] = None
    solo_pendiente: bool = False


def _borrador_out(b: dict) -> dict:
    ts = int(b.get("ts", 0))
    return {"contenido": json.loads(b.get("contenido", "{}")), "ts": ts, "edad_ms": now_ms() - ts,
            "version_local": int(b.get("version_local", 0)), "anotador_id": b.get("anotador_id")}


@app.post("/sesiones/abrir")
async def abrir(req: AbrirIn):
    lock = f"lock:tarea:{req.tarea_id}"
    ok = await r.set(lock, req.token, nx=True, px=config.LOCK_TTL_MS)
    if not ok:
        # idempotencia: el mismo token puede reabrir (reanudar tarea)
        if not await LUA_RENOVAR(keys=[lock], args=[req.token, config.LOCK_TTL_MS]):
            raise HTTPException(409, "lock_ocupado")
    ahora = now_ms()
    pipe = r.pipeline()
    pipe.hset(f"sesion:{req.anotador_id}", mapping={
        "tarea_id": req.tarea_id, "proyecto_id": req.proyecto_id, "etapa": req.etapa,
        "token": req.token, "actualizado": ahora})
    pipe.zadd(f"editando:{req.proyecto_id}", {req.anotador_id: ahora})
    pipe.hgetall(f"borrador:{req.tarea_id}")
    _, _, b = await pipe.execute()
    borrador = _borrador_out(b) if b and b.get("anotador_id") == req.anotador_id else None
    return {"ok": True, "lock_ttl_ms": config.LOCK_TTL_MS, "borrador": borrador}


@app.put("/borradores/{tarea_id}")
async def guardar_borrador(tarea_id: str, req: BorradorIn):
    if not await LUA_RENOVAR(keys=[f"lock:tarea:{tarea_id}"], args=[req.token, config.LOCK_TTL_MS]):
        raise HTTPException(409, "lock_no_vigente_o_token_invalido")
    ahora = now_ms()
    contenido = json.dumps(req.contenido, separators=(",", ":"))
    if req.pendiente:
        previo = bool(await r.exists(f"pendiente:{tarea_id}"))  # hay un envío anterior sin confirmar (503)
        await r.set(f"pendiente:{tarea_id}", json.dumps(
            {"anotador_id": req.anotador_id, "token": req.token, "contenido": req.contenido, "ts": ahora}),
            ex=PENDIENTE_TTL_S)
        return {"ok": True, "pendiente": True, "ts": ahora, "previo": previo}
    proyecto = await r.hget(f"sesion:{req.anotador_id}", "proyecto_id")
    pipe = r.pipeline()
    pipe.hset(f"borrador:{tarea_id}", mapping={
        "anotador_id": req.anotador_id, "token": req.token, "contenido": contenido,
        "ts": ahora, "version_local": req.version_local})
    pipe.expire(f"borrador:{tarea_id}", BORRADOR_TTL_S)
    pipe.hset(f"sesion:{req.anotador_id}", "actualizado", ahora)
    if proyecto:
        pipe.zadd(f"editando:{proyecto}", {req.anotador_id: ahora})
    await pipe.execute()
    return {"ok": True, "ts": ahora, "lock_ttl_ms": config.LOCK_TTL_MS}


@app.post("/sesiones/recuperar")
async def recuperar(req: RecuperarIn):
    s = await r.hgetall(f"sesion:{req.anotador_id}")
    if not s:
        return {"sesion": None, "borrador": None, "lock": None}
    tarea = s["tarea_id"]
    pipe = r.pipeline()
    pipe.get(f"lock:tarea:{tarea}")
    pipe.pttl(f"lock:tarea:{tarea}")
    pipe.hgetall(f"borrador:{tarea}")
    pipe.get(f"pendiente:{tarea}")
    valor, ttl, b, pend = await pipe.execute()
    return {"sesion": s,
            "lock": {"valido": valor == s.get("token"), "ttl_ms": ttl if ttl and ttl > 0 else 0},
            "borrador": _borrador_out(b) if b else None,
            "pendiente": json.loads(pend) if pend else None}


@app.post("/locks/{tarea_id}/liberar")
async def liberar(tarea_id: str, req: LiberarIn):
    if req.solo_pendiente:
        await r.delete(f"pendiente:{tarea_id}")
        return {"ok": True, "solo_pendiente": True}
    lock = f"lock:tarea:{tarea_id}"
    liberado = await LUA_LIBERAR(keys=[lock], args=[req.token])
    if not liberado and await r.exists(lock):
        raise HTTPException(409, "lock_de_otro_token")  # no se toca nada ajeno
    b = await r.hgetall(f"borrador:{tarea_id}")
    anotador = req.anotador_id or b.get("anotador_id")
    pipe = r.pipeline()
    pipe.delete(f"borrador:{tarea_id}", f"pendiente:{tarea_id}")
    if anotador:
        s = await r.hgetall(f"sesion:{anotador}")
        proyecto = req.proyecto_id or s.get("proyecto_id")
        if s.get("tarea_id") == tarea_id:
            pipe.delete(f"sesion:{anotador}")
        if proyecto:
            pipe.zrem(f"editando:{proyecto}", anotador)
    await pipe.execute()
    return {"ok": True, "lock_liberado": bool(liberado)}


@app.get("/proyectos/{proyecto_id}/editando")
async def editando(proyecto_id: str):
    clave = f"editando:{proyecto_id}"
    await r.zremrangebyscore(clave, "-inf", now_ms() - config.LOCK_TTL_MS)  # latidos vencidos
    items = await r.zrange(clave, 0, -1, withscores=True)
    return {"proyecto_id": proyecto_id, "n": len(items),
            "editando": [{"anotador_id": a, "ultimo_latido": int(s)} for a, s in items]}


@app.get("/salud")
async def salud():
    try:
        await r.ping()
        return {"estado": "ok", "detalle": "valkey responde"}
    except re_.RedisError as e:
        return {"estado": "caido", "detalle": f"valkey: {type(e).__name__}"}
