"""Gateway: único punto de entrada (puerto 8000). Valida, enruta, propaga
X-Request-ID / X-Anotador-ID, orquesta los flujos entre servicios y publica los
eventos en el stream `eventos` de Valkey después de que MongoDB confirma.

Políticas:
 - Reintentos seguros SÓLO en llamadas idempotentes (GET, PUT borrador, salud).
 - NUNCA se reintenta enviar / transiciones / asignar.
 - /salud nunca devuelve 500: 200 (operativo/degradado) o 503 (caído), siempre con cuerpo.
"""
import asyncio
import json
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field, model_validator

from common import config
from common.heartbeat import publicar
from common.logs import RequestIDMiddleware, request_id_ctx, setup_logging

log = setup_logging("gateway")

URL_REGISTRO = config.env("REGISTRO_URL", "http://registro:8001")
URL_SESION = config.env("SESION_URL", "http://sesion:8002")
URL_HISTORIAL = config.env("HISTORIAL_URL", "http://historial:8003")
URL_ANALITICA = config.env("ANALITICA_URL", "http://analitica:8004")

# Clientes HTTP con timeouts por destino. keepalive_expiry (2 s) < keep-alive de uvicorn en los
# servicios internos (5 s): evita reutilizar una conexión que el servidor acaba de cerrar
# (ReadError/RemoteProtocolError que, con 100 VUs en P6, producía algún 503 espurio).
http_registro = httpx.AsyncClient(base_url=URL_REGISTRO, timeout=httpx.Timeout(10.0, connect=2.0),
                                  limits=httpx.Limits(max_connections=300, keepalive_expiry=2.0))
http_sesion = httpx.AsyncClient(base_url=URL_SESION, timeout=httpx.Timeout(5.0, connect=2.0),
                                limits=httpx.Limits(max_connections=300, keepalive_expiry=2.0))
http_analitica = httpx.AsyncClient(base_url=URL_ANALITICA, timeout=httpx.Timeout(900.0, connect=2.0))
http_sonda = httpx.AsyncClient(timeout=httpx.Timeout(1.0, connect=0.5))

valkey = aioredis.from_url(config.VALKEY_URL, decode_responses=True,
                           socket_timeout=2, socket_connect_timeout=1)

STREAM = "eventos"
buffer_eventos: deque = deque(maxlen=200000)  # eventos pendientes si Valkey estuvo caído
_ultima_renovacion: dict[str, float] = {}      # tarea_id -> monotonic de la última renovación en Mongo
_tareas_bg: set = set()


# ------------------------------------------------------------ utilidades
def cabeceras(anotador: Optional[str] = None) -> dict:
    h = {"X-Request-ID": request_id_ctx.get()}
    if anotador:
        h["X-Anotador-ID"] = anotador
    return h


async def llamar(cliente: httpx.AsyncClient, metodo: str, ruta: str, *, anotador: str | None = None,
                 json_: Any = None, params: dict | None = None, reintentos: int = 0,
                 destino: str = "servicio") -> httpx.Response:
    """Una llamada interna. `reintentos`>0 sólo para operaciones idempotentes.
    Fallos de red/timeout -> HTTP 503 hacia el cliente."""
    ultimo = None
    for intento in range(reintentos + 1):
        try:
            resp = await cliente.request(metodo, ruta, json=json_, params=params, headers=cabeceras(anotador))
            if resp.status_code == 503 and intento < reintentos:
                await asyncio.sleep(0.15 * (3 ** intento))
                continue
            return resp
        except httpx.HTTPError as e:
            ultimo = e
            if intento < reintentos:
                await asyncio.sleep(0.15 * (3 ** intento))
    log.warning("%s no disponible (%s %s): %s", destino, metodo, ruta, type(ultimo).__name__)
    raise HTTPException(503, f"{destino}_no_disponible")


def detalle(resp: httpx.Response) -> Any:
    try:
        return resp.json().get("detail", resp.text)
    except Exception:
        return resp.text


def exigir_ok(resp: httpx.Response, *, destino: str = "servicio") -> Any:
    """Devuelve el JSON si 2xx; si no, propaga el código (503 si el servicio dice 5xx)."""
    if resp.status_code >= 500:
        raise HTTPException(503, detalle(resp) if resp.status_code == 503 else f"{destino}_error")
    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, detalle(resp))
    return resp.json()


def segundo_plano(coro):
    t = asyncio.create_task(coro)
    _tareas_bg.add(t)
    t.add_done_callback(_tareas_bg.discard)


# ------------------------------------------------------------ eventos -> stream
async def emitir(evento: Optional[dict]) -> None:
    """XADD del evento (tras confirmación de Mongo). Si Valkey falla, se guarda en un
    buffer en memoria que se reintenta cada 2 s y /salud lo reporta como degradado."""
    if not evento:
        return
    try:
        await valkey.xadd(STREAM, {"evento": json.dumps(evento, separators=(",", ":"))})
    except Exception as e:
        buffer_eventos.append(evento)
        log.error("XADD fallo (%s); evento %s en buffer local (%d)", type(e).__name__,
                  evento.get("event_id"), len(buffer_eventos))


async def vaciar_buffer() -> None:
    while True:
        await asyncio.sleep(2)
        try:
            while buffer_eventos:
                ev = buffer_eventos[0]
                await valkey.xadd(STREAM, {"evento": json.dumps(ev, separators=(",", ":"))})
                buffer_eventos.popleft()
                log.info("evento %s reenviado desde buffer", ev.get("event_id"))
        except Exception:
            pass


async def sondear_registro() -> dict:
    t0 = time.perf_counter()
    try:
        resp = await http_sonda.get(URL_REGISTRO + "/salud", headers=cabeceras())
        d = resp.json() if resp.status_code == 200 else {"estado": "caido", "mongo": {"estado": "caido"}}
        d["ms"] = round((time.perf_counter() - t0) * 1000)
        return d
    except Exception as e:
        return {"estado": "caido", "detalle": type(e).__name__,
                "mongo": {"estado": "desconocido", "detalle": "registro no responde"}}


async def latido_gateway() -> None:
    """El gateway publica sus propios latidos y los de registro/mongo/valkey
    (registro no puede usar Valkey: regla de aislamiento)."""
    while True:
        try:
            d = await sondear_registro()
            estado_gw = "degradado" if buffer_eventos else "ok"
            await publicar(valkey, "gateway", estado_gw,
                           f"{len(buffer_eventos)} eventos en buffer" if buffer_eventos else "")
            await publicar(valkey, "registro", d["estado"], d.get("detalle", ""))
            m = d.get("mongo", {})
            await publicar(valkey, "mongo", m.get("estado", "desconocido"), m.get("detalle", ""))
            await publicar(valkey, "valkey", "ok", "ping ok")
        except Exception as e:
            log.warning("latido gateway fallo: %s", type(e).__name__)
        await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tareas = [asyncio.create_task(latido_gateway()), asyncio.create_task(vaciar_buffer())]
    yield
    for t in tareas:
        t.cancel()


app = FastAPI(title="gateway", lifespan=lifespan)
app.add_middleware(RequestIDMiddleware)


# ------------------------------------------------------------ modelos
class EtiquetaIn(BaseModel):
    etiqueta: str = Field(min_length=1)
    instruccion: str = ""


class ProyectoIn(BaseModel):
    nombre: str = Field(min_length=1)
    modalidad: Literal["texto", "imagen", "video", "lidar"]
    taxonomia: list[EtiquetaIn] = Field(min_length=1)
    sla_horas: int = Field(gt=0)
    reglas: dict = {}
    cliente_id: Optional[str] = None
    cliente_nombre: Optional[str] = None


class ArchivoIn(BaseModel):
    uri: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    bytes: int = Field(ge=0)
    mime: str
    meta: dict = {}


class TareaIn(BaseModel):
    archivo: ArchivoIn
    tipo_tarea: Optional[Literal["span", "bbox", "poligono", "bbox_video", "cuboide_3d"]] = None
    prioridad: Optional[int] = Field(default=None, ge=1, le=5)


class LoteIn(BaseModel):
    nombre: Optional[str] = None
    prioridad: int = Field(default=3, ge=1, le=5)
    tareas: list[TareaIn] = Field(min_length=1, max_length=5000)


class SiguienteIn(BaseModel):
    anotador_id: str = Field(min_length=1)
    proyecto_id: Optional[str] = None


class BorradorIn(BaseModel):
    anotador_id: str
    token: str
    contenido: dict
    version_local: int = 0


class EnviarIn(BaseModel):
    anotador_id: str
    token: str
    anotacion: dict


class LiberarIn(BaseModel):
    anotador_id: str
    token: str


class RevisionIn(BaseModel):
    revisor_id: str
    accion: Literal["iniciar", "aprobar", "rechazar"]
    motivo_codigo: Optional[str] = None

    @model_validator(mode="after")
    def _motivo(self):
        if self.accion == "rechazar" and not self.motivo_codigo:
            raise ValueError("motivo_codigo es obligatorio para rechazar")
        return self


class RecuperarIn(BaseModel):
    anotador_id: str


# ------------------------------------------------------------ proyectos / lotes
@app.post("/proyectos", status_code=201)
async def crear_proyecto(p: ProyectoIn):
    resp = await llamar(http_registro, "POST", "/proyectos", json_=p.model_dump(), destino="registro")
    return JSONResponse(exigir_ok(resp, destino="registro"), status_code=201)


@app.post("/proyectos/{proyecto_id}/lotes", status_code=201)
async def crear_lote(proyecto_id: str, lote: LoteIn):
    resp = await llamar(http_registro, "POST", f"/proyectos/{proyecto_id}/lotes",
                        json_=lote.model_dump(), destino="registro")
    return JSONResponse(exigir_ok(resp, destino="registro"), status_code=201)


# ------------------------------------------------------------ flujo de la tarea
async def _respuesta_tarea(tarea: dict, token: str, borrador: Optional[dict], reanudada: bool,
                           lock_ttl_ms: Optional[int] = None) -> dict:
    return {"tarea": tarea, "token": token, "borrador": borrador, "reanudada": reanudada,
            "lock_ttl_ms": lock_ttl_ms, "expira": (tarea.get("asignacion") or {}).get("expira")}


@app.post("/tareas/siguiente")
async def siguiente(req: SiguienteIn):
    a = req.anotador_id
    # 1) estado de sesión previo (borrador recuperable)
    resp = await llamar(http_sesion, "POST", "/sesiones/recuperar", anotador=a,
                        json_={"anotador_id": a}, reintentos=1, destino="sesion")
    previa = exigir_ok(resp, destino="sesion")
    # 2) asignación atómica en Mongo (o reanudación de la tarea abierta vigente)
    resp = await llamar(http_registro, "POST", "/tareas/asignar", anotador=a,
                        json_={"anotador_id": a, "proyecto_id": req.proyecto_id}, destino="registro")
    if resp.status_code == 204:
        return Response(status_code=204)
    datos = exigir_ok(resp, destino="registro")
    tarea = datos["tarea"]
    token = tarea["asignacion"]["token"]
    # 3) lock + sesión
    try:
        resp = await llamar(http_sesion, "POST", "/sesiones/abrir", anotador=a, destino="sesion", json_={
            "anotador_id": a, "tarea_id": tarea["_id"], "proyecto_id": tarea["proyecto_id"], "token": token})
    except HTTPException:
        await _compensar(tarea, a, token)
        raise
    if resp.status_code != 200:
        await _compensar(tarea, a, token)
        if resp.status_code == 409:
            raise HTTPException(409, "lock_ocupado_para_la_tarea_asignada")
        raise HTTPException(503, "sesion_no_disponible")
    abierta = resp.json()
    borrador = abierta.get("borrador")
    if borrador is None and previa.get("sesion") and previa["sesion"].get("tarea_id") == tarea["_id"]:
        borrador = previa.get("borrador")
    _ultima_renovacion[tarea["_id"]] = time.monotonic()
    await emitir(datos.get("evento"))
    return await _respuesta_tarea(tarea, token, borrador, datos.get("reanudada", False),
                                  abierta.get("lock_ttl_ms"))


async def _compensar(tarea: dict, anotador: str, token: str) -> None:
    """Falló el lock tras asignar en Mongo: devolver la tarea a la cola (liberar)."""
    try:
        r = await llamar(http_registro, "POST", f"/tareas/{tarea['_id']}/transicion", anotador=anotador,
                         json_={"accion": "liberar", "anotador_id": anotador, "token": token},
                         destino="registro")
        if r.status_code == 200:
            await emitir(r.json().get("evento"))
        else:
            log.error("compensacion liberar respondio %s", r.status_code)
    except HTTPException:
        log.error("compensacion liberar fallo; la tarea %s quedara abierta hasta su plazo", tarea["_id"])


async def _renovar_en_mongo(tarea_id: str, anotador: str, token: str):
    try:
        await llamar(http_registro, "POST", f"/tareas/{tarea_id}/transicion", anotador=anotador,
                     json_={"accion": "renovar", "anotador_id": anotador, "token": token},
                     destino="registro")
    except HTTPException:
        pass


@app.put("/tareas/{tarea_id}/borrador")
async def borrador(tarea_id: str, b: BorradorIn):
    resp = await llamar(http_sesion, "PUT", f"/borradores/{tarea_id}", anotador=b.anotador_id,
                        json_=b.model_dump(), reintentos=2, destino="sesion")
    d = exigir_ok(resp, destino="sesion")
    # el plazo de Mongo se renueva con el latido, pero no en cada borrador (cada LOCK_TTL/4)
    ahora = time.monotonic()
    if ahora - _ultima_renovacion.get(tarea_id, 0) > config.LOCK_TTL_MS / 4000:
        _ultima_renovacion[tarea_id] = ahora
        segundo_plano(_renovar_en_mongo(tarea_id, b.anotador_id, b.token))
    return d


async def _liberar_sesion(tarea_id: str, anotador: str, token: str, proyecto_id: Optional[str] = None):
    try:
        await llamar(http_sesion, "POST", f"/locks/{tarea_id}/liberar", anotador=anotador, reintentos=1,
                     json_={"token": token, "anotador_id": anotador, "proyecto_id": proyecto_id},
                     destino="sesion")
    except HTTPException:
        log.warning("no se pudo limpiar la sesion de la tarea %s (el lock vence solo)", tarea_id)
    _ultima_renovacion.pop(tarea_id, None)


@app.post("/tareas/{tarea_id}/enviar")
async def enviar(tarea_id: str, e: EnviarIn):
    a = e.anotador_id
    # 1) almacenamiento temporal del envío pendiente en Valkey (verifica además el lock/token)
    # (PUT idempotente: se puede reintentar una vez sin riesgo)
    resp = await llamar(http_sesion, "PUT", f"/borradores/{tarea_id}", anotador=a, destino="sesion", reintentos=1,
                        json_={"anotador_id": a, "token": e.token, "contenido": e.anotacion,
                               "pendiente": True})
    if resp.status_code == 409:
        raise HTTPException(409, "token_o_lock_invalido")
    previo = bool(exigir_ok(resp, destino="sesion").get("previo"))  # ¿quedó un envío anterior sin confirmar?
    # 2) Mongo decide (sin reintentos)
    try:
        resp = await llamar(http_registro, "POST", f"/tareas/{tarea_id}/transicion", anotador=a,
                            destino="registro", json_={"accion": "enviar", "anotador_id": a,
                                                       "token": e.token, "anotacion": e.anotacion})
    except HTTPException:  # 503: el borrador y el pendiente siguen en Valkey
        raise
    if resp.status_code == 409 and previo:
        # Reconciliación: el intento anterior dio 503 pero Mongo pudo haber aplicado el envío (p. ej. timeout
        # de w:majority). Si la tarea sigue `anotada` con este token, se publica el evento que faltaba (mismo
        # event_id: idempotente) y se responde 200. Un segundo envío posterior ya no tendrá pendiente -> 409.
        rc = await llamar(http_registro, "POST", f"/tareas/{tarea_id}/transicion", anotador=a, destino="registro",
                          json_={"accion": "confirmar_envio", "anotador_id": a, "token": e.token})
        if rc.status_code == 200:
            d = rc.json()
            log.warning("envio reconciliado tarea=%s evento=%s", tarea_id, d["evento"]["event_id"])
            await emitir(d["evento"])
            await _liberar_sesion(tarea_id, a, e.token, d["tarea"]["proyecto_id"])
            return {"tarea_id": tarea_id, "estado": d["tarea"]["estado"], "version": d["tarea"]["version"],
                    "evento_id": d["evento"]["event_id"], "reconciliado": True}
    if resp.status_code == 409 or resp.status_code == 404:
        await llamar(http_sesion, "POST", f"/locks/{tarea_id}/liberar", anotador=a, destino="sesion",
                     json_={"token": e.token, "solo_pendiente": True})
        raise HTTPException(resp.status_code, detalle(resp))
    d = exigir_ok(resp, destino="registro")  # 5xx -> 503, pendiente conservado
    # 3) 200: evento al stream, limpiar borrador y liberar lock
    await emitir(d["evento"])
    await _liberar_sesion(tarea_id, a, e.token, d["tarea"]["proyecto_id"])
    return {"tarea_id": tarea_id, "estado": d["tarea"]["estado"], "version": d["tarea"]["version"],
            "evento_id": d["evento"]["event_id"]}


@app.post("/tareas/{tarea_id}/liberar")
async def liberar(tarea_id: str, req: LiberarIn):
    a = req.anotador_id
    resp = await llamar(http_registro, "POST", f"/tareas/{tarea_id}/transicion", anotador=a,
                        destino="registro",
                        json_={"accion": "liberar", "anotador_id": a, "token": req.token})
    d = exigir_ok(resp, destino="registro")
    await emitir(d["evento"])
    await _liberar_sesion(tarea_id, a, req.token, d["tarea"]["proyecto_id"])
    return {"tarea_id": tarea_id, "estado": d["tarea"]["estado"], "version": d["tarea"]["version"]}


ACCION_REGISTRO = {"iniciar": "iniciar_revision", "aprobar": "aprobar", "rechazar": "rechazar"}


@app.post("/tareas/{tarea_id}/revision")
async def revision(tarea_id: str, req: RevisionIn):
    resp = await llamar(http_registro, "POST", f"/tareas/{tarea_id}/transicion", destino="registro",
                        json_={"accion": ACCION_REGISTRO[req.accion], "revisor_id": req.revisor_id,
                               "motivo_codigo": req.motivo_codigo})
    d = exigir_ok(resp, destino="registro")
    await emitir(d["evento"])
    return {"tarea_id": tarea_id, "estado": d["tarea"]["estado"], "version": d["tarea"]["version"],
            "evento": d["evento"]["estado"]}


@app.post("/sesiones/recuperar")
async def recuperar(req: RecuperarIn):
    a = req.anotador_id
    resp = await llamar(http_sesion, "POST", "/sesiones/recuperar", anotador=a, json_={"anotador_id": a},
                        reintentos=1, destino="sesion")
    s = exigir_ok(resp, destino="sesion")
    if not s.get("sesion"):
        raise HTTPException(404, "sin_sesion_recuperable")
    # la tarea sólo es recuperable si Mongo la sigue teniendo abierta para este anotador
    resp = await llamar(http_registro, "POST", "/tareas/asignar", anotador=a, destino="registro",
                        json_={"anotador_id": a, "solo_reanudar": True})
    if resp.status_code == 204:
        await _liberar_sesion(s["sesion"]["tarea_id"], a, s["sesion"]["token"])
        raise HTTPException(404, "sin_sesion_recuperable")
    tarea = exigir_ok(resp, destino="registro")["tarea"]
    token = tarea["asignacion"]["token"]
    lock = s.get("lock") or {}
    if not lock.get("valido"):  # el lock se perdió (p. ej. Valkey sin AOF) pero Mongo mantiene la tarea
        resp = await llamar(http_sesion, "POST", "/sesiones/abrir", anotador=a, destino="sesion", json_={
            "anotador_id": a, "tarea_id": tarea["_id"], "proyecto_id": tarea["proyecto_id"], "token": token})
        if resp.status_code == 409:
            raise HTTPException(409, "lock_ocupado")
        exigir_ok(resp, destino="sesion")
    return {"tarea": tarea, "token": token, "borrador": s.get("borrador"),
            "pendiente": s.get("pendiente"), "lock": lock}


@app.get("/proyectos/{proyecto_id}/editando")
async def editando(proyecto_id: str):
    resp = await llamar(http_sesion, "GET", f"/proyectos/{proyecto_id}/editando", reintentos=2,
                        destino="sesion")
    return exigir_ok(resp, destino="sesion")


# ------------------------------------------------------------ exportación y analítica
@app.get("/proyectos/{proyecto_id}/exportar")
async def exportar(proyecto_id: str, cursor: Optional[str] = None, limite: int = Query(100, ge=1, le=1000)):
    params = {"limite": limite}
    if cursor:
        params["cursor"] = cursor
    resp = await llamar(http_registro, "GET", f"/proyectos/{proyecto_id}/exportar", params=params,
                        reintentos=2, destino="registro")
    return exigir_ok(resp, destino="registro")


@app.get("/analitica/{consulta}")
async def analitica(consulta: Literal["q1", "q2", "q3", "q4", "q5"],
                    conjunto: Literal["principal", "grande"] = "principal", explain: int = Query(0, ge=0, le=1)):
    resp = await llamar(http_analitica, "GET", f"/q/{consulta}", params={"conjunto": conjunto, "explain": explain},
                        destino="analitica")
    return exigir_ok(resp, destino="analitica")


# ------------------------------------------------------------ salud
CAPAS = ["gateway", "registro", "sesion", "historial", "analitica", "mongo", "valkey", "spark"]
NUCLEO = {"registro", "mongo", "valkey", "sesion"}  # sin ellas no hay flujo de tareas


async def _sonda_http(url: str) -> dict:
    t0 = time.perf_counter()
    for intento in range(2):  # salud es idempotente: 1 reintento corto
        try:
            resp = await http_sonda.get(url, headers=cabeceras())
            ms = round((time.perf_counter() - t0) * 1000)
            if resp.status_code != 200:
                return {"estado": "caido", "detalle": f"HTTP {resp.status_code}", "ms": ms}
            d = resp.json() if resp.content else {}
            return {"estado": d.get("estado", "ok") if d.get("estado") in ("ok", "degradado", "caido") else "ok",
                    "detalle": d.get("detalle", ""), "ms": ms, "_raw": d}
        except Exception as e:
            err = type(e).__name__
            if intento == 0:
                await asyncio.sleep(0.1)
    return {"estado": "caido", "detalle": err, "ms": round((time.perf_counter() - t0) * 1000)}


async def _sonda_valkey() -> dict:
    t0 = time.perf_counter()
    try:
        await asyncio.wait_for(valkey.ping(), 1.0)
        return {"estado": "ok", "detalle": "ping ok", "ms": round((time.perf_counter() - t0) * 1000)}
    except Exception as e:
        return {"estado": "caido", "detalle": type(e).__name__, "ms": round((time.perf_counter() - t0) * 1000)}


async def _latidos(valkey_ok: bool) -> dict:
    if not valkey_ok:
        return {}
    try:
        vals = await asyncio.wait_for(valkey.mget([f"salud:{c}" for c in CAPAS]), 1.0)
    except Exception:
        return {}
    out, ahora = {}, time.time() * 1000
    for c, v in zip(CAPAS, vals):
        if v:
            try:
                j = json.loads(v)
                out[c] = {"estado": j.get("estado"), "detalle": j.get("detalle", ""),
                          "edad_s": round((ahora - j.get("ts", ahora)) / 1000, 1)}
            except Exception:
                pass
    return out


@app.get("/salud")
async def salud():
    """200 con estado operativo|degradado; 503 con el mismo cuerpo si está caído.
    Nunca lanza excepción: cada sonda está aislada."""
    try:
        reg, ses, his, ana, vk = await asyncio.gather(
            _sonda_http(URL_REGISTRO + "/salud"), _sonda_http(URL_SESION + "/salud"),
            _sonda_http(URL_HISTORIAL + "/salud"), _sonda_http(URL_ANALITICA + "/salud"),
            _sonda_valkey(), return_exceptions=False)
        lat = await _latidos(vk["estado"] == "ok")
    except Exception as e:  # defensa final
        log.exception("salud: error inesperado")
        return JSONResponse(status_code=503, content={"estado": "caido", "detalle": str(e), "capas": {}})

    mongo = (reg.get("_raw") or {}).get("mongo") or {"estado": "caido" if reg["estado"] == "caido" else "ok"}
    if reg["estado"] == "caido" and "mongo" not in (reg.get("_raw") or {}):
        mongo = {"estado": "desconocido", "detalle": "registro no responde"}
    gw_estado = "degradado" if buffer_eventos else "ok"
    capas = {
        "gateway": {"estado": gw_estado, "detalle": f"{len(buffer_eventos)} eventos en buffer local"
                    if buffer_eventos else ""},
        "registro": {k: v for k, v in reg.items() if k != "_raw"},
        "mongo": {"estado": mongo.get("estado", "ok"), "detalle": mongo.get("detalle", "")},
        "sesion": {k: v for k, v in ses.items() if k != "_raw"},
        "valkey": vk,
        "historial": {k: v for k, v in his.items() if k != "_raw"},
        "analitica": {k: v for k, v in ana.items() if k != "_raw"},
    }
    # spark: sólo se conoce por el latido que publica analítica
    sp = lat.get("spark")
    if sp and sp["edad_s"] <= 15:
        capas["spark"] = {"estado": sp["estado"], "detalle": sp["detalle"]}
    else:
        capas["spark"] = {"estado": "caido" if vk["estado"] == "ok" else "desconocido",
                          "detalle": "sin latido reciente en Valkey" if vk["estado"] == "ok"
                          else "Valkey caido: no se puede leer el latido"}
    for c in capas:
        if c in lat:
            capas[c]["latido"] = lat[c]
    estados = {c: v["estado"] for c, v in capas.items()}
    if any(estados[c] == "caido" or estados[c] == "desconocido" for c in NUCLEO):
        global_ = "caido"
    elif any(e != "ok" for e in estados.values()):
        global_ = "degradado"
    else:
        global_ = "operativo"
    cuerpo = {"estado": global_, "capas": capas, "ts": int(time.time() * 1000)}
    return JSONResponse(status_code=503 if global_ == "caido" else 200, content=cuerpo)
