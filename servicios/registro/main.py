"""Servicio de registro: estado vigente de tareas en MongoDB (replica set).

Reglas: no usa Valkey/redis; decide todas las transiciones de estado; devuelve
en cada transición el evento (fila del datalake) para que el gateway lo publique.
"""
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional

import anyio.to_thread
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo import errors as pe

from common import config
from common.logs import RequestIDMiddleware, setup_logging

log = setup_logging("registro")

MODALIDAD_TIPO = {"texto": "span", "imagen": "bbox", "video": "bbox_video", "lidar": "cuboide_3d"}
TIPOS_VALIDOS = {"span", "bbox", "poligono", "bbox_video", "cuboide_3d"}

# Escritura: w configurable (majority por defecto); lectura siempre del primario.
cliente = MongoClient(
    config.MONGO_URI,
    tz_aware=True,
    w=("majority" if config.MONGO_W == "majority" else int(config.MONGO_W)),
    wTimeoutMS=3000,
    readPreference="primary",
    serverSelectionTimeoutMS=2500,
    connectTimeoutMS=2000,
    socketTimeoutMS=8000,
    maxPoolSize=100,
    appname="registro",
)
db = cliente["plataforma"]
# cliente aparte, con timeout muy corto, sólo para /salud
cliente_salud = MongoClient(config.MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=1000,
                            connectTimeoutMS=1000, socketTimeoutMS=2000, appname="registro-salud")


def ahora() -> datetime:
    return datetime.now(timezone.utc)


def crear_indices():
    t = db.tareas
    t.create_index([("proyecto_id", ASCENDING), ("estado", ASCENDING), ("prioridad", DESCENDING)],
                   name="tareas_proyecto_estado_prioridad")
    t.create_index([("asignacion.anotador_id", ASCENDING), ("estado", ASCENDING)],
                   name="tareas_anotador_estado")
    t.create_index([("batch_id", ASCENDING)], name="tareas_batch")
    # extra justificado (P2, explain): asignación SIN filtro de proyecto y re-oferta de tareas vencidas.
    # Sirve a las dos ramas del $or de /tareas/asignar con orden por prioridad (SORT_MERGE):
    # (estado=disponible, prioridad desc) y (estado=abierta, prioridad, expira<ahora).
    # Antes: IXSCAN tareas_estado_expira examinaba ~60 000 claves y ordenaba en memoria (~105 ms).
    t.create_index([("estado", ASCENDING), ("prioridad", DESCENDING), ("asignacion.expira", ASCENDING)],
                   name="tareas_estado_prioridad_expira")
    try:
        t.drop_index("tareas_estado_expira")  # versión anterior del índice extra (reemplazada)
    except pe.OperationFailure:
        pass
    db.proyectos.create_index([("cliente_id", ASCENDING), ("estado", ASCENDING)],
                              name="proyectos_cliente_estado")


@asynccontextmanager
async def lifespan(app: FastAPI):
    anyio.to_thread.current_default_thread_limiter().total_tokens = 200
    try:
        await anyio.to_thread.run_sync(crear_indices)
        log.info("indices listos")
    except pe.PyMongoError as e:  # arranca igual; se reintenta en cada /salud
        log.warning("no se pudieron crear indices al inicio: %s", e)
    yield


app = FastAPI(title="registro", lifespan=lifespan)
app.add_middleware(RequestIDMiddleware)


@app.exception_handler(pe.PyMongoError)
async def _mongo_error(request: Request, exc: pe.PyMongoError):
    # AutoReconnect (incluye ServerSelectionTimeout y NotPrimary), WTimeout, red, etc.
    log.error("MongoDB no disponible: %s %s", type(exc).__name__, exc)
    return JSONResponse(status_code=503,
                        content={"detail": "mongo_no_disponible", "error": type(exc).__name__})


# ---------------------------------------------------------------- eventos
def ms(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def construir_evento(tarea: dict, estado: str, event_id: str, ts: datetime, *,
                     motivo: str | None = None, revisor_id: str | None = None) -> dict:
    asg = tarea.get("asignacion") or {}
    return {
        "event_id": event_id,
        "ts": ms(ts),
        "mes": ts.strftime("%Y-%m"),
        "tarea_id": tarea["_id"],
        "proyecto_id": tarea["proyecto_id"],
        "cliente_id": tarea.get("cliente_id"),
        "batch_id": tarea.get("batch_id"),
        "equipo_id": asg.get("equipo_id"),
        "anotador_id": asg.get("anotador_id"),
        "revisor_id": revisor_id,
        "modalidad": tarea["modalidad"],
        "tipo_tarea": tarea["tipo_tarea"],
        "estado": estado,
        "motivo": motivo,
        "version": int(tarea["version"]),
        "tomada_en": ms(asg.get("tomada_en")),
        "sla_horas": int(tarea.get("sla_horas") or 0),
    }


def ser(doc: Any) -> Any:
    """datetime -> ISO para la respuesta JSON."""
    if isinstance(doc, dict):
        return {k: ser(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [ser(v) for v in doc]
    if isinstance(doc, datetime):
        return ms(doc)
    return doc


# ---------------------------------------------------------------- modelos
class EtiquetaIn(BaseModel):
    etiqueta: str
    instruccion: str = ""


class ProyectoIn(BaseModel):
    nombre: str
    modalidad: Literal["texto", "imagen", "video", "lidar"]
    taxonomia: list[EtiquetaIn] = Field(min_length=1)
    sla_horas: int = Field(gt=0)
    reglas: dict = {}
    cliente_id: Optional[str] = None
    cliente_nombre: Optional[str] = None


class ArchivoIn(BaseModel):
    uri: str
    sha256: str
    bytes: int = Field(ge=0)
    mime: str
    meta: dict = {}


class TareaIn(BaseModel):
    archivo: ArchivoIn
    tipo_tarea: Optional[str] = None
    prioridad: Optional[int] = Field(default=None, ge=1, le=5)


class LoteIn(BaseModel):
    nombre: Optional[str] = None
    prioridad: int = Field(default=3, ge=1, le=5)
    tareas: list[TareaIn] = Field(min_length=1, max_length=5000)


class AsignarIn(BaseModel):
    anotador_id: str
    proyecto_id: Optional[str] = None
    solo_reanudar: bool = False  # recuperar: no tomar tarea nueva


class TransicionIn(BaseModel):
    accion: Literal["enviar", "liberar", "iniciar_revision", "aprobar", "rechazar", "renovar", "confirmar_envio"]
    anotador_id: Optional[str] = None
    token: Optional[str] = None
    anotacion: Optional[dict] = None
    revisor_id: Optional[str] = None
    motivo_codigo: Optional[str] = None


# ---------------------------------------------------------------- rutas
@app.post("/proyectos", status_code=201)
def crear_proyecto(p: ProyectoIn):
    now = ahora()
    cid = p.cliente_id or str(uuid.uuid4())
    db.clientes.update_one({"_id": cid}, {"$setOnInsert": {
        "nombre": p.cliente_nombre or f"cliente-{cid[:8]}", "creado_en": now}}, upsert=True)
    doc = {"_id": str(uuid.uuid4()), "cliente_id": cid, "nombre": p.nombre, "modalidad": p.modalidad,
           "taxonomia": [e.model_dump() for e in p.taxonomia], "reglas": p.reglas,
           "sla_horas": p.sla_horas, "estado": "activo", "creado_en": now}
    db.proyectos.insert_one(doc)
    return ser(doc)


@app.post("/proyectos/{proyecto_id}/lotes", status_code=201)
def crear_lote(proyecto_id: str, lote: LoteIn):
    proy = db.proyectos.find_one({"_id": proyecto_id})
    if not proy:
        raise HTTPException(404, "proyecto_no_existe")
    if proy["estado"] != "activo":
        raise HTTPException(409, "proyecto_cerrado")
    now = ahora()
    batch_id = str(uuid.uuid4())
    por_defecto = MODALIDAD_TIPO[proy["modalidad"]]
    tareas = []
    for t in lote.tareas:
        tipo = t.tipo_tarea or por_defecto
        if tipo not in TIPOS_VALIDOS:
            raise HTTPException(422, f"tipo_tarea invalido: {tipo}")
        tareas.append({
            "_id": str(uuid.uuid4()), "proyecto_id": proyecto_id, "cliente_id": proy["cliente_id"],
            "batch_id": batch_id, "modalidad": proy["modalidad"], "tipo_tarea": tipo,
            "prioridad": t.prioridad or lote.prioridad, "archivo": t.archivo.model_dump(),
            "estado": "disponible", "asignacion": None, "anotacion": None, "ultima_revision": None,
            "sla_horas": proy["sla_horas"],  # desnormalizado para el evento
            "version": 0, "ultimo_evento_id": None, "creada_en": now, "actualizada_en": now,
        })
    db.batches.insert_one({"_id": batch_id, "proyecto_id": proyecto_id, "nombre": lote.nombre,
                           "n_tareas": len(tareas), "creado_en": now})
    db.tareas.insert_many(tareas, ordered=False)
    return {"batch_id": batch_id, "proyecto_id": proyecto_id, "n_tareas": len(tareas),
            "tarea_ids": [t["_id"] for t in tareas]}


def _equipo(anotador_id: str) -> Optional[str]:
    a = db.anotadores.find_one({"_id": anotador_id}, {"equipo_id": 1})
    return a.get("equipo_id") if a else None


@app.post("/tareas/asignar")
def asignar(req: AsignarIn):
    now = ahora()
    # 1) ¿ya tiene una tarea abierta vigente? (índice anotador+estado) -> reanudar
    previa = db.tareas.find_one({"asignacion.anotador_id": req.anotador_id, "estado": "abierta",
                                 "asignacion.expira": {"$gt": now}})
    if previa:
        return {"tarea": ser(previa), "evento": None, "reanudada": True}
    if req.solo_reanudar:
        return Response(status_code=204)
    # 2) asignación atómica (findOneAndUpdate): disponible o abierta con plazo vencido
    filtro: dict = {"$or": [{"estado": "disponible"},
                            {"estado": "abierta", "asignacion.expira": {"$lt": now}}]}
    if req.proyecto_id:
        filtro["proyecto_id"] = req.proyecto_id
    token = secrets.token_hex(16)
    event_id = str(uuid.uuid4())
    asignacion = {"anotador_id": req.anotador_id, "equipo_id": _equipo(req.anotador_id),
                  "token": token, "tomada_en": now,
                  "expira": now + timedelta(milliseconds=config.LOCK_TTL_MS)}
    tarea = db.tareas.find_one_and_update(
        filtro,
        {"$set": {"estado": "abierta", "asignacion": asignacion, "anotacion": None,
                  "ultimo_evento_id": event_id, "actualizada_en": now},
         "$inc": {"version": 1}},
        sort=[("prioridad", DESCENDING)], return_document=ReturnDocument.AFTER)
    if not tarea:
        return Response(status_code=204)
    return {"tarea": ser(tarea), "evento": construir_evento(tarea, "abierta", event_id, now),
            "reanudada": False}


def _transicion(tarea_id: str, filtro: dict, set_: dict, estado_evento: str, *,
                nuevo_estado: str, motivo=None, revisor_id=None, limpiar_asignacion=False):
    now = ahora()
    event_id = str(uuid.uuid4())
    set_ = {**set_, "estado": nuevo_estado, "ultimo_evento_id": event_id, "actualizada_en": now}
    if limpiar_asignacion:
        set_["asignacion"] = None
    antes = db.tareas.find_one_and_update(
        {"_id": tarea_id, **filtro}, {"$set": set_, "$inc": {"version": 1}},
        return_document=ReturnDocument.BEFORE)
    if not antes:
        existe = db.tareas.find_one({"_id": tarea_id}, {"estado": 1})
        if not existe:
            raise HTTPException(404, "tarea_no_existe")
        raise HTTPException(409, {"error": "conflicto", "estado_actual": existe["estado"]})
    despues = {**antes, **set_, "version": antes["version"] + 1}
    # el evento conserva la asignación vigente antes de limpiarla
    ev = construir_evento({**despues, "asignacion": antes.get("asignacion")}, estado_evento,
                          event_id, now, motivo=motivo, revisor_id=revisor_id)
    return {"tarea": ser(despues), "evento": ev}


@app.post("/tareas/{tarea_id}/transicion")
def transicion(tarea_id: str, req: TransicionIn):
    a = req.accion
    now = ahora()
    if a in ("enviar", "liberar", "renovar", "confirmar_envio"):
        if not req.token or not req.anotador_id:
            raise HTTPException(422, "token y anotador_id requeridos")
        if a == "confirmar_envio":
            # Reconciliación (P8): un enviar que dio 503 (p. ej. WriteConcernTimeout sin mayoría) pudo haberse
            # aplicado igualmente en Mongo; el reintento recibe 409 y el evento nunca llegó al stream. Este
            # método NO cambia nada: si la tarea sigue `anotada` con ese token, reconstruye el MISMO evento
            # (mismo event_id = ultimo_evento_id, mismo ts) para que el gateway lo publique; Spark deduplica.
            t = db.tareas.find_one({"_id": tarea_id, "estado": "anotada", "asignacion.token": req.token,
                                    "asignacion.anotador_id": req.anotador_id})
            if not t:
                ex = db.tareas.find_one({"_id": tarea_id}, {"estado": 1})
                if not ex:
                    raise HTTPException(404, "tarea_no_existe")
                raise HTTPException(409, {"error": "conflicto", "estado_actual": ex["estado"]})
            ev = construir_evento(t, "anotada", t["ultimo_evento_id"], t["actualizada_en"])
            return {"tarea": ser(t), "evento": ev, "reconciliado": True}
        base = {"estado": "abierta", "asignacion.token": req.token,
                "asignacion.anotador_id": req.anotador_id}
        if a == "enviar":
            if req.anotacion is None:
                raise HTTPException(422, "anotacion requerida")
            return _transicion(tarea_id, base, {"anotacion": req.anotacion}, "anotada",
                               nuevo_estado="anotada")
        if a == "liberar":
            return _transicion(tarea_id, base, {}, "disponible", nuevo_estado="disponible",
                               limpiar_asignacion=True)
        # renovar: extiende el plazo (sin cambio de estado/versión, sin evento)
        t = db.tareas.find_one_and_update(
            {"_id": tarea_id, **base},
            {"$set": {"asignacion.expira": now + timedelta(milliseconds=config.LOCK_TTL_MS)}},
            return_document=ReturnDocument.AFTER)
        if not t:
            ex = db.tareas.find_one({"_id": tarea_id}, {"estado": 1})
            if not ex:
                raise HTTPException(404, "tarea_no_existe")
            raise HTTPException(409, {"error": "conflicto", "estado_actual": ex["estado"]})
        return {"tarea": ser(t), "evento": None}
    # acciones del revisor
    if not req.revisor_id:
        raise HTTPException(422, "revisor_id requerido")
    if a == "iniciar_revision":
        return _transicion(tarea_id, {"estado": "anotada"},
                           {"ultima_revision": {"revisor_id": req.revisor_id, "resultado": "en_revision",
                                                "motivo_codigo": None, "ts": now}},
                           "en_revision", nuevo_estado="en_revision", revisor_id=req.revisor_id)
    if a == "aprobar":
        return _transicion(tarea_id, {"estado": "en_revision"},
                           {"ultima_revision": {"revisor_id": req.revisor_id, "resultado": "aprobada",
                                                "motivo_codigo": None, "ts": now}},
                           "aprobada", nuevo_estado="aprobada", revisor_id=req.revisor_id)
    if not req.motivo_codigo:  # rechazar
        raise HTTPException(422, "motivo_codigo requerido para rechazar")
    return _transicion(tarea_id, {"estado": "en_revision"},
                       {"ultima_revision": {"revisor_id": req.revisor_id, "resultado": "rechazada",
                                            "motivo_codigo": req.motivo_codigo, "ts": now}},
                       "rechazada", nuevo_estado="disponible", motivo=req.motivo_codigo,
                       revisor_id=req.revisor_id, limpiar_asignacion=True)


@app.get("/proyectos/{proyecto_id}/exportar")
def exportar(proyecto_id: str, cursor: Optional[str] = None, limite: int = Query(100, ge=1, le=1000)):
    filtro: dict = {"proyecto_id": proyecto_id, "estado": "aprobada"}
    if cursor:
        filtro["_id"] = {"$gt": cursor}
    docs = list(db.tareas.find(filtro, {"asignacion.token": 0}).sort("_id", ASCENDING).limit(limite))
    sig = docs[-1]["_id"] if len(docs) == limite else None
    return {"items": ser(docs), "siguiente_cursor": sig, "n": len(docs)}


@app.get("/salud")
def salud():
    """Siempre 200; el estado va en el cuerpo (el gateway lo interpreta)."""
    try:
        h = cliente_salud.admin.command("hello")
    except pe.PyMongoError as e:
        return {"estado": "caido", "mongo": {"estado": "caido", "detalle": type(e).__name__}}
    if not h.get("isWritablePrimary"):
        return {"estado": "caido", "mongo": {"estado": "caido", "detalle": "sin_primario"}}
    estado, detalle = "ok", f"primario={h.get('me')}"
    miembros: dict = {}
    try:
        st = cliente_salud.admin.command("replSetGetStatus")
        vivos = sum(1 for m in st["members"] if m["health"] == 1)
        miembros = {"total": len(st["members"]), "vivos": vivos}
        if vivos < len(st["members"]):
            estado, detalle = "degradado", f"{vivos}/{len(st['members'])} nodos vivos; primario={h.get('me')}"
    except pe.PyMongoError:
        pass
    return {"estado": estado, "mongo": {"estado": estado, "detalle": detalle, **miembros}}
