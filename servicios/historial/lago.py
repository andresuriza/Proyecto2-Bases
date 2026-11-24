"""Escritura atómica del datalake Parquet (sin dependencias de Valkey ni FastAPI).

Esquema fijo de CONTRATOS.md. La columna `mes` NO se guarda dentro del archivo:
es la columna de partición (directorio `mes=AAAA-MM`). Así Spark no ve la
columna duplicada ni falla por conflicto con la partición.

Publicación: se escribe en `<raiz>/_tmp/`, se hace fsync del archivo y se
publica con `os.replace` (rename atómico en el mismo sistema de archivos) en
`<raiz>/eventos/mes=AAAA-MM/part-<batch_id>.parquet`. Spark ignora los
archivos/directorios que empiezan con `_` o `.` y `_tmp` ni siquiera está bajo
`eventos/`, por lo que nunca puede leer un archivo a medias.
"""
import hashlib
import json
import os
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

TS = pa.timestamp("ms", tz="UTC")
SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("ts", TS),
    ("tarea_id", pa.string()),
    ("proyecto_id", pa.string()),
    ("cliente_id", pa.string()),
    ("batch_id", pa.string()),
    ("equipo_id", pa.string()),
    ("anotador_id", pa.string()),
    ("revisor_id", pa.string()),
    ("modalidad", pa.string()),
    ("tipo_tarea", pa.string()),
    ("estado", pa.string()),
    ("motivo", pa.string()),
    ("version", pa.int32()),
    ("tomada_en", TS),
    ("sla_horas", pa.int32()),
])
COLUMNAS = SCHEMA.names
# "liberada" es el nombre anterior de "disponible" (liberar); se sigue aceptando por
# compatibilidad con eventos que aún estén en el stream o en un lago ya generado.
ESTADOS = {"abierta", "anotada", "en_revision", "aprobada", "rechazada", "disponible", "liberada"}


def parse_ts(v):
    """ISO-8601 (con 'Z'), epoch ms (int) o datetime -> datetime UTC; None se conserva."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
    return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(timezone.utc)


def validar(ev: dict) -> dict:
    """Normaliza un evento; lanza ValueError si no cumple el contrato."""
    for k in ("event_id", "ts", "tarea_id", "proyecto_id", "estado", "modalidad", "tipo_tarea"):
        if not ev.get(k):
            raise ValueError(f"falta campo obligatorio {k}")
    if ev["estado"] not in ESTADOS:
        raise ValueError(f"estado inválido {ev['estado']}")
    out = {c: ev.get(c) for c in COLUMNAS}
    out["ts"] = parse_ts(ev["ts"])
    out["tomada_en"] = parse_ts(ev.get("tomada_en"))
    out["version"] = int(ev.get("version") or 0)
    out["sla_horas"] = int(ev.get("sla_horas") or 0)
    return out


def mes_de(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def a_tabla(filas: list) -> pa.Table:
    cols = {c: [f[c] for f in filas] for c in COLUMNAS}
    return pa.table(cols, schema=SCHEMA)


def _fsync_dir(d: str) -> None:
    try:
        fd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:  # sistemas de archivos que no lo permiten
        pass


def escribir_atomico(raiz: str, tabla: pa.Table, mes: str, nombre: str) -> str:
    """Escribe tabla a _tmp, fsync, os.replace a eventos/mes=<mes>/<nombre>. Devuelve ruta final.
    Idempotente: mismo `nombre` => el archivo se reemplaza por el mismo contenido."""
    tmp_dir = os.path.join(raiz, "_tmp")
    destino_dir = os.path.join(raiz, "eventos", f"mes={mes}")
    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(destino_dir, exist_ok=True)
    tmp = os.path.join(tmp_dir, f"{nombre}.{mes}.{os.getpid()}.tmp")
    pq.write_table(tabla, tmp, compression="snappy", coerce_timestamps="ms",
                   allow_truncated_timestamps=True, use_deprecated_int96_timestamps=False)
    fd = os.open(tmp, os.O_RDWR)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    destino = os.path.join(destino_dir, nombre)
    os.replace(tmp, destino)
    _fsync_dir(destino_dir)
    return destino


def id_lote(primero: str, ultimo: str) -> str:
    """Determinista a partir de los ids del stream: mismo micro-lote => mismo nombre."""
    return f"{primero}_{ultimo}"


def publicar_lote(raiz: str, batch_id: str, filas: list, sufijo: str = "") -> list:
    """Divide por mes (UTC de ts) y publica un archivo por mes. Devuelve rutas."""
    por_mes = {}
    for f in filas:
        por_mes.setdefault(mes_de(f["ts"]), []).append(f)
    rutas = []
    for mes, fs in sorted(por_mes.items()):
        rutas.append(escribir_atomico(raiz, a_tabla(fs), mes, f"part-{batch_id}{sufijo}.parquet"))
    return rutas


def guardar_replay(raiz: str, batch_id: str, entradas: list, conservar: int = 50) -> None:
    """Copia cruda del micro-lote (id de stream, JSON) para poder reprocesarlo."""
    d = os.path.join(raiz, "_replay")
    os.makedirs(d, exist_ok=True)
    ruta = os.path.join(d, f"{batch_id}.jsonl")
    with open(ruta + ".tmp", "w", encoding="utf8") as fh:
        for sid, js in entradas:
            fh.write(json.dumps({"id": sid, "evento": js}) + "\n")
    os.replace(ruta + ".tmp", ruta)
    archivos = sorted((os.path.getmtime(os.path.join(d, x)), x) for x in os.listdir(d) if x.endswith(".jsonl"))
    for _, x in archivos[:-conservar]:
        try:
            os.remove(os.path.join(d, x))
        except OSError:
            pass


def leer_replay(raiz: str, batch_id: str) -> list:
    ruta = os.path.join(raiz, "_replay", f"{batch_id}.jsonl")
    with open(ruta, encoding="utf8") as fh:
        return [(j["id"], j["evento"]) for j in map(json.loads, fh)]


def compactar_mes(raiz: str, mes: str, minimo: int = 2) -> dict:
    """Fusiona los `part-*.parquet` de un mes en un único archivo deduplicado por event_id.

    Orden seguro frente a lectores concurrentes:
      1. se listan los archivos origen (los que lleguen después no se tocan);
      2. se escribe el compactado (nombre determinista = hash de los orígenes) de forma
         atómica; durante esa ventana los datos están duplicados (origen + compactado),
         lo que es inocuo porque Spark hace dropDuplicates(event_id);
      3. sólo entonces se borran los orígenes. Un lector que ya listó archivos antes del
         borrado podría fallar con FileNotFound: analítica reintenta una vez.
    Nunca hay un estado intermedio con filas faltantes."""
    d = os.path.join(raiz, "eventos", f"mes={mes}")
    if not os.path.isdir(d):
        return {"mes": mes, "compactado": False, "motivo": "mes inexistente"}
    origenes = sorted(f for f in os.listdir(d) if f.startswith("part-") and f.endswith(".parquet"))
    if len(origenes) < minimo:
        return {"mes": mes, "compactado": False, "motivo": f"menos de {minimo} archivos",
                "archivos": len(origenes)}
    tablas = [pq.read_table(os.path.join(d, f), schema=SCHEMA) for f in origenes]
    t = pa.concat_tables(tablas)
    n_antes = t.num_rows
    vistos, keep = set(), []
    for i, eid in enumerate(t.column("event_id").to_pylist()):
        if eid not in vistos:
            vistos.add(eid)
            keep.append(i)
    t = t.take(pa.array(keep, type=pa.int64()))
    t = t.sort_by("ts")  # mejora el pruning por estadísticas
    h = hashlib.sha1("|".join(origenes).encode()).hexdigest()[:12]
    nombre = f"part-compact-{h}.parquet"
    escribir_atomico(raiz, t, mes, nombre)
    borrados = 0
    for f in origenes:
        if f != nombre:
            try:
                os.remove(os.path.join(d, f))
                borrados += 1
            except OSError:
                pass
    _fsync_dir(d)
    return {"mes": mes, "compactado": True, "archivo": nombre, "origenes": len(origenes),
            "filas_antes": n_antes, "filas_despues": t.num_rows,
            "duplicados_eliminados": n_antes - t.num_rows, "borrados": borrados}


def contar_archivos(raiz: str) -> dict:
    base = os.path.join(raiz, "eventos")
    res = {}
    if os.path.isdir(base):
        for m in sorted(os.listdir(base)):
            p = os.path.join(base, m)
            if os.path.isdir(p):
                res[m.replace("mes=", "")] = sum(1 for f in os.listdir(p) if f.endswith(".parquet"))
    return res
