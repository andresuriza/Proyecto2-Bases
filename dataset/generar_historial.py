"""Generador del historial sintético de eventos (datalake Parquet).

Reproduce el ciclo de vida del enunciado:
  abierta -> anotada -> en_revision -> aprobada | rechazada
  (rechazada vuelve a la cola: nueva abierta con nueva toma, hasta 5 ciclos)
  y liberaciones ocasionales (abierta -> disponible -> abierta ...).
Los ids (clientes, proyectos, batches, equipos, anotadores, revisores) salen de
generar_registro.generar(seed): son los mismos que se cargan en Mongo; los 60 proyectos
cerrados sólo aparecen aquí. Las tareas del historial son otras (tareas ya procesadas).

Distribuciones (ver datos/README.md):
  * creación de tareas: uniforme por día con menos actividad en fin de semana, hora ~ N(14h, 3.5h);
  * duración de anotación: lognormal, mediana por modalidad (texto 4 min, imagen 8, video 25,
    lidar 55), sigma 0.6-0.8;  espera hasta revisión: lognormal (mediana 3 h; lidar 8 h);
  * revisión: lognormal mediana 8 min; probabilidad de rechazo por equipo (beta, media ~0.35,
    más alta en video/lidar);  liberación 38 % de las tomas;
  * ~10 % de los proyectos "lentos" (espera x3) y el proyecto LiDAR de ejemplo (SLA 48 h) con
    espera x2.5 => incumplimientos de SLA visibles.
Escritura en chunks (memoria acotada): un archivo por (chunk, mes) con esquema fijo.

Uso:
  python dataset/generar_historial.py --seed 42 --eventos 800000 --out datalake/eventos
  python dataset/generar_historial.py --seed 7  --eventos 10000000 --out datalake_grande/eventos
"""
import argparse
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import generar_registro as reg  # noqa: E402

TS = pa.timestamp("ms", tz="UTC")
# `mes` es la partición (directorio), no va dentro del archivo.
SCHEMA = pa.schema([
    ("event_id", pa.string()), ("ts", TS), ("tarea_id", pa.string()), ("proyecto_id", pa.string()),
    ("cliente_id", pa.string()), ("batch_id", pa.string()), ("equipo_id", pa.string()),
    ("anotador_id", pa.string()), ("revisor_id", pa.string()), ("modalidad", pa.string()),
    ("tipo_tarea", pa.string()), ("estado", pa.string()), ("motivo", pa.string()),
    ("version", pa.int32()), ("tomada_en", TS), ("sla_horas", pa.int32()),
])
COLS = SCHEMA.names
MOTIVOS = ["M01_geometria_imprecisa", "M02_etiqueta_incorrecta", "M03_objeto_omitido",
           "M04_instrucciones_no_seguidas", "M05_calidad_insuficiente"]
H = 3600_000
MED_ANOT = {"texto": 4 * 60e3, "imagen": 8 * 60e3, "video": 25 * 60e3, "lidar": 55 * 60e3}
SIG_ANOT = {"texto": .6, "imagen": .6, "video": .7, "lidar": .8}
MED_ESPERA = {"texto": 2.5 * H, "imagen": 3 * H, "video": 4 * H, "lidar": 8 * H}
P_REJ_MOD = {"texto": .8, "imagen": 1.0, "video": 1.15, "lidar": 1.3}
P_LIB, MAX_CICLOS = 0.38, 5


def ms(dt):
    return int(dt.timestamp() * 1000)


def uuids(rng_np, n):
    """n UUID v4 deterministas (a partir del generador numpy)."""
    raw = bytearray(rng_np.bytes(16 * n))
    out = []
    for i in range(n):
        b = bytearray(raw[16 * i:16 * i + 16])
        b[6] = (b[6] & 0x0F) | 0x40
        b[8] = (b[8] & 0x3F) | 0x80
        out.append(str(uuid.UUID(bytes=bytes(b))))
    return out


def preparar(seed):
    d = reg.generar(seed)
    rng = random.Random(seed + 1)
    equipos = d["equipos"]
    por_eq = {e: {"anot": [], "rev": []} for e in equipos}
    for a in d["anotadores"]:
        por_eq[a["equipo_id"]]["rev" if a["rol"] == "revisor" else "anot"].append(a["_id"])
    revisores = [a for a in d["anotadores"] if a["rol"] == "revisor"]
    p_rej_equipo = {e: min(.7, max(.03, rng.betavariate(3.5, 6.5))) for e in equipos}  # media 0.35
    batches_por_proy = {}
    for b in d["batches"]:
        batches_por_proy.setdefault(b["proyecto_id"], []).append(b["_id"])
    proys = []
    for p in d["proyectos"]:
        ini = datetime.strptime(p["creado_en"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        fin = (datetime.strptime(p["cerrado_en"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
               if p["estado"] == "cerrado" else reg.FIN)
        lento = 3.0 if rng.random() < .10 else 1.0
        if p.get("ejemplo_lidar"):
            lento = 2.5
        bl = batches_por_proy.get(p["_id"]) or [str(uuid.UUID(int=rng.getrandbits(128), version=4))
                                                 for _ in range(3)]  # lotes históricos de cerrados
        proys.append({"id": p["_id"], "cliente": p["cliente_id"], "mod": p["modalidad"],
                      "tipo": p["tipo_tarea"], "sla": p["sla_horas"], "ini": ms(ini), "fin": ms(fin),
                      "lento": lento, "batches": bl, "peso": rng.lognormvariate(0, .6)})
    return d, por_eq, revisores, p_rej_equipo, proys


def generar_chunk(rng, rn, ntareas, ctx, fin_ms):
    """Simula `ntareas` tareas; devuelve columnas (listas) de sus eventos con ts <= fin_ms."""
    d, por_eq, revisores, p_rej_equipo, proys = ctx
    pesos = np.array([p["peso"] for p in proys])
    pesos = pesos / pesos.sum()
    equipos = d["equipos"]
    pi = rn.choice(len(proys), size=ntareas, p=pesos)
    cols = {c: [] for c in COLS}
    ln = rng.lognormvariate

    def emit(t, tid, p, bid, estado, eq, an, rev, motivo, ver, tom):
        cols["ts"].append(t)
        cols["tarea_id"].append(tid)
        cols["proyecto_id"].append(p["id"])
        cols["cliente_id"].append(p["cliente"])
        cols["batch_id"].append(bid)
        cols["equipo_id"].append(eq)
        cols["anotador_id"].append(an)
        cols["revisor_id"].append(rev)
        cols["modalidad"].append(p["mod"])
        cols["tipo_tarea"].append(p["tipo"])
        cols["estado"].append(estado)
        cols["motivo"].append(motivo)
        cols["version"].append(ver)
        cols["tomada_en"].append(tom)
        cols["sla_horas"].append(p["sla"])

    tids = uuids(rn, ntareas)
    for k in range(ntareas):
        p = proys[pi[k]]
        mod, lento = p["mod"], p["lento"]
        # instante de creación: día laborable preferente, hora N(14, 3.5)
        while True:
            dia = rng.randrange((p["fin"] - p["ini"]) // (24 * H) + 1)
            base = p["ini"] + dia * 24 * H
            if rng.random() < (1.0 if datetime.fromtimestamp(base / 1000, timezone.utc).weekday() < 5 else .35):
                break
        t = base + int(min(max(rng.gauss(14, 3.5), 0), 23.9) * H)
        tid, bid = tids[k], rng.choice(p["batches"])
        ver, ciclo = 0, 0
        while True:
            eq = rng.choice(equipos)
            an = rng.choice(por_eq[eq]["anot"])
            # liberaciones (tomar y soltar)
            while rng.random() < P_LIB:
                ver += 1
                emit(t, tid, p, bid, "abierta", eq, an, None, None, ver, t)
                tl = t + int(ln(math.log(10 * 60e3), .7)) + 1
                ver += 1
                emit(tl, tid, p, bid, "disponible", eq, an, None, None, ver, t)
                t = tl + int(ln(math.log(60 * 60e3), 1.0)) + 1
                eq = rng.choice(equipos)
                an = rng.choice(por_eq[eq]["anot"])
            tom = t
            ver += 1
            emit(tom, tid, p, bid, "abierta", eq, an, None, None, ver, tom)
            t_an = tom + int(ln(math.log(MED_ANOT[mod]), SIG_ANOT[mod])) + 1
            ver += 1
            emit(t_an, tid, p, bid, "anotada", eq, an, None, None, ver, tom)
            rev = rng.choice(revisores)["_id"]
            t_rv = t_an + int(ln(math.log(MED_ESPERA[mod] * lento), 1.2)) + 1
            ver += 1
            emit(t_rv, tid, p, bid, "en_revision", eq, an, rev, None, ver, tom)
            t_dec = t_rv + int(ln(math.log(8 * 60e3), .7)) + 1
            prob = min(.85, p_rej_equipo[eq] * P_REJ_MOD[mod])
            rechaza = ciclo < MAX_CICLOS - 1 and rng.random() < prob
            ver += 1
            if rechaza:
                emit(t_dec, tid, p, bid, "rechazada", eq, an, rev, rng.choice(MOTIVOS), ver, tom)
                t = t_dec + int(ln(math.log(2 * H), 1.0)) + 1
                ciclo += 1
                if t > fin_ms:
                    break
                continue
            emit(t_dec, tid, p, bid, "aprobada", eq, an, rev, None, ver, tom)
            break
    n = len(cols["ts"])
    cols["event_id"] = uuids(rn, n)
    return cols


def escribir_chunk(cols, out, tag, fin_ms):
    ts = np.array(cols["ts"], dtype="int64")
    keep = ts <= fin_ms  # eventos futuros (tareas aún en curso) se descartan
    if not keep.all():
        idx = np.nonzero(keep)[0]
        cols = {c: [v[i] for i in idx] for c, v in cols.items()}
        ts = ts[keep]
    tom = cols["tomada_en"]
    arrs = {}
    for c in COLS:
        if c in ("ts", "tomada_en"):
            continue
        arrs[c] = pa.array(cols[c], type=SCHEMA.field(c).type)
    arrs["ts"] = pa.array(ts, type=pa.int64()).cast(TS)
    arrs["tomada_en"] = pa.array(tom, type=pa.int64()).cast(TS)
    tabla = pa.table({c: arrs[c] for c in COLS}, schema=SCHEMA)
    meses = ts.astype("datetime64[ms]").astype("datetime64[M]").astype(str)
    for m in sorted(set(meses)):
        sub = tabla.filter(pa.array(meses == m))
        d = os.path.join(out, f"mes={m}")
        os.makedirs(d, exist_ok=True)
        dest = os.path.join(d, f"part-gen-{tag}.parquet")
        pq.write_table(sub, dest + ".tmp", compression="snappy", coerce_timestamps="ms")
        os.replace(dest + ".tmp", dest)
    return len(ts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--eventos", type=int, default=800_000)
    ap.add_argument("--out", default="datalake/eventos")
    ap.add_argument("--chunk-tareas", type=int, default=10_000)
    a = ap.parse_args()
    t0 = time.time()
    ctx = preparar(a.seed)
    rng, rn = random.Random(a.seed + 2), np.random.default_rng(a.seed + 3)
    fin_ms = ms(reg.FIN)
    # limpia partes de ejecuciones anteriores del generador (mismo nombre => idempotente)
    if os.path.isdir(a.out):
        for dp, _, fs in os.walk(a.out):
            for f in fs:
                if f.startswith("part-gen-"):
                    os.remove(os.path.join(dp, f))
    total, tareas, chunk = 0, 0, 0
    while total < a.eventos:
        cols = generar_chunk(rng, rn, a.chunk_tareas, ctx, fin_ms)
        n = len(cols["ts"])
        restante = a.eventos - total
        if n > restante:  # recorte final: se descartan los últimos eventos en el tiempo
            orden = np.argsort(np.array(cols["ts"]))[:restante]
            cols = {c: [v[i] for i in orden] for c, v in cols.items()}
        total += escribir_chunk(cols, a.out, f"s{a.seed}-{chunk:04d}", fin_ms)
        tareas += a.chunk_tareas
        chunk += 1
        print(f"chunk {chunk}: eventos={total:,} tareas~{tareas:,} ({time.time() - t0:.0f}s)", flush=True)
    print(f"listo: {total:,} eventos, ~{tareas:,} tareas simuladas, {time.time() - t0:.0f}s -> {a.out}")


if __name__ == "__main__":
    main()
