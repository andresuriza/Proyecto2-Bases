"""Dataset de validación pequeño y hecho a mano (para P5): eventos + respuestas esperadas.

Genera `datalake_validacion/eventos/mes=AAAA-MM/part-val-*.parquet` y
`datalake_validacion/esperado.json` con Q1-Q5 calculadas con lógica Python pura (sin Spark),
siguiendo las definiciones de CONTRATOS.md. Incluye un archivo con eventos DUPLICADOS (mismo
event_id) para comprobar la deduplicación, tomas fuera de la ventana de 90 días de Q1,
un incumplimiento de SLA, rechazos con re-toma y liberaciones.

Uso: python dataset/generar_validacion.py --out datalake_validacion
"""
import argparse
import json
import math
import os
import uuid
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

TS = pa.timestamp("ms", tz="UTC")
SCHEMA = pa.schema([
    ("event_id", pa.string()), ("ts", TS), ("tarea_id", pa.string()), ("proyecto_id", pa.string()),
    ("cliente_id", pa.string()), ("batch_id", pa.string()), ("equipo_id", pa.string()),
    ("anotador_id", pa.string()), ("revisor_id", pa.string()), ("modalidad", pa.string()),
    ("tipo_tarea", pa.string()), ("estado", pa.string()), ("motivo", pa.string()),
    ("version", pa.int32()), ("tomada_en", TS), ("sla_horas", pa.int32()),
])
PROY = {  # id: (cliente, modalidad, tipo_por_defecto, sla)
    "P1": ("C1", "texto", "span", 24),
    "P2": ("C1", "lidar", "cuboide_3d", 48),
    "P3": ("C2", "imagen", "bbox", 24),
}


def T(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)


def m(x):
    return timedelta(minutes=x)


# (tarea, proyecto, equipo, tipo|None, [tomas]) ; toma = (tomada_en, min_anotar, min_espera_revision,
#  min_revision, resultado[aprobada|rechazada|None=sin revisar], motivo)
TAREAS = [
    ("t01", "P1", "E1", None, [("2026-10-28T08:00", 10, 60, 5, "aprobada", None)]),
    ("t02", "P1", "E1", None, [("2026-10-29T09:00", 20, 120, 10, "aprobada", None)]),
    ("t03", "P1", "E2", None, [("2026-10-29T10:00", 30, 90, 10, "rechazada", "M01"),
                              ("2026-10-29T15:00", 25, 60, 5, "aprobada", None)]),
    ("t04", "P2", "E1", None, [("2026-10-25T07:00", 55, 1500, 10, "aprobada", None)]),
    ("t05", "P2", "E2", None, [("2026-10-20T07:00", 70, 2900, 10, "aprobada", None)]),   # incumple SLA 48 h
    ("t06", "P2", "E2", None, [("2026-10-27T07:00", 80, 300, 10, "rechazada", "M02")]),
    ("t07", "P3", "E1", None, [("2026-08-01T05:00", 8, 30, 5, "aprobada", None)]),   # fuera de 90 días
    ("t08", "P3", "E2", None, [("2026-08-01T12:30", 9, 30, 5, "aprobada", None)]),   # dentro (borde)
    ("t09", "P3", "E1", None, [("2026-10-30T09:00", 12, 45, 5, "aprobada", None)]),  # fija max(ts)
    ("t10", "P3", "E1", "poligono", [("2026-09-15T10:00", 15, 40, 5, "aprobada", None)]),
    ("t11", "P3", "E2", "poligono", [("2026-09-16T10:00", 18, 50, 5, "rechazada", "M01")]),
    ("t12", "P1", "E1", None, [("2026-09-20T08:00", 20, 3000, 10, "aprobada", None)]),  # incumple SLA 24 h
    ("t13", "P1", "E2", None, [("2026-10-01T10:00", None, None, None, "disponible", None)]),
    ("t14", "P2", "E1", None, [("2026-10-30T08:00", 40, None, None, None, None)]),     # sólo abierta+anotada
]


def construir():
    eventos = []
    for tid, p, eq, tipo, tomas in TAREAS:
        cli, mod, tipo_def, sla = PROY[p]
        ver = 0

        def ev(ts, estado, tom, rev=None, motivo=None):
            nonlocal ver
            ver += 1
            eventos.append({
                "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"val/{tid}/{ver}")), "ts": ts,
                "tarea_id": tid, "proyecto_id": p, "cliente_id": cli, "batch_id": f"B-{p}",
                "equipo_id": eq, "anotador_id": f"A-{eq}", "revisor_id": rev, "modalidad": mod,
                "tipo_tarea": tipo or tipo_def, "estado": estado, "motivo": motivo, "version": ver,
                "tomada_en": tom, "sla_horas": sla})
        for tomada, d_an, w, d_rv, res, motivo in tomas:
            t0 = T(tomada)
            ev(t0, "abierta", t0)
            if res == "disponible":
                ev(t0 + m(5), "disponible", t0)
                continue
            t1 = t0 + m(d_an)
            ev(t1, "anotada", t0)
            if res is None:
                continue
            t2 = t1 + m(w)
            ev(t2, "en_revision", t0, rev="R-" + eq)
            ev(t2 + m(d_rv), res, t0, rev="R-" + eq, motivo=motivo)
    return eventos


# ---------------------------------------------------------------- oráculo (Python puro)
def pct(valores, p):
    """nearest-rank: elemento ceil(p*N)-1 de la lista ordenada."""
    v = sorted(valores)
    return v[max(0, math.ceil(round(p * len(v), 9)) - 1)]


def esperado(eventos):
    dedup = {e["event_id"]: e for e in eventos}.values()
    max_ts = max(e["ts"] for e in dedup)
    corte = max_ts - timedelta(days=90)
    q1 = {}
    for e in dedup:
        if e["estado"] == "aprobada" and e["ts"] > corte:
            k = (e["ts"].strftime("%Y-%m-%d"), e["modalidad"])
            q1[k] = q1.get(k, 0) + 1
    r1 = [{"dia": d, "modalidad": mo, "aprobadas": n} for (d, mo), n in sorted(q1.items())]

    q2 = {}
    for e in dedup:
        if e["estado"] in ("aprobada", "rechazada"):
            k = (e["equipo_id"], e["ts"].strftime("%Y-%m"))
            a, r = q2.get(k, (0, 0))
            q2[k] = (a + (e["estado"] == "aprobada"), r + (e["estado"] == "rechazada"))
    r2 = [{"equipo_id": k[0], "mes": k[1], "aprobadas": a, "rechazadas": r,
           "proporcion_aprobadas": a / (a + r)} for k, (a, r) in sorted(q2.items())]

    q3 = {}
    for e in dedup:
        if e["estado"] == "anotada" and e["tomada_en"]:
            q3.setdefault((e["modalidad"], e["tipo_tarea"]), []).append((e["ts"] - e["tomada_en"]).total_seconds())
    r3 = [{"modalidad": k[0], "tipo_tarea": k[1], "n": len(v), "dur_media_s": sum(v) / len(v),
           "dur_p95_s": pct(v, .95)} for k, v in sorted(q3.items())]

    q4 = {}
    for e in dedup:
        if e["estado"] in ("aprobada", "rechazada"):
            k = (e["cliente_id"], e["proyecto_id"])
            a, r = q4.get(k, (0, 0))
            q4[k] = (a + (e["estado"] == "aprobada"), r + (e["estado"] == "rechazada"))
    r4 = [{"cliente_id": k[0], "proyecto_id": k[1], "rechazadas": r, "aprobadas": a,
           "proporcion_rechazadas": r / (a + r)} for k, (a, r) in sorted(q4.items())]

    q5 = {}
    for e in dedup:
        if e["estado"] == "aprobada" and e["tomada_en"]:
            d = (e["ts"] - e["tomada_en"]).total_seconds()
            q5.setdefault(e["cliente_id"], []).append((d, d > e["sla_horas"] * 3600))
    r5 = [{"cliente_id": k, "n": len(v), "p50_s": pct([x for x, _ in v], .5),
           "p95_s": pct([x for x, _ in v], .95),
           "pct_incumple_sla": 100.0 * sum(1 for _, b in v if b) / len(v)} for k, v in sorted(q5.items())]
    return {"max_ts": max_ts.isoformat(), "corte_q1": corte.isoformat(),
            "eventos_unicos": len(dedup), "q1": r1, "q2": r2, "q3": r3, "q4": r4, "q5": r5}


def escribir(eventos, out):
    base = os.path.join(out, "eventos")
    for dp, _, fs in os.walk(base):
        for f in fs:
            if f.startswith("part-val-"):
                os.remove(os.path.join(dp, f))
    # archivo 0: todos los eventos; archivo 1: copia de t01 (duplicados exactos del lago)
    grupos = [("0", eventos), ("1", [e for e in eventos if e["tarea_id"] == "t01"])]
    for tag, evs in grupos:
        por_mes = {}
        for e in evs:
            por_mes.setdefault(e["ts"].strftime("%Y-%m"), []).append(e)
        for mes, es in por_mes.items():
            d = os.path.join(base, f"mes={mes}")
            os.makedirs(d, exist_ok=True)
            cols = {c: [e[c] for e in es] for c in SCHEMA.names}
            pq.write_table(pa.table(cols, schema=SCHEMA), os.path.join(d, f"part-val-{tag}.parquet"),
                           coerce_timestamps="ms")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="datalake_validacion")
    a = ap.parse_args()
    evs = construir()
    escribir(evs, a.out)
    esp = esperado(evs)
    with open(os.path.join(a.out, "esperado.json"), "w", encoding="utf8") as fh:
        json.dump(esp, fh, indent=1, ensure_ascii=False)
    print(f"{len(evs)} eventos únicos (+{sum(1 for e in evs if e['tarea_id']=='t01')} duplicados) -> {a.out}")
