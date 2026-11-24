"""P4 (frescura). Segundos desde el HTTP 200 de la APROBACIÓN de una tarea hasta que `GET /analitica/q1` la
refleja en la fila (día UTC, modalidad). Q1 sólo cuenta filas `aprobada`, por eso el evento relevante es la aprobación.

Método: n muestras secuenciales (sin otros escritores). Cada muestra: siguiente -> enviar -> revision(iniciar) ->
revision(aprobar). Se anota el reloj monotónico del host en el 200 de aprobar y se consulta Q1 de forma continua
(una consulta tras otra) hasta que la fila (día, modalidad) alcanza base + k (k = aprobaciones propias hasta ahora).
Resolución: una consulta de Q1 dura ~4-6 s, así que el instante exacto de visibilidad sólo se acota entre el
inicio de la última consulta que no lo veía y el fin de la primera que sí.

Uso: python pruebas/p4_frescura_q1.py [--trials 10] [--modalidad texto]
"""
import argparse
import glob
import os
import sys
import threading
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun as C  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402


def archivos_lago():
    return set(glob.glob(C.LAGO + "/eventos/mes=*/*.parquet"))


def q1_fila(c, dia, mod):
    t_send = time.monotonic()
    r = c.get("/analitica/q1", params={"conjunto": "principal"})
    t_recv = time.monotonic()
    r.raise_for_status()
    d = r.json()
    n = next((f["aprobadas"] for f in d["filas"] if f["dia"] == dia and f["modalidad"] == mod), 0)
    return t_send, t_recv, d.get("tiempo_s"), n, d.get("n_filas")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=10)
    ap.add_argument("--modalidad", default="texto")
    a = ap.parse_args()
    ev = C.Evidencia(
        "p4_frescura_q1", "P4 (frescura). Del HTTP 200 de la aprobación a verla en Q1",
        "Medir, sin estimar, cuántos segundos pasan desde el HTTP 200 de la aprobación de una tarea hasta que Q1 (aprobadas por día UTC y modalidad, "
        "sobre Parquet) la cuenta, que es la frescura del lago hasta un resultado de Q1.",
        {"muestras": a.trials, "modalidad": a.modalidad, "flujo_por_muestra": "siguiente, enviar, revision iniciar, revision aprobar",
         "deteccion": "Q1 por el gateway en bucle continuo hasta que la fila (dia UTC, modalidad) alcanza base + k aprobaciones propias",
         "reloj": "time.monotonic() del host", "lago_host": C.LAGO, "escritores_concurrentes": "ninguno (prueba ejecutada sola, stack en reposo)"})
    c = C.cliente(120)
    C.esperar_todo_ok(120)
    est = C.interno("historial", "GET", "/estado", 8003)[1]
    ev.paso("Estado del historial al inicio", {k: est.get(k) for k in ("xlen", "lag", "pendientes", "lotes", "eventos_escritos", "archivos_escritos")})
    p, _ = C.crear_proyecto(c, f"p4-frescura-{int(time.time())}", a.modalidad, 24, a.trials + 3)
    rev = "p4-frescura-rev"
    for _ in range(2):  # calentar Q1
        q1_fila(c, "0000-00-00", a.modalidad)
    dia0 = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = q1_fila(c, dia0, a.modalidad)[3]
    ev.paso("Línea base de Q1", {"dia_utc": dia0, "modalidad": a.modalidad, "aprobadas_base": base})
    muestras, esperado = [], base
    for k in range(a.trials):
        anot = f"p4-frescura-anot-{k}"
        d = c.post("/tareas/siguiente", json={"anotador_id": anot, "proyecto_id": p["_id"]}).json()
        tid = d["tarea"]["_id"]
        c.post(f"/tareas/{tid}/enviar", json={"anotador_id": anot, "token": d["token"], "anotacion": {"spans": []}}).raise_for_status()
        c.post(f"/tareas/{tid}/revision", json={"revisor_id": rev, "accion": "iniciar"}).raise_for_status()
        time.sleep(2.5)  # enviar/iniciar ya escritos: el archivo nuevo es el de la aprobación
        antes = archivos_lago()
        r = c.post(f"/tareas/{tid}/revision", json={"revisor_id": rev, "accion": "aprobar"})
        t200 = time.monotonic()
        t200_utc = datetime.now(timezone.utc)
        r.raise_for_status()
        dia = t200_utc.strftime("%Y-%m-%d")
        if dia != dia0:  # cambio de día UTC: nueva base
            dia0, esperado = dia, q1_fila(c, dia, a.modalidad)[3]
        esperado += 1
        res = {"t_file": None, "archivo": None}

        def vigilar():
            fin = time.monotonic() + 60
            while time.monotonic() < fin:
                for f in archivos_lago() - antes:
                    try:
                        filas = pq.read_table(f, columns=["estado"]).column("estado").to_pylist()
                        if "aprobada" in filas:
                            res["t_file"], res["archivo"] = time.monotonic(), os.path.relpath(f, C.LAGO).replace("\\", "/")
                            return
                    except Exception:
                        pass
                time.sleep(0.05)
        th = threading.Thread(target=vigilar)
        th.start()
        polls = []
        while True:
            ts_, tr_, tq, n, nf = q1_fila(c, dia, a.modalidad)
            polls.append({"inicio_s": round(ts_ - t200, 2), "fin_s": round(tr_ - t200, 2), "tiempo_s_q1": tq, "aprobadas": n})
            if n >= esperado or tr_ - t200 > 120:
                break
        th.join()
        vis = n >= esperado
        m = {"muestra": k + 1, "tarea_id": tid[:8], "http_aprobar": r.status_code, "dia_utc": dia,
             "aprobadas_esperadas": esperado, "aprobadas_vistas": n, "visible": vis,
             "s_hasta_archivo_en_disco": round(res["t_file"] - t200, 2) if res["t_file"] else None,
             "s_hasta_q1_visible": round(tr_ - t200, 2) if vis else None,
             "s_inicio_de_la_consulta_que_lo_vio": polls[-1]["inicio_s"], "consultas_q1": len(polls),
             "tiempo_s_q1_ultima": polls[-1]["tiempo_s_q1"], "polls": polls, "archivo": res["archivo"]}
        muestras.append(m)
        print({x: m[x] for x in m if x != "polls"}, flush=True)
        if not vis:
            esperado = n  # no arrastrar el desfase
        time.sleep(1.0)
    ok = [m for m in muestras if m["visible"]]
    vis = [m["s_hasta_q1_visible"] for m in ok]
    ini = [m["s_inicio_de_la_consulta_que_lo_vio"] for m in ok]
    fil = [m["s_hasta_archivo_en_disco"] for m in ok if m["s_hasta_archivo_en_disco"] is not None]
    tq = [x["tiempo_s_q1"] for m in muestras for x in m["polls"] if x["tiempo_s_q1"]]
    nq = [m["consultas_q1"] for m in muestras]

    def s(xs):
        return {"n": len(xs), "min": min(xs), "p50": C.pct(xs, .5), "p95": C.pct(xs, .95), "p99": C.pct(xs, .99), "max": max(xs)} if xs else None
    res = {"n": len(muestras), "visibles": len(ok), "q1_visible_desde_http200_s": s(vis), "inicio_consulta_que_lo_vio_s": s(ini),
           "archivo_en_disco_desde_http200_s": s(fil), "tiempo_s_q1_por_consulta": s(tq), "consultas_q1_por_muestra": s(nq),
           "resolucion_s": f"una consulta de Q1 (p50 {C.pct(tq, .5)} s, max {max(tq) if tq else None} s)"}
    ev.paso("Resultado por muestra", {"resumen": res, "muestras": muestras})
    ev.resultado(**res)
    ev.cerrar("La frescura hasta Q1 es el tiempo entre el 200 de la aprobación y el fin de la primera consulta de Q1 que cuenta esa aprobación. Incluye el micro-lote "
              "(T=2 s, el archivo aparece en disco en ~2 s), la espera hasta que empieza la siguiente consulta y la propia ejecución de Q1 (~4-6 s). "
              "Como se consulta en bucle, el instante exacto de visibilidad sólo se acota entre el inicio y el fin de la consulta que la vio "
              "('inicio_consulta_que_lo_vio_s' y 'q1_visible_desde_http200_s'): la resolución es la duración de una consulta de Q1.")


if __name__ == "__main__":
    main()
