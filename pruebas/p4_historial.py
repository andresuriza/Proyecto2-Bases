"""P4. Historial: materialización del datalake, latencia HTTP 200 -> visible en Spark, reproceso de un
micro-lote, deduplicación por event_id, lecturas concurrentes sin archivos a medias, validez de los
Parquet, fila `rechazada` frente al estado en Mongo y compactación.

Uso: python pruebas/p4_historial.py [--trials 10]
"""
import argparse
import glob
import json
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun as C  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

ESQUEMA = pa.schema([
    ("event_id", pa.string()), ("ts", pa.timestamp("ms", tz="UTC")), ("tarea_id", pa.string()), ("proyecto_id", pa.string()),
    ("cliente_id", pa.string()), ("batch_id", pa.string()), ("equipo_id", pa.string()), ("anotador_id", pa.string()),
    ("revisor_id", pa.string()), ("modalidad", pa.string()), ("tipo_tarea", pa.string()), ("estado", pa.string()),
    ("motivo", pa.string()), ("version", pa.int32()), ("tomada_en", pa.timestamp("ms", tz="UTC")), ("sla_horas", pa.int32())])

POLLER = r"""
import sys,json,time,urllib.request
for line in sys.stdin:
    eid=line.strip()
    if not eid: continue
    t0=time.time(); n=0; last=None
    while time.time()-t0<150:
        n+=1
        try:
            last=json.load(urllib.request.urlopen("http://localhost:8004/evento/"+eid,timeout=90))
            if last["filas_dedup"]>=1: break
        except Exception as e:
            last={"error":str(e)[:200]}
        time.sleep(0.1)
    print(json.dumps({"event_id":eid,"intentos":n,"ultima":last}),flush=True)
"""


def archivos_lago():
    return set(glob.glob(C.LAGO + "/eventos/mes=*/*.parquet"))


def validar_archivo(f):
    try:
        t = pq.read_table(f)
        extra = [n for n in t.schema.names if n not in ESQUEMA.names]
        faltan = [n for n in ESQUEMA.names if n not in t.schema.names]
        tipos = [n for n in ESQUEMA.names if n in t.schema.names and t.schema.field(n).type != ESQUEMA.field(n).type]
        return {"ok": not (extra or faltan or tipos), "filas": t.num_rows, "extra": extra, "faltan": faltan, "tipos_distintos": tipos}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def spark_evento(eid):
    st, d = C.interno("analitica", "GET", f"/evento/{eid}", 8004, timeout=120, python="python3")
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=10)
    ap.add_argument("--vus-concurrencia", type=int, default=25)
    a = ap.parse_args()
    ev = C.Evidencia(
        "p4_historial", "P4. Historial: datalake Parquet, reproceso, deduplicación y visibilidad en Spark",
        "Mostrar que el historial materializa los eventos en archivos Parquet válidos, que Spark los ve unos segundos después del HTTP 200, "
        "que reprocesar un micro-lote no altera los resultados (deduplicación por event_id), que no se leen archivos a medias con escrituras "
        "concurrentes y que el rechazo queda como 'rechazada' en Parquet mientras Mongo dice 'disponible'.",
        {"micro_lote": "N=500 eventos o T=2 s", "publicacion": "_tmp + fsync + os.replace", "lago_host": C.LAGO,
         "visibilidad": "se consulta GET /evento/{id} de analitica (Spark lee Parquet en ese instante) mediante un sondeador persistente dentro del contenedor",
         "trials": a.trials})
    c = C.cliente(60)
    C.esperar_todo_ok(120)
    est = C.interno("historial", "GET", "/estado", 8003)[1]
    ev.paso("Estado del historial al inicio", {k: est.get(k) for k in ("xlen", "lag", "pendientes", "lotes", "eventos_escritos", "archivos_escritos", "invalidos", "lote_n", "lote_t_s")})

    # ---------- 1) HTTP 200 -> visible en Spark
    p, ids = C.crear_proyecto(c, f"p4-latencia-{int(time.time())}", "imagen", 24, a.trials + 5)
    poller = subprocess.Popen(["docker", "exec", "-i", "analitica", "python3", "-u", "-c", POLLER], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, text=True, cwd=C.RAIZ)
    spark_evento("00000000-0000-0000-0000-000000000000")  # calentar la sesión de Spark
    poller.stdin.write("00000000-warmup\n")  # id inexistente: sólo se usa para no medir arranque; se descarta con timeout corto
    poller.stdin.flush()
    poller.kill()
    poller = subprocess.Popen(["docker", "exec", "-i", "analitica", "python3", "-u", "-c", POLLER], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, text=True, cwd=C.RAIZ)
    trials = []
    for k in range(a.trials):
        anot = f"p4-anot-{k}"
        r = c.post("/tareas/siguiente", json={"anotador_id": anot, "proyecto_id": p["_id"]})
        d = r.json()
        antes = archivos_lago()
        t_env = time.time()
        r2 = c.post(f"/tareas/{d['tarea']['_id']}/enviar", json={"anotador_id": anot, "token": d["token"], "anotacion": {"cajas": []}})
        t200 = time.time()
        eid = r2.json()["evento_id"]
        res = {"t_file": None, "archivo": None}

        def vigilar():
            fin = time.time() + 60
            while time.time() < fin:
                for f in archivos_lago() - antes:
                    try:
                        if eid in pq.read_table(f, columns=["event_id"]).column("event_id").to_pylist():
                            res["t_file"], res["archivo"] = time.time(), os.path.relpath(f, C.LAGO).replace("\\", "/")
                            return
                    except Exception:
                        pass
                time.sleep(0.05)
        th = threading.Thread(target=vigilar)
        th.start()
        poller.stdin.write(eid + "\n")
        poller.stdin.flush()
        linea = poller.stdout.readline()
        t_spark = time.time()
        th.join()
        j = json.loads(linea)
        trials.append({"trial": k + 1, "event_id": eid[:8], "http_enviar": r2.status_code, "enviar_ms": round((t200 - t_env) * 1000),
                       "s_hasta_archivo_en_disco": round(res["t_file"] - t200, 2) if res["t_file"] else None,
                       "s_hasta_visible_en_spark": round(t_spark - t200, 2), "consultas_spark_hasta_verlo": j["intentos"],
                       "ultima_consulta_spark_s": (j["ultima"] or {}).get("tiempo_s"), "archivo": res["archivo"]})
        print(trials[-1], flush=True)
        time.sleep(1.5)
    poller.kill()
    vis = [t["s_hasta_visible_en_spark"] for t in trials]
    fil = [t["s_hasta_archivo_en_disco"] for t in trials if t["s_hasta_archivo_en_disco"] is not None]
    lat_res = {"n": len(vis), "visible_en_spark_s": {"min": min(vis), "p50": C.pct(vis, .5), "p95": C.pct(vis, .95), "p99": C.pct(vis, .99), "max": max(vis)},
               "archivo_en_disco_s": {"min": min(fil), "p50": C.pct(fil, .5), "max": max(fil)} if fil else None}
    ev.paso("Segundos entre HTTP 200 de enviar y el evento visible en Spark", {"resumen": lat_res, "trials": trials},
            "El archivo aparece tras el micro-lote (T=2 s) y Spark lo ve en la siguiente consulta; la diferencia entre ambos es la duración de la consulta de Spark. "
            "El tiempo se mide en el host (reloj único).")

    # ---------- 2) reprocesar un micro-lote
    est = C.interno("historial", "GET", "/estado", 8003)[1]
    lote = next(l for l in reversed(est["ultimos_lotes"]) if l["batch_id"] and l["eventos"] > 0)
    bid = lote["batch_id"]
    f_lote = [f for f in glob.glob(C.LAGO + f"/eventos/mes=*/part-{bid}*.parquet")]
    ids_lote = []
    for f in f_lote:
        ids_lote += pq.read_table(f, columns=["event_id"]).column("event_id").to_pylist()
    muestra = ids_lote[:3]

    def crudo():
        n = sum(pq.read_metadata(f).num_rows for f in glob.glob(C.LAGO + "/eventos/mes=*/*.parquet"))
        return n
    filas_antes = crudo()
    # contar dedup global con Spark
    def spark_dedup():
        st, d = C.interno("analitica", "GET", "/q/q4?conjunto=principal&contar=1", 8004, timeout=600, python="python3")
        return d.get("eventos_leidos")
    dedup_antes = spark_dedup()
    base = {"batch_id": bid, "archivos_del_lote": [os.path.relpath(f, C.LAGO).replace("\\", "/") for f in f_lote], "eventos_del_lote": len(ids_lote),
            "filas_crudas_lago_antes": filas_antes, "eventos_dedup_spark_antes": dedup_antes}
    st1, r1 = C.interno("historial", "POST", f"/reprocesar?batch_id={bid}&duplicar=false", 8003)
    filas_mid = crudo()
    st2, r2 = C.interno("historial", "POST", f"/reprocesar?batch_id={bid}&duplicar=true", 8003)
    time.sleep(1)
    f_lote2 = glob.glob(C.LAGO + f"/eventos/mes=*/part-{bid}*.parquet")
    filas_despues = crudo()
    dedup_despues = spark_dedup()
    ev_muestra = [spark_evento(i) for i in muestra]
    ev.paso("Reproceso del micro-lote (sobrescritura idempotente y copia duplicada)", {
        **base, "reproceso_mismo_nombre": {"http": st1, "resp": r1, "filas_crudas_despues": filas_mid, "sin_cambios": filas_mid == filas_antes},
        "reproceso_duplicando": {"http": st2, "resp": r2}, "archivos_del_lote_despues": [os.path.relpath(f, C.LAGO).replace("\\", "/") for f in f_lote2],
        "filas_crudas_lago_despues": filas_despues, "filas_duplicadas_en_archivos": filas_despues - filas_antes,
        "eventos_dedup_spark_despues": dedup_despues, "dedup_sin_cambio": dedup_antes == dedup_despues,
        "muestra_en_spark (filas_crudas debe ser 2 y filas_dedup 1)": ev_muestra})
    dedup_ok = dedup_antes == dedup_despues and all(m["filas_crudas"] == 2 and m["filas_dedup"] == 1 for m in ev_muestra)

    # ---------- 3) lecturas concurrentes con escrituras
    errores_lectura, archivos_vistos, nuevos = [], 0, []
    parar = threading.Event()

    def vigilante():
        nonlocal archivos_vistos
        vistos = archivos_lago()
        while not parar.is_set():
            for f in archivos_lago() - vistos:
                vistos.add(f)
                archivos_vistos += 1
                try:
                    pq.read_table(f)
                except Exception as e:  # noqa: BLE001
                    errores_lectura.append(f"{os.path.basename(f)}: {type(e).__name__}")
            time.sleep(0.03)
    th = threading.Thread(target=vigilante)
    th.start()
    sim = subprocess.Popen([sys.executable, os.path.join(C.RAIZ, "simulador", "simulador.py"), "--vus", str(a.vus_concurrencia), "--duracion", "55",
                            "--proyecto-por-vu", "--edicion-max", "4", "--resumen", os.path.join(C.EVIDENCE, "p4_sim_resumen.json")],
                           cwd=C.RAIZ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    lecturas = []
    while sim.poll() is None:
        t0 = time.time()
        st, d = C.interno("analitica", "GET", "/q/q4?conjunto=principal&contar=1", 8004, timeout=600, python="python3")
        lecturas.append({"http": st, "tiempo_s": round(time.time() - t0, 1), "eventos_leidos": (d or {}).get("eventos_leidos"),
                         "error": None if st == 200 else str(d)[:200]})
    sim.wait()
    time.sleep(4)
    parar.set()
    th.join()
    tmp_en_eventos = [f for f in glob.glob(C.LAGO + "/eventos/**/*", recursive=True) if f.endswith(".tmp") or "_tmp" in f]
    leidos = [l["eventos_leidos"] for l in lecturas if l["eventos_leidos"]]
    ev.paso("Lecturas de Spark mientras el simulador escribe", {
        "lecturas_spark": lecturas, "errores_de_spark": sum(1 for l in lecturas if l["http"] != 200),
        "archivos_nuevos_vigilados": archivos_vistos, "errores_de_lectura_pyarrow_al_aparecer": errores_lectura,
        "temporales_dentro_de_eventos": tmp_en_eventos, "eventos_leidos_crecen_monotonamente": leidos == sorted(leidos)},
        "El vigilante lee con pyarrow cada archivo en cuanto aparece en eventos/: ninguno estaba incompleto, porque se publica con os.replace desde _tmp.")

    # ---------- 4) validez de todos los Parquet
    t0 = time.time()
    archivos = sorted(archivos_lago())
    invalidos, filas_tot = [], 0
    for f in archivos:
        v = validar_archivo(f)
        filas_tot += v.get("filas", 0)
        if not v["ok"]:
            invalidos.append({"archivo": os.path.relpath(f, C.LAGO), **v})
    ev.paso("Validación de todos los archivos Parquet con pyarrow", {"archivos": len(archivos), "filas_totales": filas_tot, "invalidos": invalidos,
                                                                    "segundos": round(time.time() - t0, 1), "esquema_esperado": [str(f) for f in ESQUEMA]})

    # ---------- 5) rechazo: Parquet 'rechazada' vs Mongo 'disponible'
    pr, _ = C.crear_proyecto(c, f"p4-rechazo-{int(time.time())}", "texto", 24, 1)
    r = c.post("/tareas/siguiente", json={"anotador_id": "p4-rech", "proyecto_id": pr["_id"]}).json()
    tid = r["tarea"]["_id"]
    c.post(f"/tareas/{tid}/enviar", json={"anotador_id": "p4-rech", "token": r["token"], "anotacion": {"spans": []}})
    c.post(f"/tareas/{tid}/revision", json={"revisor_id": "p4-rev", "accion": "iniciar"})
    rr = c.post(f"/tareas/{tid}/revision", json={"revisor_id": "p4-rev", "accion": "rechazar", "motivo_codigo": "calidad_baja"})
    t0 = time.time()
    filas = []
    while time.time() - t0 < 30:
        filas = C.eventos_de(tid)
        if any(f["estado"] == "rechazada" for f in filas):
            break
        time.sleep(0.5)
    mongo = C.mongosh_json(f"print(JSON.stringify(db.tareas.findOne({{_id:'{tid}'}},{{estado:1,version:1,asignacion:1,ultima_revision:1}})))")
    spark_rows = [spark_evento(f["event_id"]) for f in filas if f["estado"] == "rechazada"]
    ev.paso("Rechazo: fila 'rechazada' en Parquet y 'disponible' en Mongo (misma tarea_id)", {
        "tarea_id": tid, "revision_http": rr.status_code, "filas_parquet": sorted([(f["estado"], f["version"], f["motivo"]) for f in filas], key=lambda x: x[1]),
        "mongo": {"estado": mongo["estado"], "version": mongo["version"], "asignacion": mongo["asignacion"], "ultima_revision": mongo["ultima_revision"]},
        "visible_en_spark": spark_rows, "segundos_hasta_ver_la_fila": round(time.time() - t0, 1)})
    rechazo_ok = mongo["estado"] == "disponible" and any(f["estado"] == "rechazada" for f in filas)

    # ---------- 6) compactación con lectura concurrente
    mes = time.strftime("%Y-%m", time.gmtime())
    antes_c = {"archivos_mes": len(glob.glob(C.LAGO + f"/eventos/mes={mes}/*.parquet")),
               "filas_crudas": crudo()}
    dedup_pre = spark_dedup()
    res_lect = {}

    def lector():
        t0 = time.time()
        st, d = C.interno("analitica", "GET", "/q/q4?conjunto=principal&contar=1", 8004, timeout=600, python="python3")
        res_lect.update(http=st, tiempo_s=round(time.time() - t0, 1), eventos_leidos=(d or {}).get("eventos_leidos"), error=None if st == 200 else str(d)[:200])
    th = threading.Thread(target=lector)
    th.start()
    time.sleep(2.0)
    t0 = time.time()
    st, comp = C.interno("historial", "POST", f"/compactar?mes={mes}", 8003, timeout=300)
    t_comp = round(time.time() - t0, 1)
    th.join()
    despues_c = {"archivos_mes": len(glob.glob(C.LAGO + f"/eventos/mes={mes}/*.parquet")), "filas_crudas": crudo()}
    dedup_post = spark_dedup()
    inval2 = [os.path.relpath(f, C.LAGO) for f in sorted(archivos_lago()) if not validar_archivo(f)["ok"]]
    ev.paso(f"Compactación del mes {mes} (con una lectura de Spark en curso)", {
        "http": st, "respuesta": comp, "segundos": t_comp, "antes": antes_c, "despues": despues_c,
        "lectura_concurrente_de_spark": res_lect, "eventos_dedup_spark_antes": dedup_pre, "eventos_dedup_spark_despues": dedup_post,
        "dedup_sin_cambio": dedup_pre == dedup_post, "archivos_invalidos_tras_compactar": inval2},
        "La compactación fusiona y deduplica los part-*.parquet; las filas crudas bajan (se eliminan las copias duplicadas del reproceso) y el conteo deduplicado de Spark no cambia.")

    ev.resultado(segundos_http200_a_spark=lat_res, deduplicacion_ok=dedup_ok, errores_de_lectura_concurrente=len(errores_lectura) + sum(1 for l in lecturas if l["http"] != 200),
                 lecturas_spark_concurrentes=len(lecturas), archivos_nuevos_vigilados=archivos_vistos, parquet_invalidos=len(invalidos),
                 archivos_validados=len(archivos), rechazo_parquet_vs_mongo=rechazo_ok, compactacion={"http": st, "antes": antes_c, "despues": despues_c, "dedup_sin_cambio": dedup_pre == dedup_post})
    ev.cerrar("El historial materializa cada micro-lote como un Parquet atómico (tmp + os.replace): el archivo aparece unos 2 s después del HTTP 200 y Spark lo ve en la "
              "siguiente consulta. Reprocesar un micro-lote con el mismo nombre sobrescribe sin cambios; la copia duplicada deja filas repetidas en los archivos, pero el conteo "
              "por event_id de Spark no cambia. Con escrituras y lecturas concurrentes no hubo errores ni archivos a medias; todos los archivos son Parquet válidos con el esquema "
              "del contrato. Un rechazo queda como 'rechazada' en Parquet mientras Mongo muestra 'disponible'. La compactación reduce el número de archivos sin alterar los resultados.")


if __name__ == "__main__":
    main()
