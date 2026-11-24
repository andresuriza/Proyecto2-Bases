"""P2. Registro y concurrencia.

 a) rs.status() del conjunto de réplicas.
 b) Proyecto aislado con exactamente 10 tareas; 50 solicitudes /tareas/siguiente simultáneas
    (ProcessPoolExecutor, anotador_id distintos) -> 10 x 200 y 40 x 204; ninguna tarea con dos dueños
    (respuestas + agregación en Mongo + locks en Valkey). Se repite R rondas.
 c) Segundo enviar -> 409 y sin fila extra en el datalake.
 d) w:majority vs w:1 (registro reiniciado con MONGO_W=1), mismo microbenchmark.
 e) explain(executionStats) de las consultas con índice (4 obligatorios + 1 extra).

Uso: python pruebas/p2_concurrencia.py [--rondas 3] [--ops 600] [--hilos 8]
"""
import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun as C  # noqa: E402
import httpx  # noqa: E402


_CLIENTE = None


def calentar(_):
    """Proceso hijo: crea su cliente HTTP y abre la conexión (para que los 50 procesos existan antes de la ronda)."""
    global _CLIENTE
    _CLIENTE = httpx.Client(base_url=C.GATEWAY, timeout=60.0)
    _CLIENTE.get("/salud")
    time.sleep(4)  # obliga al pool a crear un proceso por tarea
    return os.getpid()


def disparar(args):
    """Proceso hijo: espera el instante común y envía UNA solicitud siguiente."""
    anotador, pid, t_inicio, url = args
    global _CLIENTE
    c = _CLIENTE or httpx.Client(base_url=url, timeout=60.0)
    while time.time() < t_inicio:
        pass
    t0 = time.time()
    p = time.perf_counter()
    try:
        r = c.post("/tareas/siguiente", json={"anotador_id": anotador, "proyecto_id": pid})
        ms = (time.perf_counter() - p) * 1000
        d = r.json() if r.status_code == 200 else None
        return {"anotador": anotador, "status": r.status_code, "t_envio": t0, "ms": ms,
                "tarea_id": d["tarea"]["_id"] if d else None, "token": d["token"] if d else None}
    except Exception as e:  # noqa: BLE001
        return {"anotador": anotador, "status": 0, "t_envio": t0, "ms": None, "error": type(e).__name__}


def ronda_concurrente(ex, pid, n=50, espera=2.0):
    t_inicio = time.time() + espera
    args = [(f"conc-{pid[:6]}-{i:02d}", pid, t_inicio, C.GATEWAY) for i in range(n)]
    return list(ex.map(disparar, args))


def verificar_mongo(pid):
    js = f"""
const pid='{pid}';
const porEstado = db.tareas.aggregate([{{$match:{{proyecto_id:pid}}}},{{$group:{{_id:'$estado',n:{{$sum:1}}}}}}]).toArray();
const dueños = db.tareas.aggregate([{{$match:{{proyecto_id:pid,estado:'abierta'}}}},
   {{$group:{{_id:'$asignacion.anotador_id',tareas:{{$addToSet:'$_id'}}}}}},{{$match:{{$expr:{{$gt:[{{$size:'$tareas'}},1]}}}}}}]).toArray();
const tareasConVariosDueños = db.tareas.find({{proyecto_id:pid,estado:'abierta'}},{{'asignacion.anotador_id':1,'asignacion.token':1}}).toArray();
const unicos = new Set(tareasConVariosDueños.map(t=>t.asignacion.anotador_id)).size;
print(JSON.stringify({{por_estado:porEstado,anotadores_con_mas_de_una_tarea:dueños.length,
   tareas_abiertas:tareasConVariosDueños.length,anotadores_distintos_en_abiertas:unicos,
   tokens:tareasConVariosDueños.map(t=>[t._id,t.asignacion.anotador_id,t.asignacion.token])}}));"""
    return C.mongosh_json(js)


def bench(c, ops, hilos, etiqueta):
    """Microbenchmark: `hilos` anotadores toman y envían tareas hasta `ops` envíos. Devuelve latencias."""
    p, ids = C.crear_proyecto(c, f"p2-bench-{etiqueta}", "imagen", 24, ops)
    lock = threading.Lock()
    contador = {"n": 0}
    lat_sig, lat_env, err = [], [], {"sig": 0, "env": 0}

    def trabajo(k):
        cl = httpx.Client(base_url=C.GATEWAY, timeout=30.0)
        a = f"bench-{etiqueta}-{k}"
        while True:
            with lock:
                if contador["n"] >= ops:
                    return
                contador["n"] += 1
            t = time.perf_counter()
            r = cl.post("/tareas/siguiente", json={"anotador_id": a, "proyecto_id": p["_id"]})
            lat_sig.append((time.perf_counter() - t) * 1000)
            if r.status_code != 200:
                err["sig"] += 1
                continue
            d = r.json()
            t = time.perf_counter()
            r = cl.post(f"/tareas/{d['tarea']['_id']}/enviar", json={"anotador_id": a, "token": d["token"], "anotacion": {"cajas": []}})
            lat_env.append((time.perf_counter() - t) * 1000)
            if r.status_code != 200:
                err["env"] += 1

    t0 = time.time()
    with ThreadPoolExecutor(hilos) as ex:
        list(ex.map(trabajo, range(hilos)))
    dur = time.time() - t0
    return {"proyecto_id": p["_id"], "ops": ops, "hilos": hilos, "duracion_s": round(dur, 1),
            "ciclos_por_s": round(ops / dur, 1), "siguiente": C.lat(lat_sig), "enviar": C.lat(lat_env), "errores": err}


EXPLAIN_JS = """
function etapas(p){ p = p.queryPlan || p; let o=[p.stage+(p.indexName?'('+p.indexName+')':'')];
  const h = p.inputStage ? etapas(p.inputStage) : (p.inputStages ? [].concat(...p.inputStages.map(etapas)) : (p.shards?[]:[]));
  return o.concat(h); }
function res(c, hint){ let cur = hint ? c.hint(hint) : c; const e = cur.explain('executionStats'); const s=e.executionStats;
  return {plan: etapas(e.queryPlanner.winningPlan), nReturned:s.nReturned, keysExaminadas:s.totalKeysExamined,
          docsExaminados:s.totalDocsExamined, ms:s.executionTimeMillis}; }
"""


def explains():
    pid = C.mongosh_json("print(JSON.stringify([db.tareas.findOne({estado:'disponible'}).proyecto_id]))")[0]
    js = EXPLAIN_JS + f"""
const pid='{pid}'; const ahora = new Date();
const a = db.tareas.findOne({{estado:'abierta'}}) || {{asignacion:{{anotador_id:'x'}}}};
const anot = (a.asignacion||{{}}).anotador_id || 'x';
const batch = db.tareas.findOne({{proyecto_id:pid}}).batch_id;
const cli = db.proyectos.findOne({{estado:'activo'}}).cliente_id;
const filtroAsignar = {{$or:[{{estado:'disponible'}},{{estado:'abierta','asignacion.expira':{{$lt:ahora}}}}], proyecto_id:pid}};
const out = {{
 '1_asignar_con_proyecto (tareas_proyecto_estado_prioridad + tareas_estado_expira)': res(db.tareas.find(filtroAsignar).sort({{prioridad:-1}}).limit(1)),
 '1b_asignar_con_proyecto_forzando_COLLSCAN (comparacion)': res(db.tareas.find(filtroAsignar).sort({{prioridad:-1}}).limit(1), {{$natural:1}}),
 '2_asignar_sin_proyecto (global)': res(db.tareas.find({{$or:[{{estado:'disponible'}},{{estado:'abierta','asignacion.expira':{{$lt:ahora}}}}]}}).sort({{prioridad:-1}}).limit(1)),
 '3_reanudar_tarea_abierta_del_anotador (tareas_anotador_estado)': res(db.tareas.find({{'asignacion.anotador_id':anot,estado:'abierta','asignacion.expira':{{$gt:ahora}}}}).limit(1)),
 '4_tareas_de_un_batch (tareas_batch)': res(db.tareas.find({{batch_id:batch}})),
 '5_proyectos_activos_del_cliente (proyectos_cliente_estado)': res(db.proyectos.find({{cliente_id:cli,estado:'activo'}})),
 '6_EXTRA_tareas_abiertas_vencidas (tareas_estado_expira)': res(db.tareas.find({{estado:'abierta','asignacion.expira':{{$lt:ahora}}}})),
 '6b_EXTRA_sin_indice_forzando_COLLSCAN (comparacion)': res(db.tareas.find({{estado:'abierta','asignacion.expira':{{$lt:ahora}}}}), {{$natural:1}}),
 '7_exportar_aprobadas_paginado': res(db.tareas.find({{proyecto_id:pid,estado:'aprobada',_id:{{$gt:''}}}}).sort({{_id:1}}).limit(100)),
}};
const idx = {{}};
['tareas','proyectos'].forEach(c => idx[c] = db[c].getIndexes().map(i=>({{nombre:i.name,clave:i.key}})));
print(JSON.stringify({{proyecto_probado:pid,total_tareas:db.tareas.estimatedDocumentCount(),explain:out,indices:idx}}));"""
    return C.mongosh_json(js)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rondas", type=int, default=3)
    ap.add_argument("--ops", type=int, default=600)
    ap.add_argument("--hilos", type=int, default=8)
    ap.add_argument("--rondas-bench", type=int, default=3)
    a = ap.parse_args()
    ev = C.Evidencia(
        "p2_concurrencia", "P2. Registro, réplicas y concurrencia de asignación",
        "Estado del replica set; 50 solicitudes simultáneas con 10 tareas libres (10 asignaciones y 40 respuestas 204, "
        "ninguna tarea con dos dueños); segundo envío -> 409 sin fila extra en el datalake; w:majority vs w:1; "
        "explain de los índices.",
        {"rondas_concurrentes": a.rondas, "solicitudes_por_ronda": 50, "tareas_libres": 10,
         "ops_bench": a.ops, "hilos_bench": a.hilos, "rondas_bench_por_modo": a.rondas_bench,
         "mecanismo": "ProcessPoolExecutor (50 procesos) con barrera de reloj; httpx contra el gateway"})
    c = C.cliente(60)
    C.esperar_todo_ok(120)

    # a) rs.status
    js = """const s=rs.status(); const cfg=rs.conf();
print(JSON.stringify({set:s.set, miembros:s.members.map(m=>({nombre:m.name,estado:m.stateStr,salud:m.health,optime:m.optimeDate,pingMs:m.pingMs,uptime_s:m.uptime})),
 prioridades:cfg.members.map(m=>({host:m.host,prioridad:m.priority,votos:m.votes})),
 writeConcernMajorityJournalDefault:cfg.writeConcernMajorityJournalDefault,
 defaultRW:db.adminCommand({getDefaultRWConcern:1}).defaultWriteConcern||null}));"""
    rs = C.mongosh_json(js, db="admin")
    ev.paso("rs.status() resumen", rs)

    # b) 50 concurrentes x R rondas
    rondas = []
    pool = ProcessPoolExecutor(max_workers=50)
    pids = set(pool.map(calentar, range(50)))
    ev.paso("Pool de procesos", {"procesos_distintos": len(pids)})
    for k in range(a.rondas):
        p, ids = C.crear_proyecto(c, f"p2-conc-{k}-{int(time.time())}", "imagen", 24, 10)
        assert len(ids) == 10
        res = ronda_concurrente(pool, p["_id"])
        cont = {}
        for r in res:
            cont[r["status"]] = cont.get(r["status"], 0) + 1
        tareas = [r["tarea_id"] for r in res if r["status"] == 200]
        t_envios = [r["t_envio"] for r in res]
        mong = verificar_mongo(p["_id"])
        # locks en Valkey: cada tarea abierta tiene lock con el token de su dueño
        locks_ok = 0
        for tid, anot, tok in mong["tokens"]:
            if C.valkey_cli("GET", f"lock:tarea:{tid}") == tok:
                locks_ok += 1
        ronda = {"ronda": k + 1, "proyecto_id": p["_id"], "codigos": cont, "tareas_distintas_en_respuestas_200": len(set(tareas)),
                 "dispersion_inicio_ms": round((max(t_envios) - min(t_envios)) * 1000, 1),
                 "latencia_ms": C.lat([r["ms"] for r in res if r["ms"] is not None]),
                 "mongo": {k2: v for k2, v in mong.items() if k2 != "tokens"},
                 "locks_valkey_con_token_del_dueno": locks_ok,
                 "cumple": cont.get(200) == 10 and cont.get(204) == 40 and len(set(tareas)) == 10
                           and mong["anotadores_con_mas_de_una_tarea"] == 0 and mong["tareas_abiertas"] == 10
                           and mong["anotadores_distintos_en_abiertas"] == 10 and locks_ok == 10}
        rondas.append(ronda)
        ev.paso(f"Ronda concurrente {k + 1}", ronda)
        if k == 0:
            primer_pid, primer_tareas, primer_tokens = p["_id"], tareas, {t[0]: (t[1], t[2]) for t in mong["tokens"]}
    pool.shutdown()
    # c) segundo envío
    tid = primer_tareas[0]
    anot, tok = primer_tokens[tid]
    r1 = c.post(f"/tareas/{tid}/enviar", json={"anotador_id": anot, "token": tok, "anotacion": {"cajas": [1]}})
    time.sleep(0.2)
    r2 = c.post(f"/tareas/{tid}/enviar", json={"anotador_id": anot, "token": tok, "anotacion": {"cajas": [1]}})
    r3 = c.post(f"/tareas/{tid}/enviar", json={"anotador_id": "otro-anotador", "token": tok, "anotacion": {"cajas": [1]}})
    time.sleep(6)  # micro-lote del historial (T=2 s) + margen
    filas1 = C.eventos_de(tid)
    time.sleep(6)
    filas2 = C.eventos_de(tid)
    doc = C.mongosh(f"printjson(db.tareas.findOne({{_id:'{tid}'}},{{estado:1,version:1,ultimo_evento_id:1}}))")
    envio = {"tarea_id": tid, "primer_enviar": r1.status_code, "segundo_enviar": r2.status_code,
             "enviar_otro_anotador_mismo_token": r3.status_code,
             "filas_lago_tras_6s": [(f["estado"], f["event_id"][:8]) for f in sorted(filas1, key=lambda f: f["ts"])],
             "filas_lago_tras_12s": [(f["estado"], f["event_id"][:8]) for f in sorted(filas2, key=lambda f: f["ts"])],
             "filas_estado_anotada": sum(1 for f in filas2 if f["estado"] == "anotada"),
             "mongo": doc, "cumple": r1.status_code == 200 and r2.status_code == 409 and r3.status_code == 409
                                     and sum(1 for f in filas2 if f["estado"] == "anotada") == 1 and len(filas1) == len(filas2)}
    ev.paso("Segundo envío de una tarea ya enviada", envio)

    # d) w:majority vs w:1
    modos = {"majority": [], "w1": []}
    orden = []
    for i in range(a.rondas_bench):
        orden += [("majority", "majority"), ("w1", "1")]
    for nombre, w in orden:
        entorno = {"MONGO_W": w}
        C.dc("up", "-d", "--force-recreate", "registro", env=entorno, timeout=180)
        C.esperar_todo_ok(120)
        cfg = C.sh(["docker", "exec", "registro", "printenv", "MONGO_W"])[1].strip()
        time.sleep(3)
        b = bench(c, a.ops, a.hilos, f"{nombre}{len(modos[nombre])}")
        b["MONGO_W_en_contenedor"] = cfg
        modos[nombre].append(b)
        ev.paso(f"Microbenchmark con MONGO_W={cfg}", b)
    C.dc("up", "-d", "--force-recreate", "registro", timeout=180)  # restaura majority
    C.esperar_todo_ok(120)
    w_final = C.sh(["docker", "exec", "registro", "printenv", "MONGO_W"])[1].strip()

    def agrega(lista, clave):
        xs = []
        return {k: round(sum(b[clave][k] for b in lista) / len(lista), 2) for k in ("p50_ms", "p95_ms", "p99_ms")} | {"n_total": sum(b[clave]["n"] for b in lista)}
    comp = {m: {"enviar": agrega(l, "enviar"), "siguiente": agrega(l, "siguiente"),
                "ciclos_por_s_media": round(sum(b["ciclos_por_s"] for b in l) / len(l), 1)} for m, l in modos.items()}
    ev.paso("Comparación w:majority vs w:1 (media de las rondas)", comp,
            "Los valores son medias de los percentiles de cada ronda. En un solo host con 3 nodos en contenedores la "
            "latencia de replicación es mínima, así que la diferencia NO permite inferir el rendimiento de un clúster distribuido.")

    # d2) coste del write concern aislado del HTTP: updateOne secuenciales en mongosh
    js = """
const col = db.p2_wc; col.drop(); const N = 1500; const docs=[]; for (let i=0;i<N;i++) docs.push({_id:i,v:0}); col.insertMany(docs,{writeConcern:{w:'majority'}});
function medir(w){ const xs=[]; const t0=Date.now(); for (let i=0;i<N;i++){ const a=Date.now(); col.updateOne({_id:i},{$inc:{v:1}},{writeConcern:{w:w}}); xs.push(Date.now()-a);} 
  xs.sort((a,b)=>a-b); const p=q=>xs[Math.max(0,Math.ceil(q*xs.length)-1)]; return {w:String(w),n:N,total_ms:Date.now()-t0,media_ms:(Date.now()-t0)/N,p50_ms:p(.5),p95_ms:p(.95),p99_ms:p(.99)}; }
const r=[]; for (let k=0;k<3;k++){ r.push(medir('majority')); r.push(medir(1)); } col.drop(); print(JSON.stringify(r));"""
    wc = C.mongosh_json(js, timeout=600)
    ev.paso("w:majority vs w:1 directamente en Mongo (1500 updateOne secuenciales, 3 pares, resolución 1 ms)", wc,
            "Sin HTTP ni gateway: aísla el coste del reconocimiento de la mayoría en un clúster local de 3 contenedores.")

    # e) explain
    ex = explains()
    ev.paso("explain(executionStats) por consulta", ex)

    todas = all(r["cumple"] for r in rondas)
    ev.resultado(rondas_concurrentes=[{"ronda": r["ronda"], "codigos": r["codigos"], "cumple": r["cumple"],
                                       "dispersion_inicio_ms": r["dispersion_inicio_ms"]} for r in rondas],
                 todas_las_rondas_cumplen=todas, segundo_enviar=envio["segundo_enviar"],
                 fila_extra_en_lago=envio["filas_estado_anotada"] != 1, w_majority_vs_w1=comp, w_directo_en_mongo=wc,
                 MONGO_W_restaurado=w_final, replica_set={m["nombre"]: m["estado"] for m in rs["miembros"]})
    ev.cerrar("Con 10 tareas libres y 50 solicitudes simultáneas se obtuvieron exactamente 10 asignaciones (200) y 40 respuestas 204 en "
              f"cada una de las {a.rondas} rondas; la asignación es el findOneAndUpdate atómico de Mongo, por lo que ninguna tarea tuvo dos "
              "dueños (comprobado en las respuestas, con agregación en Mongo y con los tokens de los locks en Valkey). El segundo "
              "envío devolvió 409 y el lago no recibió una fila extra. La comparación w:majority/w:1 se midió en una sola "
              "máquina (3 nodos en contenedores): w:1 evita esperar la confirmación de la mayoría, pero la diferencia absoluta es pequeña porque "
              "no hay latencia de red real entre los nodos; no es extrapolable a un clúster distribuido. Los explain muestran qué índice usa cada consulta.")


if __name__ == "__main__":
    main()
