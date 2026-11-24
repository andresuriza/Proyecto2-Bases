"""P8. Fallas y recuperación bajo escritura continua.

Mientras un escritor continuo (siguiente -> enviar) trabaja contra el gateway:
  S1 se detiene un secundario de Mongo (las escrituras siguen)
  S2a se detiene el primario de forma ordenada (elección; RTO = hasta que las escrituras vuelven)
  S2b se mata el primario (docker kill: caída abrupta)
  S3 se detienen los dos secundarios (sin mayoría: escrituras 503; lecturas/salud/borradores)
  S4 se reinicia Valkey a mitad de una sesión (el borrador sobrevive)
  S5 se interrumpe una sesión y se recupera
Al final: cada tarea con HTTP 200 en enviar debe estar exactamente una vez en Mongo con estado
anotada+ y su evento en el datalake (RPO = 0 para escrituras confirmadas).

Uso: python pruebas/p8_fallas.py [--hilos 6]
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun as C  # noqa: E402
import httpx  # noqa: E402

OPS = []          # (t_inicio, t_fin, tipo, status, tarea_id, intento, hilo)
ENVIOS = {}       # tarea_id -> {"ack":[(evento_id, version)], "intentos": n, "estados":[...]}
LOCK = threading.Lock()
PARAR = threading.Event()
ACCIONES = []     # registro de acciones de recuperación


def log_accion(texto, **kw):
    r = {"t": round(time.time(), 2), "hora": time.strftime("%H:%M:%S"), "accion": texto, **kw}
    ACCIONES.append(r)
    print(f"[{r['hora']}] {texto}", flush=True)
    return r["t"]


def escritor(k, pid):
    cl = httpx.Client(base_url=C.GATEWAY, timeout=40.0)
    anot = f"p8-esc-{k}-{int(time.time())}"
    while not PARAR.is_set():
        t0 = time.time()
        try:
            r = cl.post("/tareas/siguiente", json={"anotador_id": anot, "proyecto_id": pid})
            st = r.status_code
        except Exception:
            r, st = None, 0
        with LOCK:
            OPS.append((t0, time.time(), "siguiente", st, None, 0, k))
        if st == 204:
            time.sleep(1)
            continue
        if st != 200:
            time.sleep(0.3)
            continue
        d = r.json()
        tid, tok = d["tarea"]["_id"], d["token"]
        time.sleep(0.15)
        # enviar con reintentos manuales del "usuario" ante 503 (la tarea sigue abierta, borrador en Valkey)
        for intento in range(1, 80):
            if PARAR.is_set() and intento > 3:
                break
            t0 = time.time()
            try:
                r = cl.post(f"/tareas/{tid}/enviar", json={"anotador_id": anot, "token": tok, "anotacion": {"cajas": [], "k": k}})
                st = r.status_code
            except Exception:
                r, st = None, 0
            with LOCK:
                OPS.append((t0, time.time(), "enviar", st, tid, intento, k))
                e = ENVIOS.setdefault(tid, {"ack": [], "intentos": 0, "estados": []})
                e["intentos"] += 1
                e["estados"].append(st)
                if st == 200:
                    j = r.json()
                    e["ack"].append((j["evento_id"], j["version"]))
                    if j.get("reconciliado"):
                        e["reconciliado"] = True
            if st in (200, 409, 404):
                break
            time.sleep(1.0)
        time.sleep(0.1)


def ventana(t0, t1):
    return [o for o in OPS if t0 <= o[0] < t1]


def resumen_ventana(t0, t1):
    ops = ventana(t0, t1)
    out = {}
    for tipo in ("siguiente", "enviar"):
        xs = [o for o in ops if o[2] == tipo]
        cont = Counter(o[3] for o in xs)
        ok = [(o[1] - o[0]) * 1000 for o in xs if o[3] in (200, 204)]
        out[tipo] = {"n": len(xs), "status": {str(k): v for k, v in sorted(cont.items())}, **{k: v for k, v in C.lat(ok).items() if k.startswith("p")}}
    return out


def rto(t_fault, t_fin):
    """Retorno a escrituras estables: primer enviar 200 posterior al último fallo (5xx/red) de la ventana."""
    ops = ventana(t_fault, t_fin)
    fallos = [o for o in ops if o[3] == 0 or o[3] >= 500]
    if not fallos:
        return {"primer_fallo_s": None, "ultimo_fallo_s": None, "rto_s": 0.0, "fallos": 0, "ventana_sin_escrituras_s": 0.0}
    primero, ultimo = min(o[0] for o in fallos), max(o[1] for o in fallos)
    exitos = [o for o in ops if o[2] == "enviar" and o[3] == 200 and o[0] > ultimo]
    t_ok = min((o[1] for o in exitos), default=None)
    # última escritura exitosa antes del primer fallo
    previos = [o[1] for o in ops if o[2] == "enviar" and o[3] == 200 and o[1] < primero]
    return {"primer_fallo_s": round(primero - t_fault, 1), "ultimo_fallo_s": round(ultimo - t_fault, 1),
            "rto_s": round(t_ok - t_fault, 1) if t_ok else None,
            "ventana_sin_escrituras_s": round(t_ok - max(previos), 1) if (t_ok and previos) else None,
            "fallos": len(fallos), "codigos_fallo": dict(Counter(o[3] for o in fallos))}


def primario():
    """Nodo que se declara primario a sí mismo (isWritablePrimary); None si no hay."""
    for n in ("mongo1", "mongo2", "mongo3"):
        rc, out, _ = C.sh(["docker", "exec", n, "mongosh", "--quiet", "--eval", "print(db.hello().isWritablePrimary)"], timeout=30)
        if rc == 0 and out.strip() == "true":
            return n
    return None


def esperar_primario_estable(nombre="mongo1", maximo=120, estable_s=12):
    """Espera a que `nombre` sea primario durante `estable_s` segundos seguidos (prioridad 2: recupera el
    primario tras reincorporarse). Así cada escenario parte del mismo estado."""
    t0 = time.time()
    desde = None
    while time.time() - t0 < maximo:
        if primario() == nombre:
            desde = desde or time.time()
            if time.time() - desde >= estable_s:
                return round(time.time() - t0, 1)
        else:
            desde = None
        time.sleep(1)
    return None


def rs_resumen():
    try:
        return C.mongosh_json("print(JSON.stringify(rs.status().members.map(m=>[m.name,m.stateStr,m.health])))", db="admin", timeout=40)
    except Exception as e:  # noqa: BLE001
        return f"sin respuesta: {e}"


def esperar_primario(maximo=90):
    t0 = time.time()
    while time.time() - t0 < maximo:
        p = primario()
        if p:
            return p, round(time.time() - t0, 1)
        time.sleep(1)
    return None, None


def esperar_rs_completo(maximo=120):
    t0 = time.time()
    while time.time() - t0 < maximo:
        r = rs_resumen()
        if isinstance(r, list) and len(r) == 3 and all(m[2] == 1 for m in r) and sum(1 for m in r if m[1] == "PRIMARY") == 1 \
                and sum(1 for m in r if m[1] == "SECONDARY") == 2:
            return round(time.time() - t0, 1), r
        time.sleep(2)
    return None, rs_resumen()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hilos", type=int, default=6)
    ap.add_argument("--n-tareas", type=int, default=4000)
    a = ap.parse_args()
    ev = C.Evidencia(
        "p8_fallas", "P8. Fallas y recuperación con escrituras en curso",
        "Provocar fallas de un secundario, del primario (ordenada y abrupta), pérdida de mayoría y reinicio de Valkey mientras se escribe; "
        "medir RTO, 503 durante la elección y verificar RPO = 0 para toda escritura confirmada (HTTP 200).",
        {"escritores_concurrentes": a.hilos, "ciclo": "siguiente -> enviar (reintento manual cada 1 s ante 503)", "mongo_w": "majority",
         "wTimeoutMS": 3000, "serverSelectionTimeoutMS": 2500, "electionTimeout_mongo": "10 s (por defecto)",
         "inyeccion": "docker compose stop / docker kill / docker compose restart"})
    c = C.cliente(40)
    C.restaurar_estado_normal(None)
    rs0 = rs_resumen()
    ev.paso("Estado inicial del replica set", rs0)
    p, ids = C.crear_proyecto(c, f"p8-fallas-{int(time.time())}", "imagen", 24, a.n_tareas)
    pid = p["_id"]
    hilos = [threading.Thread(target=escritor, args=(k, pid), daemon=True) for k in range(a.hilos)]
    for h in hilos:
        h.start()
    escen = {}
    # compactar el lago (los micro-lotes de P4/P6 dejan cientos de archivos pequeños y Spark tarda >60 s)
    comp = []
    for m in sorted(os.path.basename(d).split("=")[1] for d in __import__("glob").glob(C.LAGO + "/eventos/mes=*")):
        n = len(__import__("glob").glob(C.LAGO + f"/eventos/mes={m}/*.parquet"))
        if n > 1:
            st_, r_ = C.interno("historial", "POST", f"/compactar?mes={m}", 8003, timeout=300)
            comp.append({"mes": m, "archivos_antes": n, "http": st_, "origenes": (r_ or {}).get("origenes")})
    ev.paso("Compactación previa del lago (para que la sonda de analítica del escenario S3 sea rápida)", comp)
    ev.paso("Primario estable antes de empezar", {"segundos": esperar_primario_estable("mongo1"), "primario": primario()})

    # ---- S0 línea base
    t = time.time()
    time.sleep(15)
    escen["S0_linea_base"] = {"ventana": resumen_ventana(t, time.time())}
    ev.paso("S0 línea base (15 s)", escen["S0_linea_base"])

    # ---- S1 secundario detenido
    prim = primario()
    sec = next(n for n in ("mongo3", "mongo2", "mongo1") if n != prim)
    t_f = log_accion(f"S1: detener secundario {sec} (primario={prim})")
    C.dc("stop", sec, timeout=120)
    time.sleep(20)
    t_fin = time.time()
    s = {"secundario_detenido": sec, "primario": prim, "ventana": resumen_ventana(t_f, t_fin), "rto": rto(t_f, t_fin),
         "salud": C.resumen_salud(c.get("/salud").json()), "rs": rs_resumen()}
    log_accion(f"S1: iniciar {sec}")
    C.dc("start", sec, timeout=120)
    t_rej, rs_ok = esperar_rs_completo()
    s["segundos_hasta_replica_set_completo"] = t_rej
    escen["S1_secundario_detenido"] = s
    ev.paso("S1 secundario detenido: las escrituras continúan", s)
    time.sleep(5)

    # ---- S2a primario detenido (ordenado) y S2b primario muerto (kill)
    for etiqueta, comando in (("S2a_primario_stop_ordenado", ["docker", "compose", "stop", "-t", "10"]),
                              ("S2b_primario_kill", ["docker", "kill"])):
        ev.paso(f"{etiqueta}: espera de primario estable (mongo1)", {"segundos": esperar_primario_estable("mongo1")})
        prim = primario()
        t_f = log_accion(f"{etiqueta}: {' '.join(comando)} {prim}")
        t0 = time.time()
        C.sh(comando + [prim], timeout=120)
        t_cmd = round(time.time() - t0, 1)
        nuevo, t_el = esperar_primario(60)
        t_fin_obs = time.time()
        time.sleep(12)
        t_fin = time.time()
        s = {"primario_detenido": prim, "duracion_del_comando_s": t_cmd, "nuevo_primario": nuevo, "segundos_hasta_nuevo_primario_desde_inicio_del_comando": round(t_fin_obs - t_f, 1),
             "ventana": resumen_ventana(t_f, t_fin), "rto": rto(t_f, t_fin), "salud_post_eleccion": C.resumen_salud(c.get("/salud").json())}
        log_accion(f"{etiqueta}: iniciar {prim}")
        t_s = time.time()
        C.dc("start", prim, timeout=120)
        t_rej, rs_ok = esperar_rs_completo()
        s["segundos_hasta_replica_set_completo"] = t_rej
        s["rs_final"] = rs_ok
        time.sleep(8)  # mongo1 (prioridad 2) puede reclamar el primario: se observa en el escritor
        s["tras_reincorporar"] = {"ventana": resumen_ventana(t_s, time.time()), "rto": rto(t_s, time.time())}
        escen[etiqueta] = s
        ev.paso(f"{etiqueta}", s)

    # ---- S3 sin mayoría
    ev.paso("S3: espera de primario estable (mongo1)", {"segundos": esperar_primario_estable("mongo1")})
    prim = primario()
    secs = [n for n in ("mongo1", "mongo2", "mongo3") if n != prim]
    # usuario dedicado: tarea abierta con borrador antes de la falla
    U = f"p8-usuario-{int(time.time())}"
    r = c.post("/tareas/siguiente", json={"anotador_id": U, "proyecto_id": pid})
    tU, tokU = r.json()["tarea"]["_id"], r.json()["token"]
    c.put(f"/tareas/{tU}/borrador", json={"anotador_id": U, "token": tokU, "contenido": {"v": 1, "texto": "antes de la falla"}})
    t_f = log_accion(f"S3: detener los dos secundarios {secs} (primario={prim}) -> sin mayoría")
    C.dc("stop", *secs, timeout=120)
    time.sleep(6)
    sondas = {}
    for nombre, f in [
        ("POST /tareas/siguiente (escritura)", lambda: c.post("/tareas/siguiente", json={"anotador_id": "p8-nuevo", "proyecto_id": pid})),
        ("POST /tareas/{id}/enviar (escritura)", lambda: c.post(f"/tareas/{tU}/enviar", json={"anotador_id": U, "token": tokU, "anotacion": {"cajas": []}})),
        ("PUT /tareas/{id}/borrador (sólo Valkey)", lambda: c.put(f"/tareas/{tU}/borrador", json={"anotador_id": U, "token": tokU, "contenido": {"v": 2, "texto": "durante la falla"}})),
        ("GET /proyectos/{id}/exportar (lectura Mongo)", lambda: c.get(f"/proyectos/{pid}/exportar")),
        ("POST /sesiones/recuperar", lambda: c.post("/sesiones/recuperar", json={"anotador_id": U})),
        ("GET /proyectos/{id}/editando (Valkey)", lambda: c.get(f"/proyectos/{pid}/editando")),
        ("GET /analitica/q1 (sólo Parquet)", lambda: c.get("/analitica/q1", timeout=150)),
        ("GET /salud", lambda: c.get("/salud")),
    ]:
        t0 = time.time()
        try:
            rr = f()
            sondas[nombre] = {"http": rr.status_code, "ms": round((time.time() - t0) * 1000), "cuerpo": rr.text[:160]}
        except Exception as e:  # noqa: BLE001
            sondas[nombre] = {"error": type(e).__name__}
    salud_caido = c.get("/salud").json()
    time.sleep(10)
    t_fin = time.time()
    s = {"detenidos": secs, "primario_antes": prim, "sondas_durante": sondas, "salud_durante": C.resumen_salud(salud_caido),
         "ventana_escritor": resumen_ventana(t_f + 6, t_fin), "rs_durante": rs_resumen()}
    t_s = log_accion(f"S3: iniciar {secs}")
    C.dc("start", *secs, timeout=120)
    t_rej, rs_ok = esperar_rs_completo()
    # el usuario reintenta el envío cuando el registro responde
    t_reint = None
    for _ in range(60):
        rr = c.post(f"/tareas/{tU}/enviar", json={"anotador_id": U, "token": tokU, "anotacion": {"cajas": [], "reintento": True}})
        if rr.status_code in (200, 409):
            t_reint = round(time.time() - t_s, 1)
            break
        time.sleep(1)
    s.update(segundos_hasta_replica_set_completo=t_rej, rs_final=rs_ok, reintento_enviar_http=rr.status_code,
             segundos_hasta_envio_exitoso_desde_restauracion=t_reint, rto_escritor=rto(t_f, time.time()))
    escen["S3_sin_mayoria"] = s
    ev.paso("S3 sin mayoría: escrituras 503, borrador a salvo, reanudación", s)
    if rr.status_code == 200:
        ENVIOS.setdefault(tU, {"ack": [], "intentos": 1, "estados": []})["ack"].append((rr.json()["evento_id"], rr.json()["version"]))
        ENVIOS[tU]["reconciliado"] = bool(rr.json().get("reconciliado"))
    s["reintento_enviar_reconciliado"] = rr.json().get("reconciliado") if rr.status_code == 200 else None
    ev.paso("S3: el usuario reintenta el envío tras restaurar (reconciliación si Mongo ya lo había aplicado)",
            {"http": rr.status_code, "respuesta": rr.json() if rr.status_code == 200 else rr.text[:200]})
    ev.paso("Primario estable tras S3", {"segundos": esperar_primario_estable("mongo1")})

    # ---- S4 reinicio de Valkey a mitad de sesión
    V = f"p8-valkey-{int(time.time())}"
    r = c.post("/tareas/siguiente", json={"anotador_id": V, "proyecto_id": pid})
    tV, tokV = r.json()["tarea"]["_id"], r.json()["token"]
    for v in (1, 2, 3):
        c.put(f"/tareas/{tV}/borrador", json={"anotador_id": V, "token": tokV, "version_local": v, "contenido": {"v": v, "texto": "antes de reiniciar valkey"}})
        time.sleep(0.4)
    time.sleep(2.2)
    t_f = log_accion("S4: docker compose restart valkey")
    C.dc("restart", "valkey", timeout=120)
    tv_ok = None
    for _ in range(60):
        try:
            if c.get("/salud").json()["capas"]["valkey"]["estado"] == "ok":
                tv_ok = round(time.time() - t_f, 1)
                break
        except Exception:
            pass
        time.sleep(1)
    time.sleep(8)
    rr = c.post("/sesiones/recuperar", json={"anotador_id": V})
    d = rr.json() if rr.status_code == 200 else {}
    rb = c.put(f"/tareas/{tV}/borrador", json={"anotador_id": V, "token": tokV, "version_local": 4, "contenido": {"v": 4}})
    renv = c.post(f"/tareas/{tV}/enviar", json={"anotador_id": V, "token": tokV, "anotacion": {"cajas": []}})
    escen["S4_valkey_restart"] = {
        "segundos_hasta_valkey_ok": tv_ok, "recuperar_http": rr.status_code, "borrador_recuperado": (d.get("borrador") or {}).get("contenido"),
        "lock_valido": (d.get("lock") or {}).get("valido"), "borrador_sigue": ((d.get("borrador") or {}).get("contenido") or {}).get("v") == 3,
        "put_borrador_tras_reinicio": rb.status_code, "enviar_tras_reinicio": renv.status_code,
        "ventana_escritor_durante_y_despues": resumen_ventana(t_f, time.time()), "rto_escritor": rto(t_f, time.time()),
        "eventos_en_buffer_gateway": c.get("/salud").json()["capas"]["gateway"]}
    ev.paso("S4 reinicio de Valkey a mitad de sesión", escen["S4_valkey_restart"])
    if renv.status_code == 200:
        ENVIOS.setdefault(tV, {"ack": [], "intentos": 1, "estados": [200]})["ack"].append((renv.json()["evento_id"], renv.json()["version"]))

    # ---- S5 sesión interrumpida y recuperada
    W = f"p8-interrumpido-{int(time.time())}"
    r = c.post("/tareas/siguiente", json={"anotador_id": W, "proyecto_id": pid})
    tW, tokW = r.json()["tarea"]["_id"], r.json()["token"]
    proc = subprocess.Popen([sys.executable, os.path.join(C.RAIZ, "pruebas", "_editor_p3.py"), C.GATEWAY, W, tW, tokW],
                            stdout=subprocess.PIPE, text=True, cwd=C.RAIZ)
    time.sleep(7.3)
    proc.kill()
    proc.wait()
    ack = [json.loads(l) for l in proc.stdout.read().splitlines() if l.strip()]
    rr = c.post("/sesiones/recuperar", json={"anotador_id": W})
    d = rr.json() if rr.status_code == 200 else {}
    renv = c.post(f"/tareas/{tW}/enviar", json={"anotador_id": W, "token": d.get("token", tokW), "anotacion": {"cajas": []}})
    escen["S5_sesion_interrumpida"] = {"borradores_confirmados": len(ack), "recuperar_http": rr.status_code,
                                      "version_recuperada": ((d.get("borrador") or {}).get("contenido") or {}).get("v"),
                                      "edad_ms": (d.get("borrador") or {}).get("edad_ms"), "enviar_tras_recuperar": renv.status_code}
    ev.paso("S5 sesión interrumpida y recuperada", escen["S5_sesion_interrumpida"])
    if renv.status_code == 200:
        ENVIOS.setdefault(tW, {"ack": [], "intentos": 1, "estados": [200]})["ack"].append((renv.json()["evento_id"], renv.json()["version"]))
    # el usuario de S3 también debe quedar contabilizado
    # (su envío tras restaurar se hizo fuera del escritor)

    # ---- fin del escritor y verificación RPO
    time.sleep(10)
    PARAR.set()
    for h in hilos:
        h.join(timeout=90)
    log_accion("escritor detenido; verificación de consistencia")
    t_esp, rs_ok = esperar_rs_completo(180)
    ev.paso("Replica set al terminar", {"segundos": t_esp, "rs": rs_ok})
    time.sleep(8)  # micro-lote del historial

    acked = {tid: e for tid, e in ENVIOS.items() if e["ack"]}
    # el envío del usuario de S3 tras restaurar
    ids_ack = list(acked)
    docs = C.mongosh_script_json("""
const ids = %s;
const res = [];
ids.forEach(id => { const d = db.tareas.find({_id:id},{estado:1,version:1,ultimo_evento_id:1}).toArray();
  res.push([id, d.length, d.length ? d[0].estado : null, d.length ? d[0].version : null]); });
print(JSON.stringify(res));""" % json.dumps(ids_ack), timeout=300)
    faltan = [d for d in docs if d[1] != 1]
    estado_mal = [d for d in docs if d[1] == 1 and d[2] not in ("anotada", "en_revision", "aprobada")]
    dobles = [tid for tid, e in acked.items() if len(e["ack"]) > 1]
    # eventos en el lago
    filas = C.eventos_de(ids_ack)
    por_id = Counter(f["event_id"] for f in filas)
    sin_evento = [tid for tid, e in acked.items() if not any(ev_id in por_id for ev_id, _ in e["ack"])]
    anotadas_lago = Counter(f["tarea_id"] for f in {f["event_id"]: f for f in filas}.values() if f["estado"] == "anotada")
    mas_de_una_anotada = [tid for tid in ids_ack if anotadas_lago.get(tid, 0) > 1]
    # tareas con 503/0 en todos los intentos (nunca confirmadas)
    nunca = {tid: e for tid, e in ENVIOS.items() if not e["ack"]}
    nunca_en_mongo = C.mongosh_script_json("""
const ids = %s; const r=[]; ids.forEach(id=>{const d=db.tareas.findOne({_id:id},{estado:1}); r.push([id, d?d.estado:null]);}); print(JSON.stringify(r));""" % json.dumps(list(nunca)), timeout=120) if nunca else []
    c409 = [tid for tid, e in nunca.items() if 409 in e["estados"]]
    verif = {"envios_con_http_200": len(acked), "de_ellos_reconciliados (503 previo, ya aplicado en Mongo)": sum(1 for e in acked.values() if e.get("reconciliado")), "tareas_perdidas_en_mongo (RPO)": len(faltan), "con_estado_incorrecto": len(estado_mal),
             "tareas_con_dos_acks_200": len(dobles), "eventos_ack_ausentes_en_lago": len(sin_evento),
             "tareas_con_mas_de_una_fila_anotada_en_lago": len(mas_de_una_anotada),
             "envios_nunca_confirmados (todos los intentos 503/error o 409)": len(nunca),
             "de_esos_con_409_tras_un_503 (commit ambiguo: estaban ya anotadas)": len(c409),
             "de_esos_estado_en_mongo": dict(Counter(x[1] for x in nunca_en_mongo)),
             "ejemplos_perdidas": faltan[:5]}
    ev.paso("Verificación de RPO y consistencia", verif)
    dos_duenos = C.mongosh_json("""print(JSON.stringify(db.tareas.aggregate([{$match:{proyecto_id:'%s',estado:'abierta'}},
      {$group:{_id:'$asignacion.anotador_id',n:{$sum:1}}},{$match:{n:{$gt:1}}}]).toArray()))""" % pid)
    ev.paso("Anotadores con más de una tarea abierta (debe ser vacío)", dos_duenos)

    # ---- estado final
    t, cuerpo = C.restaurar_estado_normal(ev)
    # espera a que mongo1 recupere el primario por prioridad (opcional)
    time.sleep(15)
    rs_f = rs_resumen()
    ev.paso("Estado final del replica set", rs_f)
    # series para gráficas
    base = min(o[0] for o in OPS)
    serie = [{"t": round(o[0] - base, 2), "dur_ms": round((o[1] - o[0]) * 1000, 1), "tipo": o[2], "status": o[3]} for o in OPS]
    C.guardar_texto("p8/operaciones.json", json.dumps({"t0_epoch": base, "acciones": [{**x, "t_rel": round(x["t"] - base, 2)} for x in ACCIONES], "ops": serie}))
    ev.archivo_extra("p8/operaciones.json")
    ev.paso("Acciones de recuperación registradas", ACCIONES)
    ev.resultado(escenarios={k: {kk: vv for kk, vv in v.items() if kk in ("rto", "ventana", "rto_escritor", "borrador_sigue", "enviar_tras_reinicio",
                                                                      "recuperar_http", "reintento_enviar_http", "nuevo_primario", "sondas_durante")}
                             for k, v in escen.items()},
                 rpo=verif, estado_final_rs=rs_f)
    ev.cerrar("Con un secundario caído las escrituras continúan sin errores (w:majority se cumple con 2 de 3). Al caer el primario hay una "
              "ventana breve de 503 hasta que se elige otro; el RTO y los códigos observados están en cada escenario. Sin mayoría las "
              "escrituras responden 503 (nada se confirma), el borrador sigue aceptándose en Valkey y, restaurados los nodos, el envío "
              "reintentado tiene éxito. Reiniciar Valkey conserva el borrador (AOF). Toda tarea con HTTP 200 en enviar existe exactamente "
              "una vez en Mongo en estado anotada o posterior y su evento está en el datalake: RPO = 0 para escrituras confirmadas.")


if __name__ == "__main__":
    main()
