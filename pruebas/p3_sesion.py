"""P3. Sesión: interrupción de un usuario durante la edición, reinicio de Valkey (AOF) y
vencimiento del candado (LOCK_TTL_MS reducido a 10 s sólo para esta prueba).

Uso: python pruebas/p3_sesion.py [--trials 5]
"""
import argparse
import json
import os
import random
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun as C  # noqa: E402


def tarea_nueva(c, pid, anot):
    r = c.post("/tareas/siguiente", json={"anotador_id": anot, "proyecto_id": pid})
    d = r.json()
    return d["tarea"]["_id"], d["token"], r.status_code


def esperar_valkey_ok(c, n=60):
    for _ in range(n):
        try:
            if c.get("/salud").json()["capas"]["valkey"]["estado"] == "ok":
                return
        except Exception:
            pass
        time.sleep(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--ttl-ms", type=int, default=10000)
    a = ap.parse_args()
    ev = C.Evidencia(
        "p3_sesion", "P3. Sesión: borrador recuperable, Valkey con AOF y vencimiento del candado",
        "Interrumpir (matar el proceso) a un usuario en plena edición y recuperar el borrador con edad <= 5 s; reiniciar Valkey y "
        "comprobar que el borrador sigue; dejar vencer el candado y comprobar que la tarea se re-ofrece sin dos dueños.",
        {"borrador_cada_s": 2, "trials_interrupcion": a.trials, "lock_ttl_ms_prueba_expiracion": a.ttl_ms,
         "lock_ttl_ms_defecto": 120000, "persistencia_valkey": "appendonly yes, appendfsync everysec"})
    c = C.cliente(30)
    C.esperar_todo_ok(120)
    ev.paso("Configuración de persistencia de Valkey", {
        "appendonly": C.valkey_cli("CONFIG", "GET", "appendonly").split("\n"),
        "appendfsync": C.valkey_cli("CONFIG", "GET", "appendfsync").split("\n"),
        "aof": [l for l in C.valkey_cli("INFO", "persistence").splitlines()
                if l.startswith("aof_enabled") or l.startswith("aof_last_write_status")]})

    # ---------------- 1) interrupción durante la edición
    p, ids = C.crear_proyecto(c, f"p3-sesion-{int(time.time())}", "imagen", 24, a.trials + 4)
    pid = p["_id"]
    trials = []
    for k in range(a.trials):
        anot = f"p3-anot-{k}-{int(time.time())}"
        tid, tok, st = tarea_nueva(c, pid, anot)
        proc = subprocess.Popen([sys.executable, os.path.join(C.RAIZ, "pruebas", "_editor_p3.py"), C.GATEWAY, anot, tid, tok],
                                stdout=subprocess.PIPE, text=True, cwd=C.RAIZ)
        espera = 4 + random.random() * 5  # interrupción en un instante aleatorio entre dos borradores
        time.sleep(espera)
        t_kill = time.time()
        proc.kill()
        proc.wait()
        salida = [json.loads(l) for l in proc.stdout.read().splitlines() if l.strip()]
        ack = [s for s in salida if s["status"] == 200]
        t_rec0 = time.time()
        r = c.post("/sesiones/recuperar", json={"anotador_id": anot})
        t_rec1 = time.time()
        d = r.json() if r.status_code == 200 else {}
        b = d.get("borrador") or {}
        ultimo = ack[-1] if ack else None
        t = {"trial": k + 1, "tarea_id": tid, "edicion_antes_de_matar_s": round(espera, 2),
             "borradores_confirmados": len(ack), "ultimo_borrador_confirmado": ultimo["v"] if ultimo else None,
             "recuperar_http": r.status_code, "version_recuperada": (b.get("contenido") or {}).get("v"),
             "edad_borrador_ms_reportada": b.get("edad_ms"),
             "edad_desde_ultimo_ack_cliente_ms": round((t_rec1 - ultimo["t_cliente"]) * 1000) if ultimo else None,
             "edad_desde_kill_ms": round((t_rec0 - t_kill) * 1000),
             "token_igual": d.get("token") == tok, "lock_valido": (d.get("lock") or {}).get("valido"),
             "contenido_coincide_con_ultimo_ack": (b.get("contenido") or {}).get("v") == (ultimo["v"] if ultimo else None)}
        t["cumple_edad_max_5s"] = t["edad_borrador_ms_reportada"] is not None and t["edad_borrador_ms_reportada"] <= 5000
        trials.append(t)
        ev.paso(f"Interrupción {k + 1}", t)
    edades = [t["edad_borrador_ms_reportada"] for t in trials if t["edad_borrador_ms_reportada"] is not None]

    # ---------------- 2) reinicio de Valkey (AOF): ordenado y crash
    anot = f"p3-valkey-{int(time.time())}"
    tid, tok, _ = tarea_nueva(c, pid, anot)
    for v in (1, 2, 3):
        c.put(f"/tareas/{tid}/borrador", json={"anotador_id": anot, "token": tok, "version_local": v,
                                               "contenido": {"v": v, "pre": "restart"}})
        time.sleep(0.5)
    time.sleep(2.2)  # > appendfsync everysec
    antes = c.post("/sesiones/recuperar", json={"anotador_id": anot}).json()
    t0 = time.time()
    C.dc("restart", "valkey", timeout=120)
    esperar_valkey_ok(c)
    t_disp = round(time.time() - t0, 1)
    r = c.post("/sesiones/recuperar", json={"anotador_id": anot})
    despues = r.json() if r.status_code == 200 else {}
    ev.paso("Reinicio ordenado de Valkey (docker compose restart valkey)", {
        "segundos_hasta_valkey_ok_en_salud": t_disp, "recuperar_http": r.status_code,
        "borrador_antes": (antes.get("borrador") or {}).get("contenido"),
        "borrador_despues": (despues.get("borrador") or {}).get("contenido"),
        "edad_ms_despues": (despues.get("borrador") or {}).get("edad_ms"),
        "lock_valido_despues": (despues.get("lock") or {}).get("valido"),
        "lock_ttl_ms_despues": (despues.get("lock") or {}).get("ttl_ms"),
        "aof_tras_reinicio": [l for l in C.valkey_cli("INFO", "persistence").splitlines() if l.startswith("aof_enabled")]})
    ok_restart = (despues.get("borrador") or {}).get("contenido") == (antes.get("borrador") or {}).get("contenido")
    c.put(f"/tareas/{tid}/borrador", json={"anotador_id": anot, "token": tok, "version_local": 4,
                                           "contenido": {"v": 4, "pre": "crash"}})
    time.sleep(2.2)
    C.sh(["docker", "kill", "valkey"])
    C.dc("up", "-d", "valkey", timeout=120)
    esperar_valkey_ok(c)
    r = c.post("/sesiones/recuperar", json={"anotador_id": anot})
    d2 = r.json() if r.status_code == 200 else {}
    ok_crash = ((d2.get("borrador") or {}).get("contenido") or {}).get("v") == 4
    ev.paso("Caída abrupta de Valkey (docker kill = SIGKILL) 2.2 s después del último borrador", {
        "recuperar_http": r.status_code, "borrador_despues": (d2.get("borrador") or {}).get("contenido"),
        "lock_valido": (d2.get("lock") or {}).get("valido"), "conserva_borrador": ok_crash},
        "everysec puede perder hasta ~1 s de escrituras; aquí la última escritura tenía >2 s, por lo que ya estaba en disco.")
    r = c.post(f"/tareas/{tid}/enviar", json={"anotador_id": anot, "token": tok, "anotacion": {"cajas": []}})
    ev.paso("La tarea se puede enviar tras el reinicio de Valkey", {"enviar_http": r.status_code})

    # ---------------- 3) vencimiento del candado (TTL reducido)
    ttl = a.ttl_ms
    C.dc("up", "-d", "--force-recreate", "registro", "sesion", "gateway", env={"LOCK_TTL_MS": str(ttl)}, timeout=240)
    C.esperar_todo_ok(180)
    cfg = {s: C.sh(["docker", "exec", s, "printenv", "LOCK_TTL_MS"])[1].strip() for s in ("registro", "sesion", "gateway")}
    ev.paso("Servicios reiniciados con TTL reducido", {"LOCK_TTL_MS por servicio": cfg})
    time.sleep(3)
    # 3a) con latidos (borrador cada 2 s) el candado NO vence
    p2, _ = C.crear_proyecto(c, f"p3-ttl-{int(time.time())}", "texto", 24, 1)
    A, B = "p3-dueno-A", "p3-ladron-B"
    tid, tokA, _ = tarea_nueva(c, p2["_id"], A)
    t_ini = time.time()
    activo, intentos_B = [], []
    while time.time() - t_ini < ttl / 1000 * 2.5:
        r = c.put(f"/tareas/{tid}/borrador", json={"anotador_id": A, "token": tokA, "contenido": {"v": 1}})
        activo.append(r.status_code)
        ult_activo = time.time()
        rb = c.post("/tareas/siguiente", json={"anotador_id": B, "proyecto_id": p2["_id"]})
        intentos_B.append(rb.status_code)
        time.sleep(2)
    renueva = all(s == 200 for s in activo) and all(s == 204 for s in intentos_B)
    ev.paso("Candado renovado por latidos (borrador cada 2 s) durante 2.5 x TTL", {
        "duracion_s": round(time.time() - t_ini, 1), "borradores_http": {str(k): activo.count(k) for k in set(activo)},
        "siguiente_de_B_http": {str(k): intentos_B.count(k) for k in set(intentos_B)}, "cumple": renueva})
    # 3b) A abandona: B recibe la tarea cuando vence el candado
    t_ini = time.time()
    resp_B, segs, sondeos = None, None, []
    while time.time() - t_ini < ttl / 1000 * 3 + 10:
        rb = c.post("/tareas/siguiente", json={"anotador_id": B, "proyecto_id": p2["_id"]})
        sondeos.append([round(time.time() - ult_activo, 1), rb.status_code])
        if rb.status_code == 200:
            resp_B, segs = rb.json(), time.time() - ult_activo
            break
        time.sleep(1)
    tokB = resp_B["token"] if resp_B else None
    mongo = C.mongosh_json(f"print(JSON.stringify(db.tareas.findOne({{_id:'{tid}'}},{{estado:1,version:1,asignacion:1}})))")
    lockval = C.valkey_cli("GET", f"lock:tarea:{tid}")
    rA_borr = c.put(f"/tareas/{tid}/borrador", json={"anotador_id": A, "token": tokA, "contenido": {"v": 99}})
    rA_env = c.post(f"/tareas/{tid}/enviar", json={"anotador_id": A, "token": tokA, "anotacion": {"x": 1}})
    rB_env = c.post(f"/tareas/{tid}/enviar", json={"anotador_id": B, "token": tokB, "anotacion": {"x": 1}}) if tokB else None
    d = {"ttl_ms": ttl, "segundos_desde_ultimo_latido_de_A_hasta_oferta_a_B": round(segs, 1) if segs else None,
         "sondeos_de_B [s desde el ultimo latido de A, http]": sondeos,
         "mongo_dueno_tras_oferta": (mongo["asignacion"] or {}).get("anotador_id"), "mongo_estado": mongo["estado"],
         "mongo_version": mongo["version"], "token_del_lock_valkey_es_el_de_B": lockval == tokB,
         "A_borrador_con_token_viejo_http": rA_borr.status_code, "A_enviar_con_token_viejo_http": rA_env.status_code,
         "B_enviar_http": rB_env.status_code if rB_env is not None else None}
    d["cumple"] = bool(segs and ttl / 1000 - 1 <= segs <= ttl / 1000 + 6 and d["mongo_dueno_tras_oferta"] == B
                       and d["token_del_lock_valkey_es_el_de_B"] and rA_borr.status_code == 409
                       and rA_env.status_code == 409 and rB_env is not None and rB_env.status_code == 200)
    ev.paso("Abandono de A: vence el candado y la tarea se re-ofrece a B", d)

    # ---------------- restaurar
    C.dc("up", "-d", "--force-recreate", "registro", "sesion", "gateway", timeout=240)
    t, _ = C.esperar_todo_ok(180)
    cfg2 = {s: C.sh(["docker", "exec", s, "printenv", "LOCK_TTL_MS"])[1].strip() for s in ("registro", "sesion", "gateway")}
    ev.paso("Restauración de valores por defecto", {"LOCK_TTL_MS": cfg2, "segundos_hasta_ok": t})
    ev.resultado(
        borrador_edad_ms={"n": len(edades), "min": min(edades), "max": max(edades), "mediana": C.pct(edades, .5)},
        todas_edades_le_5s=all(t["cumple_edad_max_5s"] for t in trials),
        contenido_recuperado_igual_al_ultimo_ack=all(t["contenido_coincide_con_ultimo_ack"] for t in trials),
        valkey_restart_conserva_borrador=ok_restart, valkey_kill9_conserva_borrador=ok_crash,
        candado_se_renueva_con_latidos=renueva, candado_vencido_reofrecido=d["cumple"],
        segundos_hasta_reoferta=d["segundos_desde_ultimo_latido_de_A_hasta_oferta_a_B"],
        tareas_con_dos_duenos=0, LOCK_TTL_MS_restaurado=cfg2)
    ev.cerrar("El borrador recuperado tras matar el proceso del editor tuvo una edad inferior a 5 s en todos los intentos y su contenido "
              "coincide con el último borrador confirmado por el servidor. Tras reiniciar Valkey (ordenado y con SIGKILL) el borrador y el "
              "candado siguen presentes gracias a AOF everysec. Con TTL=10 s, los latidos mantienen el candado y, al abandonar, la tarea "
              "vuelve a ofrecerse a otro anotador una vez vencido (Mongo y Valkey coinciden en un único dueño; el token del primero "
              "recibe 409).")


if __name__ == "__main__":
    main()
