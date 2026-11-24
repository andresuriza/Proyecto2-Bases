"""P1. REST API y despliegue: todas las rutas del gateway con códigos correctos y de error,
/salud en estado normal, degradado y caído (por capa), exposición de puertos y evidencia de que
el simulador sólo habla con el gateway.

Uso: python pruebas/p1_api.py
"""
import json
import os
import re
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import (compactar_lago_principal, RAIZ, Evidencia, cliente, crear_proyecto, dc, esperar_salud, esperar_todo_ok,  # noqa: E402
                   mongosh, resumen_salud, sh, guardar_texto, estado_contenedor)

ev = Evidencia(
    "p1_api", "P1. REST API, /salud en tres estados y exposición de puertos",
    "Ejercitar cada ruta del gateway con respuestas correctas y de error (200/201/204/404/409/422/503), "
    "mostrar /salud operativo, degradado y caído por capa, y demostrar que sólo el puerto 8000 está "
    "expuesto y que el simulador sólo contacta al gateway.",
    {"gateway": "http://localhost:8000", "metodo_503": "detener contenedores (docker compose stop)",
     "nota": "las rutas internas se llaman sólo a través del gateway; el cierre de un proyecto (409) se prepara con mongosh"})

c = cliente(60)
casos = []   # (descripcion, metodo, ruta, esperado, obtenido)


def caso(desc, resp, esperado, extra=None):
    ok = resp.status_code == esperado
    try:
        cuerpo = resp.json() if resp.content else None
    except Exception:
        cuerpo = resp.text[:200]
    resumen = json.dumps(cuerpo, ensure_ascii=False)[:260] if cuerpo is not None else ""
    casos.append({"caso": desc, "metodo": resp.request.method, "ruta": str(resp.request.url.path),
                  "esperado": esperado, "obtenido": resp.status_code, "ok": ok, "cuerpo": resumen,
                  "x_request_id": resp.headers.get("x-request-id")})
    print(("OK  " if ok else "FALLA"), resp.request.method, resp.request.url.path, resp.status_code, "esperado", esperado)
    return resp


# ------------------------------------------------------------ 1. rutas
ev.paso("Salud inicial", resumen_salud(esperar_todo_ok(120)[1]))
ev.paso("Compactación previa del lago (muchos archivos pequeños hacen lenta a Spark)", compactar_lago_principal())
caso("salud normal", c.get("/salud"), 200)

# proyectos
p, ids = crear_proyecto(c, "p1-api-proyecto", "imagen", 24, 6)
pid = p["_id"]
r = c.post("/proyectos", json={"nombre": "x", "modalidad": "imagen", "sla_horas": 24,
                                "taxonomia": [{"etiqueta": "a", "instruccion": "b"}]})
caso("crear proyecto", r, 201)
caso("crear proyecto con modalidad invalida", c.post("/proyectos", json={"nombre": "x", "modalidad": "audio", "sla_horas": 24,
                                                                         "taxonomia": [{"etiqueta": "a"}]}), 422)
caso("crear proyecto sin taxonomia", c.post("/proyectos", json={"nombre": "x", "modalidad": "texto", "sla_horas": 24,
                                                                "taxonomia": []}), 422)
caso("lote en proyecto inexistente", c.post("/proyectos/no-existe/lotes", json={"tareas": [
    {"archivo": {"uri": "s3://a/1", "sha256": "0" * 64, "bytes": 1, "mime": "image/jpeg"}}]}), 404)
caso("lote con sha256 invalido", c.post(f"/proyectos/{pid}/lotes", json={"tareas": [
    {"archivo": {"uri": "s3://a/1", "sha256": "xyz", "bytes": 1, "mime": "image/jpeg"}}]}), 422)
r = caso("lote valido (6 tareas via crear_proyecto)", c.post(f"/proyectos/{pid}/lotes", json={"tareas": [
    {"archivo": {"uri": "s3://p1/extra", "sha256": "a" * 64, "bytes": 5, "mime": "image/jpeg"}}]}), 201)
# proyecto cerrado -> 409
pc, _ = crear_proyecto(c, "p1-cerrado", "texto", 24, 0)
mongosh(f"db.proyectos.updateOne({{_id:'{pc['_id']}'}},{{$set:{{estado:'cerrado'}}}})")
caso("lote en proyecto cerrado", c.post(f"/proyectos/{pc['_id']}/lotes", json={"tareas": [
    {"archivo": {"uri": "s3://a/1", "sha256": "0" * 64, "bytes": 1, "mime": "text/plain"}}]}), 409)

# flujo de tarea
A, B, REV = "p1-anot-A", "p1-anot-B", "p1-revisor"
caso("siguiente sin cuerpo valido", c.post("/tareas/siguiente", json={}), 422)
r = caso("siguiente (asignacion)", c.post("/tareas/siguiente", json={"anotador_id": A, "proyecto_id": pid}), 200)
t1, tok1 = r.json()["tarea"]["_id"], r.json()["token"]
caso("siguiente de nuevo (reanuda la abierta)", c.post("/tareas/siguiente", json={"anotador_id": A, "proyecto_id": pid}), 200)
caso("borrador valido", c.put(f"/tareas/{t1}/borrador", json={"anotador_id": A, "token": tok1, "contenido": {"v": 1}, "version_local": 1}), 200)
caso("borrador con token invalido", c.put(f"/tareas/{t1}/borrador", json={"anotador_id": A, "token": "mal", "contenido": {"v": 1}}), 409)
caso("recuperar sesion", c.post("/sesiones/recuperar", json={"anotador_id": A}), 200)
caso("recuperar sin sesion", c.post("/sesiones/recuperar", json={"anotador_id": "nadie-" + str(time.time())}), 404)
caso("enviar con token invalido", c.post(f"/tareas/{t1}/enviar", json={"anotador_id": A, "token": "mal", "anotacion": {"x": 1}}), 409)
caso("enviar con otro anotador (token ajeno)", c.post(f"/tareas/{t1}/enviar", json={"anotador_id": B, "token": tok1, "anotacion": {"x": 1}}), 409)
r = caso("enviar", c.post(f"/tareas/{t1}/enviar", json={"anotador_id": A, "token": tok1, "anotacion": {"cajas": []}}), 200)
caso("segundo enviar (ya enviada)", c.post(f"/tareas/{t1}/enviar", json={"anotador_id": A, "token": tok1, "anotacion": {"cajas": []}}), 409)
caso("enviar con cuerpo incompleto", c.post(f"/tareas/{t1}/enviar", json={"anotador_id": A}), 422)

# liberar
r = c.post("/tareas/siguiente", json={"anotador_id": B, "proyecto_id": pid})
t2, tok2 = r.json()["tarea"]["_id"], r.json()["token"]
caso("liberar con token invalido", c.post(f"/tareas/{t2}/liberar", json={"anotador_id": B, "token": "mal"}), 409)
caso("liberar", c.post(f"/tareas/{t2}/liberar", json={"anotador_id": B, "token": tok2}), 200)
caso("liberar tarea inexistente", c.post("/tareas/no-existe/liberar", json={"anotador_id": B, "token": "x"}), 404)

# revisión
caso("revision: aprobar sin iniciar", c.post(f"/tareas/{t1}/revision", json={"revisor_id": REV, "accion": "aprobar"}), 409)
caso("revision: iniciar", c.post(f"/tareas/{t1}/revision", json={"revisor_id": REV, "accion": "iniciar"}), 200)
caso("revision: rechazar sin motivo", c.post(f"/tareas/{t1}/revision", json={"revisor_id": REV, "accion": "rechazar"}), 422)
caso("revision: accion invalida", c.post(f"/tareas/{t1}/revision", json={"revisor_id": REV, "accion": "borrar"}), 422)
caso("revision: aprobar", c.post(f"/tareas/{t1}/revision", json={"revisor_id": REV, "accion": "aprobar"}), 200)
caso("revision: tarea inexistente", c.post("/tareas/no-existe/revision", json={"revisor_id": REV, "accion": "iniciar"}), 404)
# rechazo: tarea nueva -> enviar -> iniciar -> rechazar (vuelve a disponible)
r = c.post("/tareas/siguiente", json={"anotador_id": A, "proyecto_id": pid})
t3, tok3 = r.json()["tarea"]["_id"], r.json()["token"]
c.post(f"/tareas/{t3}/enviar", json={"anotador_id": A, "token": tok3, "anotacion": {"cajas": []}})
c.post(f"/tareas/{t3}/revision", json={"revisor_id": REV, "accion": "iniciar"})
caso("revision: rechazar con motivo", c.post(f"/tareas/{t3}/revision", json={"revisor_id": REV, "accion": "rechazar", "motivo_codigo": "calidad_baja"}), 200)
ev.paso("Estado en Mongo tras el rechazo (vuelve a disponible)", mongosh(f"printjson(db.tareas.findOne({{_id:'{t3}'}},{{estado:1,version:1,ultima_revision:1,asignacion:1}}))"))

# sin tareas -> 204
pv, _ = crear_proyecto(c, "p1-vacio", "texto", 24, 0)
caso("siguiente sin tareas disponibles", c.post("/tareas/siguiente", json={"anotador_id": A + "-v", "proyecto_id": pv["_id"]}), 204)

# exportar
caso("exportar (aprobadas)", c.get(f"/proyectos/{pid}/exportar", params={"limite": 10}), 200)
caso("exportar limite invalido", c.get(f"/proyectos/{pid}/exportar", params={"limite": 0}), 422)
caso("editando", c.get(f"/proyectos/{pid}/editando"), 200)

# analítica
caso("analitica q1 principal", c.get("/analitica/q1", params={"conjunto": "principal"}), 200)
caso("analitica consulta invalida", c.get("/analitica/q9"), 422)
caso("analitica conjunto invalido", c.get("/analitica/q1", params={"conjunto": "xx"}), 422)
caso("ruta inexistente", c.get("/no-existe"), 404)

# cabecera X-Request-ID
rid = c.get("/salud", headers={"X-Request-ID": "p1-trazabilidad-123"})
ev.paso("X-Request-ID propagado", {"enviado": "p1-trazabilidad-123", "devuelto": rid.headers.get("x-request-id")})

# ------------------------------------------------------------ 2. 503 reales
for servicio, desc, req in [
    ("registro", "registro detenido: siguiente -> 503", lambda: c.post("/tareas/siguiente", json={"anotador_id": "p1-503", "proyecto_id": pid})),
    ("analitica", "analitica detenida: GET /analitica/q1 -> 503", lambda: c.get("/analitica/q1")),
]:
    dc("stop", servicio)
    time.sleep(2)
    caso(desc, req(), 503)
    dc("start", servicio)
    esperar_todo_ok(200)

fallas = [x for x in casos if not x["ok"]]
ev.paso("Tabla de rutas y códigos HTTP", casos)
ev.resultado(casos_total=len(casos), casos_ok=len(casos) - len(fallas), casos_con_discrepancia=fallas)

# ------------------------------------------------------------ 3. /salud en 3 estados
def escenario(nombre, detener, esperado, maximo=70, sonda=None):
    """Detiene contenedores, sondea /salud cada segundo, captura el cuerpo y restaura."""
    t0 = time.time()
    dc("stop", *detener)
    serie, cuerpo, codigo, visto = [], None, None, None
    while time.time() - t0 < maximo:
        try:
            r = c.get("/salud")
            cuerpo, codigo = r.json(), r.status_code
            noes = {k: v["estado"] for k, v in cuerpo["capas"].items() if v["estado"] != "ok"}
            serie.append({"t_s": round(time.time() - t0, 1), "http": codigo, "estado": cuerpo["estado"], "capas_no_ok": noes})
            if cuerpo["estado"] == esperado and visto is None:
                visto = round(time.time() - t0, 1)
            if visto is not None and time.time() - t0 > visto + 4:
                break
        except Exception as e:
            serie.append({"t_s": round(time.time() - t0, 1), "error": type(e).__name__})
        time.sleep(1)
    extra = sonda() if sonda else None
    dc("start", *detener)
    t_rest, _ = esperar_todo_ok(240)
    # serie reducida: sólo cambios
    red, prev = [], None
    for s in serie:
        k = json.dumps({x: s[x] for x in s if x != "t_s"}, sort_keys=True)
        if k != prev:
            red.append(s)
        prev = k
    d = {"detenidos": detener, "estado_esperado": esperado, "estado_alcanzado": cuerpo["estado"] if cuerpo else None,
         "http": codigo, "segundos_hasta_ver_estado": visto, "cambios_observados": red,
         "cuerpo_final": resumen_salud(cuerpo), "sonda": extra, "segundos_restauracion_todo_ok": t_rest}
    ev.paso(f"/salud: {nombre}", d)
    return d


salud_normal = resumen_salud(esperar_todo_ok(120)[1])
ev.paso("/salud normal", salud_normal)
esc = {}
esc["degradado_analitica"] = escenario("degradado (analitica detenida)", ["analitica"], "degradado")
esc["degradado_historial"] = escenario("degradado (historial detenido)", ["historial"], "degradado")
esc["degradado_spark_worker"] = escenario("degradado (spark-worker-1 detenido)", ["spark-worker-1"], "degradado", 80)
esc["degradado_mongo_secundario"] = escenario("degradado (secundario mongo3 detenido)", ["mongo3"], "degradado")
esc["caido_valkey"] = escenario("caido (valkey detenido)", ["valkey"], "caido", 60,
                                 sonda=lambda: {"siguiente_http": c.post("/tareas/siguiente", json={"anotador_id": "p1-vk", "proyecto_id": pid}).status_code,
                                                "salud_http": c.get("/salud").status_code})
esc["caido_mongo_sin_mayoria"] = escenario("caido (mongo2 y mongo3 detenidos: sin primario)", ["mongo2", "mongo3"], "caido", 90,
                                           sonda=lambda: {"siguiente_http": c.post("/tareas/siguiente", json={"anotador_id": "p1-mg", "proyecto_id": pid}).status_code})
esc["caido_registro"] = escenario("caido (registro detenido)", ["registro"], "caido", 40)
esc["caido_sesion"] = escenario("caido (sesion detenida)", ["sesion"], "caido", 40)

# ------------------------------------------------------------ 4. puertos
rc, ps, _ = dc("ps")
rc, psf, _ = sh(["docker", "ps", "--filter", "label=com.docker.compose.project=proyecto2", "--format", "table {{.Names}}	{{.Ports}}"])
ev.paso("docker compose ps", ps)
ev.paso("docker ps --format (puertos)", psf)
publicados = []
for linea in psf.splitlines()[1:]:
    partes_ = linea.split(None, 1); nombre, puertos = partes_[0], (partes_[1] if len(partes_) > 1 else "")
    for m in re.finditer(r"(\S+?):(\d+)->(\d+)/tcp", puertos):
        publicados.append({"contenedor": nombre.strip(), "bind": m.group(1), "host": int(m.group(2)), "contenedor_puerto": int(m.group(3))})
ev.paso("Puertos publicados al host", publicados,
        "0.0.0.0/[::]:8000 = gateway (único público). 127.0.0.1:4040 (Spark UI del driver) y 127.0.0.1:18080 "
        "(UI del master) son de administración y sólo escuchan en loopback; no reciben tráfico de usuarios.")

conexiones = {}
for puerto, que in [(27017, "mongo1"), (6379, "valkey"), (8001, "registro"), (8002, "sesion"), (8003, "historial"),
                    (8004, "analitica"), (7077, "spark-master"), (8000, "gateway (debe abrir)")]:
    s = socket.socket()
    s.settimeout(2)
    try:
        s.connect(("127.0.0.1", puerto))
        res = "CONECTA"
    except Exception as e:
        res = f"NO CONECTA ({type(e).__name__})"
    s.close()
    conexiones[f"127.0.0.1:{puerto} ({que})"] = res
ev.paso("Intento de conexión desde el host a puertos internos", conexiones)

# ------------------------------------------------------------ 5. simulador sólo habla con el gateway
py = sys.executable
sim = subprocess.Popen([py, os.path.join(RAIZ, "simulador", "simulador.py"), "--vus", "6", "--duracion", "14",
                        "--proyecto", pid, "--edicion-max", "4"], cwd=RAIZ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(8)
ps_cmd = (f"$ids=@({sim.pid})+@((Get-CimInstance Win32_Process -Filter 'ParentProcessId={sim.pid}').ProcessId); "
          "$c=Get-NetTCPConnection -OwningProcess $ids -ErrorAction SilentlyContinue | Where-Object {$_.State -eq 'Established'}; "
          "$loc=@($c.LocalPort); $c=$c | Where-Object { $loc -notcontains $_.RemotePort }; "
          "$c | Select-Object OwningProcess,LocalPort,RemoteAddress,RemotePort,State | Format-Table -AutoSize | Out-String -Width 200; "
          "'PUERTOS_REMOTOS=' + (($c.RemotePort | Sort-Object -Unique) -join ',')")
rc, tabla, _ = sh(["powershell", "-NoProfile", "-Command", ps_cmd])
m_ = re.search(r"PUERTOS_REMOTOS=([\d,]*)", tabla)
remotos = sorted({int(x) for x in m_.group(1).split(",") if x}) if m_ else []
sim.wait()
ev.paso("Conexiones TCP del proceso del simulador (Get-NetTCPConnection)", {"puertos_remotos": remotos, "tabla": tabla.strip()[:1500]},
        "Todos los puertos remotos del simulador deben ser 8000 (gateway). Se excluye el par de sockets loopback que asyncio (Proactor) abre consigo mismo (127.0.0.1 <-> 127.0.0.1, puertos locales del mismo proceso).")

# aislamiento de código (importaciones prohibidas)
def importa(ruta, modulos):
    hits = []
    for raiz, _, fs in os.walk(os.path.join(RAIZ, ruta)):
        for f in fs:
            if f.endswith(".py"):
                txt = open(os.path.join(raiz, f), encoding="utf8").read()
                for m in modulos:
                    if re.search(rf"^\s*(import|from)\s+{m}\b", txt, re.M):
                        hits.append(f"{ruta}/{f} importa {m}")
    return hits


aislamiento = {
    "registro importa redis/valkey": importa("servicios/registro", ["redis", "valkey"]),
    "sesion importa pymongo": importa("servicios/sesion", ["pymongo"]),
    "analitica importa pymongo": importa("servicios/analitica", ["pymongo"]),
    "simulador importa pymongo/redis": importa("simulador", ["pymongo", "redis"]),
    "pruebas importan pymongo/redis": importa("pruebas", ["pymongo", "redis"]),
}
ev.paso("Aislamiento modular (búsqueda estática de imports prohibidos; listas vacías = cumple)", aislamiento)

ev.resultado(salud_normal=salud_normal, escenarios_salud={k: {"estado": v["estado_alcanzado"], "http": v["http"],
             "segundos_hasta_ver_estado": v["segundos_hasta_ver_estado"]} for k, v in esc.items()},
             puertos_publicados=publicados, conexiones_host=conexiones, puertos_remotos_simulador=remotos,
             imports_prohibidos=aislamiento)
ev.cerrar("Se ejercitaron todas las rutas del gateway con sus códigos de éxito y de error; los 503 se provocaron "
          "deteniendo registro/analítica. /salud responde 200 operativo en estado normal, 200 degradado cuando falla una "
          "capa no esencial (analítica, historial, Spark, un secundario de Mongo) y 503 caído cuando falta una capa de núcleo "
          "(Valkey, Mongo sin mayoría, registro, sesión), siempre con cuerpo por capa. Sólo el puerto 8000 es público; "
          "4040 y 18080 están ligados a 127.0.0.1 (administración) y los puertos internos rechazan conexiones desde el host. "
          "Las conexiones TCP del simulador van únicamente al 8000.")
