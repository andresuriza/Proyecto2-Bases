"""Simulador de usuarios virtuales (VUs) de la plataforma de anotación.

Habla EXCLUSIVAMENTE con el gateway por HTTP (regla de aislamiento del enunciado §4:
no importa pymongo ni redis). Los ids de anotadores/revisores/proyectos salen de
`anotadores.json` y `proyectos.json` (volcados una vez del dataset cargado; ver
datos/README.md); si faltan, se inventan ids `sim-a-N` (el equipo queda null).

Roles de VU:
  anotador   siguiente -> borrador cada 2 s durante 2-10 s -> enviar (a veces liberar o
             interrupción + /sesiones/recuperar)
  revisor    toma ids de tareas enviadas en esta misma corrida (cola compartida en memoria)
             -> revision iniciar -> aprobar (80 %) / rechazar con motivo (20 %)
  consumidor exportar (paginado), /salud y, opcionalmente, analítica

Uso (desde el host):
  python simulador/simulador.py --vus 40 --duracion 90 --calentamiento 30 \\
         --salida evidence/p6/sim_40.jsonl [--proyecto <id>] [--url http://localhost:8000]

Cada petición produce un registro JSONL {ts, ep, status, ms, rol, vu, warm}; `ts` es epoch
en segundos. Las peticiones del calentamiento llevan warm=true y se excluyen del resumen.
"""
import argparse
import asyncio
import json
import os
import random
import time
from collections import Counter, defaultdict, deque

import httpx

AQUI = os.path.dirname(os.path.abspath(__file__))
MOTIVOS = ["calidad_baja", "etiqueta_incorrecta", "geometria_imprecisa", "incompleta", "instruccion_incumplida"]


def _cargar(nombre, defecto):
    try:
        with open(os.path.join(AQUI, nombre), encoding="utf8") as fh:
            return json.load(fh)
    except Exception:
        return defecto


def anotacion_falsa(modalidad):
    """Anotación mínima válida según la geometría de la modalidad."""
    if modalidad == "texto":
        return {"spans": [{"inicio": 0, "fin": 5, "etiqueta": "x"}]}
    if modalidad == "lidar":
        return {"cuboides": [{"etiqueta": "lidar#vehiculo", "centro": [1, 2, 3], "dimensiones": [1, 1, 1], "yaw": 0.1}]}
    if modalidad == "video":
        return {"cajas": [{"frame": 1, "bbox": [1, 2, 3, 4], "etiqueta": "x"}]}
    return {"cajas": [{"bbox": [1, 2, 3, 4], "etiqueta": "x"}]}


class Contexto:
    def __init__(self, a):
        self.a = a
        self.t0 = time.time()
        self.t_medicion = self.t0 + a.calentamiento
        self.t_fin = self.t_medicion + a.duracion
        self.parar = False
        self.registros = []
        self.cola_revision = deque()   # ids de tareas enviadas (estado anotada)
        self.proyectos_vistos = set()
        self.envios_ok = 0
        self.cli = httpx.AsyncClient(base_url=a.url, timeout=httpx.Timeout(a.timeout, connect=5.0),
                                     limits=httpx.Limits(max_connections=max(50, a.vus * 3), keepalive_expiry=3.0))  # < keep-alive de uvicorn (5 s): evita ReadError al reutilizar una conexión que el servidor cierra

    def activo(self):
        return not self.parar and time.time() < self.t_fin

    async def req(self, rol, vu, metodo, ruta, plantilla, **kw):
        """Una petición medida. status 0 = error de red/timeout."""
        t = time.time()
        p = time.perf_counter()
        try:
            r = await self.cli.request(metodo, ruta, headers={"X-Anotador-ID": str(vu)}, **kw)
            st = r.status_code
        except Exception as e:  # noqa: BLE001
            r, st, err = None, 0, type(e).__name__
        else:
            err = None
        ms = (time.perf_counter() - p) * 1000
        reg = {"ts": round(t, 3), "ep": f"{metodo} {plantilla}", "status": st,
               "ms": round(ms, 2), "rol": rol, "vu": vu, "warm": t < self.t_medicion}
        if err:
            reg["err"] = err  # tipo de excepción del cliente (p. ej. RemoteProtocolError, ReadTimeout)
        self.registros.append(reg)
        return r, st

    async def dormir(self, s):
        """sleep interrumpible al terminar la corrida."""
        fin = time.time() + s
        while time.time() < fin and self.activo():
            await asyncio.sleep(min(0.25, max(0.0, fin - time.time())))


async def vu_anotador(ctx: Contexto, vu: str, proyecto):
    a = ctx.a
    rnd = random.Random(hash(vu) & 0xFFFFFFFF)
    tarea = token = None
    while ctx.activo():
        r, st = await ctx.req("anotador", vu, "POST", "/tareas/siguiente", "/tareas/siguiente",
                              json={"anotador_id": vu, "proyecto_id": proyecto})
        if st == 204:
            await ctx.dormir(1 + rnd.random())
            continue
        if st != 200:
            await ctx.dormir(0.5 + rnd.random())
            continue
        d = r.json()
        tarea, token = d["tarea"], d["token"]
        ctx.proyectos_vistos.add(tarea["proyecto_id"])
        tid = tarea["_id"]
        dur = rnd.uniform(a.edicion_min, a.edicion_max)
        liberar = rnd.random() < a.p_liberar
        interrumpe = (not liberar) and rnd.random() < a.p_interrupcion
        t_ini, ver = time.time(), 0
        interrumpido = False
        while time.time() - t_ini < dur and ctx.activo():
            await ctx.dormir(min(a.borrador_s, max(0.0, dur - (time.time() - t_ini))))
            if not ctx.activo():
                break
            ver += 1
            await ctx.req("anotador", vu, "PUT", f"/tareas/{tid}/borrador", "/tareas/{id}/borrador",
                          json={"anotador_id": vu, "token": token, "version_local": ver,
                                "contenido": {"parcial": anotacion_falsa(tarea["modalidad"]), "v": ver}})
            if interrumpe and not interrumpido and time.time() - t_ini > dur / 2:
                interrumpido = True  # el usuario "pierde la conexión": deja de enviar borradores
                await ctx.dormir(rnd.uniform(3, 8))
                r2, st2 = await ctx.req("anotador", vu, "POST", "/sesiones/recuperar", "/sesiones/recuperar",
                                        json={"anotador_id": vu})
                if st2 == 200:
                    token = r2.json().get("token", token)
        if liberar or not ctx.activo():
            await ctx.req("anotador", vu, "POST", f"/tareas/{tid}/liberar", "/tareas/{id}/liberar",
                          json={"anotador_id": vu, "token": token})
            continue
        r, st = await ctx.req("anotador", vu, "POST", f"/tareas/{tid}/enviar", "/tareas/{id}/enviar",
                              json={"anotador_id": vu, "token": token,
                                    "anotacion": anotacion_falsa(tarea["modalidad"])})
        if st == 200:
            ctx.envios_ok += 1
            ctx.cola_revision.append(tid)


async def vu_revisor(ctx: Contexto, vu: str):
    a = ctx.a
    rnd = random.Random(hash(vu) & 0xFFFFFFFF)
    while ctx.activo():
        if not ctx.cola_revision:
            await ctx.dormir(0.5)
            continue
        tid = ctx.cola_revision.popleft()
        r, st = await ctx.req("revisor", vu, "POST", f"/tareas/{tid}/revision", "/tareas/{id}/revision",
                              json={"revisor_id": vu, "accion": "iniciar"})
        if st != 200:
            continue
        await ctx.dormir(rnd.uniform(a.revision_min, a.revision_max))
        if rnd.random() < a.p_rechazo:
            cuerpo = {"revisor_id": vu, "accion": "rechazar", "motivo_codigo": rnd.choice(MOTIVOS)}
        else:
            cuerpo = {"revisor_id": vu, "accion": "aprobar"}
        await ctx.req("revisor", vu, "POST", f"/tareas/{tid}/revision", "/tareas/{id}/revision", json=cuerpo)
        # terminar siempre la revisión iniciada, aunque la corrida acabe


async def vu_consumidor(ctx: Contexto, vu: str):
    a = ctx.a
    rnd = random.Random(hash(vu) & 0xFFFFFFFF)
    ultima_ana = time.time()
    while ctx.activo():
        await ctx.dormir(rnd.uniform(1.5, 4))
        if not ctx.activo():
            break
        if a.analitica_cada > 0 and time.time() - ultima_ana >= a.analitica_cada:
            ultima_ana = time.time()
            await ctx.req("consumidor", vu, "GET", f"/analitica/{rnd.choice(['q1', 'q2', 'q4'])}",
                          "/analitica/{q}", params={"conjunto": "principal"})
            continue
        if ctx.proyectos_vistos and rnd.random() < 0.6:
            pid = rnd.choice(sorted(ctx.proyectos_vistos))
            cursor = None
            for _ in range(3):  # hasta 3 páginas
                p = {"limite": 50, **({"cursor": cursor} if cursor else {})}
                r, st = await ctx.req("consumidor", vu, "GET", f"/proyectos/{pid}/exportar",
                                      "/proyectos/{id}/exportar", params=p)
                cursor = r.json().get("siguiente_cursor") if st == 200 else None
                if not cursor:
                    break
        else:
            await ctx.req("consumidor", vu, "GET", "/salud", "/salud")


def percentiles(xs):
    if not xs:
        return {"n": 0, "p50": None, "p95": None, "p99": None, "media": None, "max": None}
    s = sorted(xs)

    def p(q):  # nearest-rank
        import math
        return round(s[max(0, math.ceil(q * len(s)) - 1)], 2)
    return {"n": len(s), "p50": p(0.50), "p95": p(0.95), "p99": p(0.99),
            "media": round(sum(s) / len(s), 2), "max": round(s[-1], 2)}


def resumir(registros, duracion_s=None):
    """Resumen por endpoint sobre los registros NO calentamiento.
    Error = status 0 o >= 500. 409/204/404 se reportan aparte (esperables en el flujo)."""
    medidos = [r for r in registros if not r.get("warm")]
    por_ep = defaultdict(list)
    for r in medidos:
        por_ep[r["ep"]].append(r)
    out = {}
    for ep, rs in sorted(por_ep.items()):
        c = Counter(r["status"] for r in rs)
        n = len(rs)
        errores = sum(v for k, v in c.items() if k == 0 or k >= 500)
        out[ep] = {**percentiles([r["ms"] for r in rs]), "status": {str(k): v for k, v in sorted(c.items())},
                   "errores": errores, "tasa_error_pct": round(100 * errores / n, 3)}
    tot_err = sum(v["errores"] for v in out.values())
    n_tot = len(medidos)
    envios = sum(1 for r in medidos if r["ep"].endswith("/enviar") and r["status"] == 200)
    res = {"n_peticiones": n_tot, "errores": tot_err,
           "tasa_error_pct": round(100 * tot_err / n_tot, 3) if n_tot else None,
           "envios_ok": envios, "por_endpoint": out}
    if duracion_s:
        res["duracion_s"] = duracion_s
        res["envios_por_min"] = round(envios / (duracion_s / 60), 1)
        res["rps"] = round(n_tot / duracion_s, 1)
    return res


async def correr(a):
    """Ejecuta una corrida; devuelve (registros, resumen)."""
    ctx = Contexto(a)
    anot = _cargar("anotadores.json", [])
    anotadores = [x["id"] for x in anot if x["rol"] == "anotador"] or [f"sim-a-{i}" for i in range(500)]
    revisores = [x["id"] for x in anot if x["rol"] == "revisor"] or [f"sim-r-{i}" for i in range(50)]
    proyectos = _cargar("proyectos.json", [])
    rnd = random.Random(a.semilla)
    rnd.shuffle(anotadores)
    rnd.shuffle(revisores)
    n_cons = max(1, round(a.vus * a.mix[2]))
    n_rev = max(1, round(a.vus * a.mix[1]))
    n_ano = max(1, a.vus - n_cons - n_rev)
    tareas = []
    for i in range(n_ano):
        vu = anotadores[i % len(anotadores)]
        proy = a.proyecto or (rnd.choice(proyectos) if (proyectos and a.proyecto_por_vu) else None)
        tareas.append(asyncio.create_task(vu_anotador(ctx, vu, proy)))
    for i in range(n_rev):
        tareas.append(asyncio.create_task(vu_revisor(ctx, revisores[i % len(revisores)])))
    for i in range(n_cons):
        tareas.append(asyncio.create_task(vu_consumidor(ctx, f"consumidor-{i}")))
    while time.time() < ctx.t_fin:
        await asyncio.sleep(0.5)
    ctx.parar = True
    # tiempo de gracia para cerrar (liberar/terminar revisiones) sin colgarse
    await asyncio.wait(tareas, timeout=30)
    for t in tareas:
        t.cancel()
    await ctx.cli.aclose()
    resumen = resumir(ctx.registros, a.duracion)
    resumen["config"] = {"vus": a.vus, "anotadores": n_ano, "revisores": n_rev, "consumidores": n_cons,
                         "duracion_s": a.duracion, "calentamiento_s": a.calentamiento,
                         "edicion_s": [a.edicion_min, a.edicion_max], "borrador_s": a.borrador_s,
                         "proyecto": a.proyecto, "proyecto_por_vu": a.proyecto_por_vu}
    resumen["inicio_medicion"] = ctx.t_medicion
    resumen["fin_medicion"] = ctx.t_fin
    return ctx.registros, resumen


def parser():
    ap = argparse.ArgumentParser(description="Simulador de VUs (sólo HTTP contra el gateway)")
    ap.add_argument("--url", default=os.environ.get("GATEWAY_URL", "http://localhost:8000"))
    ap.add_argument("--vus", type=int, default=10)
    ap.add_argument("--duracion", type=float, default=60, help="segundos medidos")
    ap.add_argument("--calentamiento", type=float, default=0, help="segundos previos NO medidos")
    ap.add_argument("--proyecto", default=None, help="proyecto_id fijo para todos los anotadores")
    ap.add_argument("--proyecto-por-vu", action="store_true",
                    help="cada anotador trabaja un proyecto activo al azar (de proyectos.json)")
    ap.add_argument("--mix", default="0.7,0.2,0.1", help="fracción anotadores,revisores,consumidores")
    ap.add_argument("--edicion-min", type=float, default=2.0)
    ap.add_argument("--edicion-max", type=float, default=10.0)
    ap.add_argument("--borrador-s", type=float, default=2.0)
    ap.add_argument("--revision-min", type=float, default=1.0)
    ap.add_argument("--revision-max", type=float, default=3.0)
    ap.add_argument("--p-liberar", type=float, default=0.05)
    ap.add_argument("--p-interrupcion", type=float, default=0.03)
    ap.add_argument("--p-rechazo", type=float, default=0.2)
    ap.add_argument("--analitica-cada", type=float, default=0, help="s entre consultas analíticas por consumidor (0=nunca)")
    ap.add_argument("--timeout", type=float, default=30)
    ap.add_argument("--semilla", type=int, default=1)
    ap.add_argument("--salida", default="", help="archivo JSONL con un registro por petición")
    ap.add_argument("--resumen", default="", help="archivo JSON con el resumen")
    return ap


def main(argv=None):
    a = parser().parse_args(argv)
    a.mix = [float(x) for x in a.mix.split(",")]
    registros, resumen = asyncio.run(correr(a))
    guardar(registros, resumen, a.salida, a.resumen)
    print(json.dumps({k: v for k, v in resumen.items() if k != "por_endpoint"}, indent=1))
    for ep, v in resumen["por_endpoint"].items():
        print(f"{ep:38s} n={v['n']:6d} p50={v['p50']:8} p95={v['p95']:8} p99={v['p99']:8} err={v['errores']}")


def guardar(registros, resumen, salida="", resumen_path=""):
    if salida:
        os.makedirs(os.path.dirname(os.path.abspath(salida)), exist_ok=True)
        with open(salida, "w", encoding="utf8") as fh:
            for r in registros:
                fh.write(json.dumps(r, separators=(",", ":")) + "\n")
    if resumen_path:
        with open(resumen_path, "w", encoding="utf8") as fh:
            json.dump(resumen, fh, indent=1, ensure_ascii=False)


if __name__ == "__main__":
    main()
