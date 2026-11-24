"""P6. Carga: 10, 40 y 100 usuarios virtuales; 30 s de calentamiento (no medido) + 3 corridas de
90 s por nivel; p50/p95/p99 por endpoint, envíos/min, errores y `docker stats` cada ~5 s.

El simulador (simulador/simulador.py) corre en el host y sólo habla HTTP con el gateway.

Uso: python pruebas/p6_carga.py [--niveles 10,40,100] [--duracion 90] [--calentamiento 30] [--corridas 3]
"""
import argparse
import asyncio
import csv
import json
import os
import subprocess
import sys
import threading
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "simulador"))
import comun as C  # noqa: E402
import simulador as S  # noqa: E402

DIR = None  # se fija en main() según --modo
MODO = "red"


def lanzar(vus, duracion, calentamiento, semilla):
    """Ejecuta el simulador y devuelve (registros, resumen).
    modo host: en este proceso (HTTP a localhost:8000, atraviesa el reenvío de puertos de Docker Desktop).
    modo red : dentro del contenedor `herramientas` de la red p2net (HTTP a gateway:8000)."""
    if MODO == "host":
        ns = S.parser().parse_args(["--vus", str(vus), "--duracion", str(duracion), "--calentamiento", str(calentamiento),
                                    "--proyecto-por-vu", "--semilla", str(semilla)])
        ns.mix = [float(x) for x in ns.mix.split(",")]
        return asyncio.run(S.correr(ns))
    rc, out, err = C.sh(["docker", "exec", "herramientas", "python", "/sim/simulador.py", "--url", "http://gateway:8000",
                         "--vus", str(vus), "--duracion", str(duracion), "--calentamiento", str(calentamiento), "--proyecto-por-vu",
                         "--semilla", str(semilla), "--salida", "/tmp/o.jsonl", "--resumen", "/tmp/o.json"],
                        timeout=int(duracion + calentamiento + 180))
    if rc != 0:
        raise RuntimeError(f"simulador en herramientas rc={rc}: {err[-500:]}")
    C.sh(["docker", "cp", "herramientas:/tmp/o.jsonl", os.path.join(DIR, "_o.jsonl")])
    C.sh(["docker", "cp", "herramientas:/tmp/o.json", os.path.join(DIR, "_o.json")])
    regs = [json.loads(l) for l in open(os.path.join(DIR, "_o.jsonl"), encoding="utf8")]
    res = json.load(open(os.path.join(DIR, "_o.json"), encoding="utf8"))
    os.remove(os.path.join(DIR, "_o.jsonl"))
    os.remove(os.path.join(DIR, "_o.json"))
    return regs, res


class MuestreoDocker(threading.Thread):
    """docker stats --no-stream cada ~5 s -> lista de filas."""

    def __init__(self, etiqueta, periodo=5):
        super().__init__(daemon=True)
        self.etiqueta, self.periodo, self.filas, self.parar = etiqueta, periodo, [], False

    def run(self):
        while not self.parar:
            t = time.time()
            rc, out, _ = C.sh(["docker", "stats", "--no-stream", "--format", "{{json .}}"], timeout=60)
            for l in out.splitlines():
                try:
                    j = json.loads(l)
                except ValueError:
                    continue
                if j["Name"].startswith("ti4601"):
                    continue
                self.filas.append({"etiqueta": self.etiqueta, "ts": round(t, 1), "contenedor": j["Name"],
                                   "cpu_pct": float(j["CPUPerc"].rstrip("%") or 0),
                                   "mem_uso": j["MemUsage"].split("/")[0].strip(), "mem_pct": float(j["MemPerc"].rstrip("%") or 0),
                                   "net_io": j["NetIO"], "block_io": j["BlockIO"], "pids": j["PIDs"]})
            resto = self.periodo - (time.time() - t)
            if resto > 0:
                time.sleep(resto)


def a_mib(s):
    s = s.strip()
    for u, f in (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("B", 1 / 2 ** 20)):
        if s.endswith(u):
            return float(s[:-len(u)]) * f
    return 0.0


def disponibles():
    r = C.mongosh_json("print(JSON.stringify({d:db.tareas.countDocuments({estado:'disponible'})}))")
    return r["d"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--niveles", default="10,40,100")
    ap.add_argument("--duracion", type=float, default=90)
    ap.add_argument("--calentamiento", type=float, default=30)
    ap.add_argument("--corridas", type=int, default=3)
    ap.add_argument("--modo", choices=["red", "host"], default="red",
                    help="red: simulador dentro de la red docker (herramientas); host: desde Windows vía localhost:8000")
    a = ap.parse_args()
    global DIR, MODO
    MODO = a.modo
    sub = "p6" if a.modo == "red" else "p6_host"
    DIR = os.path.join(C.EVIDENCE, sub)
    os.makedirs(DIR, exist_ok=True)
    if a.modo == "red":
        C.dc("--profile", "tools", "up", "-d", "herramientas", timeout=300)
        C.sh(["docker", "exec", "herramientas", "rm", "-rf", "/sim"])
        C.sh(["docker", "cp", os.path.join(C.RAIZ, "simulador"), "herramientas:/sim"])
    niveles = [int(x) for x in a.niveles.split(",")]
    ev = C.Evidencia(
        "p6_carga" if a.modo == "red" else "p6_carga_host",
        "P6. Carga con 10, 40 y 100 usuarios virtuales (simulador " + ("dentro de la red docker, contra gateway:8000)" if a.modo == "red"
                                                                       else "en el host, contra localhost:8000)"),
        "Medir p50/p95/p99 por endpoint, envíos por minuto, tasa de errores y consumo de recursos (docker stats) con 10, 40 y 100 "
        "VUs, con calentamiento de 30 s y 3 corridas de 90 s por nivel.",
        {"niveles_vus": niveles, "calentamiento_s": a.calentamiento, "corridas_por_nivel": a.corridas, "duracion_corrida_s": a.duracion,
         "mezcla_roles": "70 % anotadores, 20 % revisores, 10 % consumidores (exportar/salud; analítica desactivada para no competir por CPU)",
         "anotador": "siguiente -> borrador cada 2 s durante 2-10 s -> enviar; 5 % liberar; 3 % interrupción+recuperar",
         "revisor": "iniciar -> 1-3 s -> aprobar 80 % / rechazar 20 %", "proyectos": "cada anotador elige un proyecto activo al azar (120)",
         "percentiles": "nearest-rank sobre todas las peticiones de las corridas (sin calentamiento)",
         "simulador": ("simulador/simulador.py dentro del contenedor `herramientas` (red p2net), HTTP a http://gateway:8000" if a.modo == "red"
                       else "simulador/simulador.py en el host Windows (Python 3.14), HTTP a localhost:8000 (reenvío de puertos de Docker Desktop)"),
         "lock_ttl_ms": 120000, "mongo_w": "majority"})
    C.esperar_todo_ok(120)
    disp0 = disponibles()
    ev.paso("Tareas disponibles al inicio", {"disponible": disp0})
    todas_filas = []
    resumen_niveles = {}
    for vus in niveles:
        if disponibles() < 5000:
            ev.paso("ADVERTENCIA: quedan pocas tareas disponibles", {"disponible": disponibles()},
                    "Debe reponerse con dataset/cargar_registro.py (anotar fecha, motivo y efecto en la comparabilidad).")
        muestreo = MuestreoDocker(f"vus{vus}")
        muestreo.start()
        # calentamiento: mismo patrón de carga, no medido
        lanzar(vus, 0, a.calentamiento, 7)
        ev.paso(f"{vus} VUs: calentamiento terminado", {"segundos": a.calentamiento})
        corridas = []
        todos = []
        for k in range(1, a.corridas + 1):
            t_ini = time.time()
            regs, res = lanzar(vus, a.duracion, 0, 100 * vus + k)
            jsonl = f"{sub}/sim_{vus}vu_run{k}.jsonl"
            S.guardar(regs, res, os.path.join(C.EVIDENCE, jsonl), os.path.join(DIR, f"resumen_{vus}vu_run{k}.json"))
            ev.archivo_extra(jsonl)
            todos += [r for r in regs if not r["warm"]]
            corridas.append({"corrida": k, "inicio": t_ini, "n_peticiones": res["n_peticiones"], "errores": res["errores"],
                             "tasa_error_pct": res["tasa_error_pct"], "envios_ok": res["envios_ok"],
                             "envios_por_min": res["envios_por_min"], "rps": res["rps"],
                             "por_endpoint": {ep: {k2: v[k2] for k2 in ("n", "p50", "p95", "p99", "errores")}
                                              for ep, v in res["por_endpoint"].items()}})
            ev.paso(f"{vus} VUs, corrida {k}: {res['envios_por_min']} envios/min, {res['rps']} req/s, errores={res['errores']}", corridas[-1])
            time.sleep(5)
        muestreo.parar = True
        muestreo.join(timeout=70)
        todas_filas += muestreo.filas
        # agregado del nivel
        resumen = S.resumir(todos, a.duracion * a.corridas)
        env = [c["envios_por_min"] for c in corridas]
        docker = defaultdict(lambda: {"cpu": [], "mem": []})
        for f in muestreo.filas:
            docker[f["contenedor"]]["cpu"].append(f["cpu_pct"])
            docker[f["contenedor"]]["mem"].append(a_mib(f["mem_uso"]))
        resumen_niveles[str(vus)] = {
            "corridas": corridas, "envios_por_min": {"media": round(sum(env) / len(env), 1), "min": min(env), "max": max(env)},
            "rps_medio": round(sum(c["rps"] for c in corridas) / len(corridas), 1), "n_peticiones": resumen["n_peticiones"],
            "errores": resumen["errores"], "tasa_error_pct": resumen["tasa_error_pct"],
            "por_endpoint": {ep: {k2: v[k2] for k2 in ("n", "p50", "p95", "p99", "media", "max", "status", "errores", "tasa_error_pct")}
                             for ep, v in resumen["por_endpoint"].items()},
            "docker_stats": {n: {"cpu_pct_media": round(sum(v["cpu"]) / len(v["cpu"]), 1), "cpu_pct_max": round(max(v["cpu"]), 1),
                                 "mem_mib_max": round(max(v["mem"]), 1), "muestras": len(v["cpu"])} for n, v in sorted(docker.items())}}
        ev.paso(f"Resumen del nivel {vus} VUs", resumen_niveles[str(vus)])
        time.sleep(10)
        C.esperar_todo_ok(60)

    # archivos de resumen
    with open(os.path.join(DIR, "docker_stats.csv"), "w", newline="", encoding="utf8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(todas_filas[0].keys()))
        w.writeheader()
        w.writerows(todas_filas)
    ev.archivo_extra(f"{sub}/docker_stats.csv")
    with open(os.path.join(DIR, "resumen_p6.json"), "w", encoding="utf8") as fh:
        json.dump({"fecha": ev.data["fecha_hora"], "niveles": resumen_niveles}, fh, indent=1, ensure_ascii=False)
    ev.archivo_extra(f"{sub}/resumen_p6.json")
    # tabla markdown
    eps = sorted({ep for n in resumen_niveles.values() for ep in n["por_endpoint"]})
    md = ["# P6 resumen de carga", "", f"Fecha: {ev.data['fecha_hora']}. Percentiles en ms sobre {a.corridas} corridas x {a.duracion:.0f} s por nivel.", "",
          "| Endpoint | VUs | n | p50 | p95 | p99 | errores |", "|---|---|---|---|---|---|---|"]
    for ep in eps:
        for v in niveles:
            e = resumen_niveles[str(v)]["por_endpoint"].get(ep)
            if e:
                md.append(f"| {ep} | {v} | {e['n']} | {e['p50']} | {e['p95']} | {e['p99']} | {e['errores']} |")
    md += ["", "| VUs | envios/min (media; min-max) | req/s | peticiones | errores (5xx/red) | tasa error % |", "|---|---|---|---|---|---|"]
    for v in niveles:
        n = resumen_niveles[str(v)]
        e = n["envios_por_min"]
        md.append(f"| {v} | {e['media']} ({e['min']}-{e['max']}) | {n['rps_medio']} | {n['n_peticiones']} | {n['errores']} | {n['tasa_error_pct']} |")
    md += ["", "## docker stats (CPU % media / máx; memoria máx MiB)", ""]
    conts = sorted({c for n in resumen_niveles.values() for c in n["docker_stats"]})
    md += ["| Contenedor | " + " | ".join(f"{v} VUs" for v in niveles) + " |", "|---|" + "---|" * len(niveles)]
    for c in conts:
        md.append(f"| {c} | " + " | ".join(
            (lambda d: f"{d['cpu_pct_media']} / {d['cpu_pct_max']} ; {d['mem_mib_max']}" if d else "-")(resumen_niveles[str(v)]["docker_stats"].get(c))
            for v in niveles) + " |")
    C.guardar_texto(f"{sub}/resumen_p6.md", "\n".join(md) + "\n")
    ev.archivo_extra(f"{sub}/resumen_p6.md")
    ev.resultado(niveles={v: {"envios_por_min_media": resumen_niveles[v]["envios_por_min"]["media"], "rps": resumen_niveles[v]["rps_medio"],
                               "errores": resumen_niveles[v]["errores"], "tasa_error_pct": resumen_niveles[v]["tasa_error_pct"],
                               "enviar": {k: resumen_niveles[v]["por_endpoint"].get("POST /tareas/{id}/enviar", {}).get(k) for k in ("p50", "p95", "p99", "n")},
                               "siguiente": {k: resumen_niveles[v]["por_endpoint"].get("POST /tareas/siguiente", {}).get(k) for k in ("p50", "p95", "p99", "n")}}
                         for v in resumen_niveles},
                 tareas_disponibles_inicio=disp0, tareas_disponibles_fin=disponibles(), reposicion_del_dataset="no fue necesaria"
                 if disp0 > 0 and disponibles() > 0 else "ver pasos")
    ev.cerrar("La latencia por endpoint y los envíos por minuto se midieron sobre 3 corridas de 90 s por nivel tras 30 s de calentamiento. "
              f"Las tablas por endpoint, los envíos/min y los docker stats están en {sub}/resumen_p6.md/json; los registros por petición en {sub}/*.jsonl. "
              "El generador de carga corre en el mismo equipo que Docker (12 vCPU lógicos compartidos), de modo que la saturación observada "
              "mezcla la capacidad de la plataforma con la del propio simulador: los números describen este equipo, no un despliegue distribuido.")


if __name__ == "__main__":
    main()
