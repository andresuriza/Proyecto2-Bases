"""Utilidades compartidas por las pruebas P1..P8 (se ejecutan en el HOST contra el gateway).

- Sólo HTTP contra http://localhost:8000 para el flujo funcional.
- Inyección de fallas y mediciones de infraestructura con `docker compose` / `docker exec`.
- Evidencia: cada prueba escribe evidence/<prueba>_<AAAAMMDDThhmm>.json y .md con
  fecha/hora, objetivo, configuración, comando, resultado observado, interpretación y archivo.
"""
import json
import math
import os
import platform
import subprocess
import sys
import time
from datetime import datetime

import httpx

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVIDENCE = os.path.join(RAIZ, "evidence")
GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:8000")
LAGO = os.environ.get("LAGO_LOCAL", "C:/p2work/datalake")
LAGO_GRANDE = os.environ.get("LAGO_GRANDE_LOCAL", "C:/p2work/datalake_grande")
os.makedirs(EVIDENCE, exist_ok=True)


# ------------------------------------------------------------------ shell
def sh(cmd, timeout=300, entrada=None, check=False, env=None):
    """Ejecuta un comando (lista o str). Devuelve (rc, stdout, stderr). Nunca lanza por rc != 0."""
    e = dict(os.environ)
    e.update(env or {})
    try:
        p = subprocess.run(cmd, cwd=RAIZ, capture_output=True, timeout=timeout, input=entrada,
                           text=True, encoding="utf-8", errors="replace", shell=isinstance(cmd, str), env=e)
        rc, out, err = p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as ex:
        rc, out, err = 124, (ex.stdout or ""), f"timeout {timeout}s"
    if check and rc != 0:
        raise RuntimeError(f"{cmd} -> rc={rc}\n{err}")
    return rc, out, err


def dc(*args, timeout=300, env=None, entrada=None):
    return sh(["docker", "compose", *args], timeout=timeout, env=env, entrada=entrada)


def mongosh(js, db="plataforma", contenedor=None, timeout=120):
    """Ejecuta JS en mongosh dentro de un nodo vivo (el primero que responda)."""
    for c in ([contenedor] if contenedor else ["mongo1", "mongo2", "mongo3"]):
        rc, out, err = sh(["docker", "exec", c, "mongosh", "--quiet", db, "--eval", js], timeout=timeout)
        if rc == 0:
            return out.strip()
    return f"ERROR: {err.strip()}"


def mongosh_json(js, **kw):
    """El JS debe terminar imprimiendo JSON.stringify(...)."""
    salida = mongosh(js, **kw)
    for linea in reversed(salida.splitlines()):
        linea = linea.strip()
        if linea.startswith("{") or linea.startswith("["):
            try:
                return json.loads(linea)
            except ValueError:
                continue
    raise RuntimeError("salida de mongosh no es JSON: " + salida[:400])


def interno(servicio, metodo, ruta, puerto, cuerpo=None, timeout=120, python="python"):
    """Llama a un servicio interno (no publicado) ejecutando urllib DENTRO de su contenedor.
    Sólo para pruebas/administración (el flujo funcional va por el gateway)."""
    codigo = (
        "import json,sys,urllib.request as u,urllib.error as e\n"
        f"d={json.dumps(cuerpo)!r}\n"
        f"rq=u.Request('http://localhost:{puerto}{ruta}',method='{metodo}',"
        "data=(d.encode() if d!='null' else None),headers={'content-type':'application/json'})\n"
        "try:\n r=u.urlopen(rq,timeout=%d);print(r.status);print(r.read().decode())\n"
        "except e.HTTPError as x:\n print(x.code);print(x.read().decode())\n" % timeout)
    rc, out, err = dc("exec", "-T", servicio, python, "-c", codigo, timeout=timeout + 30)
    lineas = out.strip().split("\n", 1)
    try:
        return int(lineas[0]), json.loads(lineas[1]) if len(lineas) > 1 and lineas[1].strip() else None
    except Exception:
        return -1, {"raw": out[:500], "err": err[:500]}


def estado_contenedor(nombre):
    rc, out, _ = sh(["docker", "inspect", "-f", "{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{end}}", nombre])
    return out.strip()


# ------------------------------------------------------------------ http (host -> gateway)
def cliente(timeout=30.0):
    return httpx.Client(base_url=GATEWAY, timeout=timeout)


def post_json(c, ruta, cuerpo, **kw):
    return c.post(ruta, json=cuerpo, **kw)


def crear_proyecto(c, nombre, modalidad="imagen", sla=24, n_tareas=0, **extra):
    r = c.post("/proyectos", json={"nombre": nombre, "modalidad": modalidad, "sla_horas": sla,
                                    "taxonomia": [{"etiqueta": "x", "instruccion": "prueba"}], **extra})
    r.raise_for_status()
    p = r.json()
    ids = []
    if n_tareas:
        r = c.post(f"/proyectos/{p['_id']}/lotes", json={"nombre": "lote-prueba", "tareas": [
            {"archivo": {"uri": f"s3://pruebas/{nombre}/{i}.jpg", "sha256": f"{i:064x}", "bytes": 1000 + i,
                         "mime": "image/jpeg", "meta": {"w": 640, "h": 480}}} for i in range(n_tareas)]})
        r.raise_for_status()
        ids = r.json()["tarea_ids"]
    return p, ids


def esperar_salud(estados=("operativo", "degradado"), timeout=90, c=None, exigir_capas_ok=()):
    """Espera hasta que /salud tenga alguno de los estados. Devuelve (segundos, cuerpo)."""
    t0 = time.time()
    c = c or httpx.Client(base_url=GATEWAY, timeout=5.0)
    ultimo = None
    while time.time() - t0 < timeout:
        try:
            r = c.get("/salud")
            ultimo = r.json()
            if ultimo.get("estado") in estados and all(
                    ultimo["capas"].get(k, {}).get("estado") == "ok" for k in exigir_capas_ok):
                return round(time.time() - t0, 1), ultimo
        except Exception:
            pass
        time.sleep(1)
    return None, ultimo


def esperar_todo_ok(timeout=180):
    """Estado normal completo: /salud operativo y todas las capas ok."""
    t0 = time.time()
    c = httpx.Client(base_url=GATEWAY, timeout=5.0)
    ultimo = None
    while time.time() - t0 < timeout:
        try:
            ultimo = c.get("/salud").json()
            if ultimo["estado"] == "operativo" and all(v["estado"] == "ok" for v in ultimo["capas"].values()):
                return round(time.time() - t0, 1), ultimo
        except Exception:
            pass
        time.sleep(1.5)
    return None, ultimo


def resumen_salud(cuerpo):
    if not cuerpo:
        return None
    return {"estado": cuerpo.get("estado"),
            "capas": {k: v.get("estado") + (f" ({v.get('detalle')})" if v.get("detalle") else "")
                      for k, v in cuerpo.get("capas", {}).items()}}


# ------------------------------------------------------------------ estadística
def pct(xs, q):
    """Percentil nearest-rank (q en 0..1)."""
    if not xs:
        return None
    s = sorted(xs)
    return s[max(0, math.ceil(q * len(s)) - 1)]


def lat(xs_ms):
    """p50/p95/p99 + n (ms)."""
    if not xs_ms:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None}
    r = lambda v: round(v, 2)
    return {"n": len(xs_ms), "p50_ms": r(pct(xs_ms, .5)), "p95_ms": r(pct(xs_ms, .95)),
            "p99_ms": r(pct(xs_ms, .99)), "media_ms": r(sum(xs_ms) / len(xs_ms)), "max_ms": r(max(xs_ms))}


# ------------------------------------------------------------------ evidencia
def ahora_str():
    return datetime.now().strftime("%Y%m%dT%H%M")


def entorno_resumen():
    """Características de la máquina para cada archivo de evidencia (detalle en evidence/entorno.txt)."""
    info = {"so": platform.platform(), "python_host": sys.version.split()[0], "cpu_logicos": os.cpu_count()}
    rc, out, _ = sh(["docker", "info", "--format", "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}|{{.OperatingSystem}}"])
    if rc == 0 and "|" in out:
        v, n, m, so = out.strip().split("|", 3)
        info.update(docker=v, docker_cpus=int(n), docker_mem_gib=round(int(m) / 2**30, 1), docker_so=so)
    info["detalle"] = "evidence/entorno.txt"
    return info


class Evidencia:
    """Acumula pasos de una prueba y escribe <prueba>_<fecha>.json y .md."""

    def __init__(self, prueba, titulo, objetivo, configuracion, comando=None):
        self.prueba = prueba
        self.inicio = datetime.now()
        self.stamp = self.inicio.strftime("%Y%m%dT%H%M")
        self.data = {
            "prueba": prueba, "titulo": titulo, "fecha_hora": self.inicio.isoformat(timespec="seconds"),
            "objetivo": objetivo, "configuracion": configuracion,
            "comando": comando or ("python " + " ".join(sys.argv)),
            "maquina": entorno_resumen(), "resultado": {}, "pasos": [], "interpretacion": "",
            "archivos": [],
        }
        self.lineas_md = []

    def paso(self, titulo, datos=None, nota=None):
        print(f"[{datetime.now():%H:%M:%S}] {titulo}", flush=True)
        self.data["pasos"].append({"titulo": titulo, "hora": datetime.now().strftime("%H:%M:%S"),
                                   "datos": datos, "nota": nota})

    def resultado(self, **kw):
        self.data["resultado"].update(kw)

    def archivo_extra(self, ruta_rel):
        self.data["archivos"].append(ruta_rel)

    def cerrar(self, interpretacion):
        d = self.data
        d["interpretacion"] = interpretacion
        d["duracion_s"] = round((datetime.now() - self.inicio).total_seconds(), 1)
        base = f"{self.prueba}_{self.stamp}"
        d["archivo"] = f"evidence/{base}.json"
        d["archivos"] = [f"evidence/{base}.json", f"evidence/{base}.md"] + d["archivos"]
        with open(os.path.join(EVIDENCE, base + ".json"), "w", encoding="utf8") as fh:
            json.dump(d, fh, indent=1, ensure_ascii=False, default=str)
        with open(os.path.join(EVIDENCE, base + ".md"), "w", encoding="utf8") as fh:
            fh.write(self._md())
        print(f"evidencia escrita: evidence/{base}.json / .md")
        return base

    def _md(self):
        d = self.data
        m = d["maquina"]
        o = [f"# {d['titulo']}", "",
             f"- **Fecha y hora:** {d['fecha_hora']} (duración {d.get('duracion_s')} s)",
             f"- **Comando ejecutado:** `{d['comando']}`",
             f"- **Máquina:** {m.get('so')}; CPU lógicos={m.get('cpu_logicos')}; Docker {m.get('docker')} "
             f"({m.get('docker_cpus')} vCPU, {m.get('docker_mem_gib')} GiB); detalle en `evidence/entorno.txt`",
             f"- **Archivos que respaldan:** " + ", ".join(f"`{a}`" for a in d["archivos"]), "",
             "## Objetivo", "", d["objetivo"], "", "## Configuración", "",
             "```json", json.dumps(d["configuracion"], indent=1, ensure_ascii=False, default=str), "```", "",
             "## Resultado observado", ""]
        if d["resultado"]:
            o += ["```json", json.dumps(d["resultado"], indent=1, ensure_ascii=False, default=str), "```", ""]
        for p in d["pasos"]:
            o.append(f"### {p['titulo']}  _({p['hora']})_")
            if p.get("nota"):
                o += ["", p["nota"]]
            if p.get("datos") is not None:
                txt = json.dumps(p["datos"], indent=1, ensure_ascii=False, default=str)
                if len(txt) > 6000:
                    txt = txt[:6000] + "\n... (truncado; ver el .json)"
                o += ["", "```json", txt, "```"]
            o.append("")
        o += ["## Interpretación", "", d["interpretacion"], ""]
        return "\n".join(o)


def guardar_texto(nombre, contenido):
    ruta = os.path.join(EVIDENCE, nombre)
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", encoding="utf8") as fh:
        fh.write(contenido)
    return "evidence/" + nombre.replace("\\", "/")


def restaurar_estado_normal(ev=None, timeout=240):
    """Deja el stack en el estado conocido: servicios del núcleo arriba, valores por defecto."""
    dc("up", "-d", "mongo1", "mongo2", "mongo3", "valkey", "registro", "sesion", "gateway", "historial",
       "analitica", "spark-master", "spark-worker-1", timeout=300)
    t, cuerpo = esperar_todo_ok(timeout)
    if ev:
        ev.paso("Restauración al estado normal", {"segundos_hasta_todo_ok": t, "salud": resumen_salud(cuerpo)})
    return t, cuerpo


# ------------------------------------------------------------------ lectura directa del lago (host, sólo lectura, pyarrow)
def lago_dataset(ruta=None):
    import pyarrow.dataset as ds
    ruta = (ruta or LAGO) + "/eventos"
    return ds.dataset(ruta, format="parquet", partitioning="hive", exclude_invalid_files=False)


def eventos_de(tarea_ids, ruta=None):
    """Filas del lago (lista de dicts) para unas tareas. Lectura directa de Parquet con pyarrow."""
    import pyarrow.dataset as ds
    if isinstance(tarea_ids, str):
        tarea_ids = [tarea_ids]
    d = lago_dataset(ruta)
    cols = ["event_id", "tarea_id", "estado", "ts", "version", "motivo", "anotador_id", "revisor_id"]
    t = d.to_table(columns=cols, filter=ds.field("tarea_id").isin(list(tarea_ids)))
    return t.to_pylist()


def valkey_cli(*args):
    rc, out, err = sh(["docker", "exec", "valkey", "valkey-cli", *args])
    return out.strip()


def mongosh_script_json(js, db="plataforma", timeout=300):
    """Como mongosh_json pero el script va en un archivo (evita el límite de longitud de la línea de comandos
    cuando incluye miles de ids): docker cp al nodo y mongosh <archivo>."""
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf8") as fh:
        fh.write(js)
        ruta = fh.name
    try:
        for nodo in ("mongo1", "mongo2", "mongo3"):
            rc, _, _ = sh(["docker", "cp", ruta, f"{nodo}:/tmp/script_p.js"])
            if rc != 0:
                continue
            rc, out, err = sh(["docker", "exec", nodo, "mongosh", "--quiet", db, "/tmp/script_p.js"], timeout=timeout)
            if rc == 0:
                for linea in reversed(out.splitlines()):
                    if linea.strip().startswith(("{", "[")):
                        return json.loads(linea.strip())
    finally:
        os.remove(ruta)
    raise RuntimeError("mongosh_script_json falló")


def compactar_lago_principal():
    """Compacta con POST /compactar (historial) cada mes con más de un archivo. Devuelve lista de resultados."""
    import glob
    res = []
    for d in sorted(glob.glob(LAGO + "/eventos/mes=*")):
        mes = os.path.basename(d).split("=")[1]
        n = len(glob.glob(d + "/*.parquet"))
        if n > 1:
            st, r = interno("historial", "POST", f"/compactar?mes={mes}", 8003, timeout=600)
            res.append({"mes": mes, "archivos_antes": n, "http": st, "origenes": (r or {}).get("origenes"),
                        "duplicados_eliminados": (r or {}).get("duplicados_eliminados")})
    return res


if __name__ == "__main__":
    print(json.dumps(compactar_lago_principal(), indent=1))
