"""Gráficas (PNG) a partir de los archivos de evidencia: evidence/graficas/*.png

Lee: evidence/p6/resumen_p6.json, evidence/p5/resumen_p5.json, evidence/p8/operaciones.json,
y los JSON más recientes de p2 y p4. Cada gráfica se omite si falta su fuente.
Uso: python pruebas/graficas.py
"""
import glob
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EV = os.path.join(RAIZ, "evidence")
OUT = os.path.join(EV, "graficas")
os.makedirs(OUT, exist_ok=True)
PAL = ["#0072B2", "#E69F00", "#009E73", "#CC79A7", "#D55E00", "#56B4E9", "#F0E442", "#666666"]
plt.rcParams.update({"axes.spines.top": False, "axes.spines.right": False, "font.size": 9, "axes.grid": True, "grid.alpha": 0.25})


def cargar(ruta):
    try:
        with open(ruta, encoding="utf8") as fh:
            return json.load(fh)
    except Exception:
        return None


def ultimo(patron):
    fs = sorted(glob.glob(os.path.join(EV, patron)))
    return cargar(fs[-1]) if fs else None


def guardar(fig, nombre):
    ruta = os.path.join(OUT, nombre)
    fig.tight_layout()
    fig.savefig(ruta, dpi=130)
    plt.close(fig)
    print("escrito", ruta)


def p6():
    d = cargar(os.path.join(EV, "p6", "resumen_p6.json"))
    if not d:
        return
    niv = d["niveles"]
    vus = sorted(niv, key=int)
    eps = [e for e in ("POST /tareas/siguiente", "PUT /tareas/{id}/borrador", "POST /tareas/{id}/enviar", "POST /tareas/{id}/revision",
                       "GET /proyectos/{id}/exportar", "GET /salud") if any(e in niv[v]["por_endpoint"] for v in vus)]
    for pc, titulo in (("p95", "p95"), ("p50", "p50"), ("p99", "p99")):
        fig, ax = plt.subplots(figsize=(8, 4))
        w = 0.8 / len(vus)
        for i, v in enumerate(vus):
            ys = [niv[v]["por_endpoint"].get(e, {}).get(pc) or 0 for e in eps]
            ax.bar([x + i * w for x in range(len(eps))], ys, w, label=f"{v} VUs", color=PAL[i])
        ax.set_xticks([x + w for x in range(len(eps))])
        ax.set_xticklabels([e.replace(" /", "\n/") for e in eps], fontsize=7)
        ax.set_ylabel(f"{titulo} (ms)")
        ax.set_title(f"P6: latencia {titulo} por endpoint y nivel de carga")
        ax.legend()
        guardar(fig, f"p6_latencia_{pc}.png")
    fig, ax = plt.subplots(figsize=(5, 3.6))
    m = [niv[v]["envios_por_min"]["media"] for v in vus]
    lo = [niv[v]["envios_por_min"]["media"] - niv[v]["envios_por_min"]["min"] for v in vus]
    hi = [niv[v]["envios_por_min"]["max"] - niv[v]["envios_por_min"]["media"] for v in vus]
    ax.errorbar([int(v) for v in vus], m, yerr=[lo, hi], marker="o", color=PAL[0], capsize=4)
    ax.set_xlabel("usuarios virtuales")
    ax.set_ylabel("envíos confirmados por minuto")
    ax.set_title("P6: envíos/min (media y rango de 3 corridas)")
    guardar(fig, "p6_envios_por_min.png")
    conts = ["gateway", "registro", "sesion", "mongo1", "mongo2", "mongo3", "valkey", "historial"]
    fig, ax = plt.subplots(figsize=(8, 4))
    w = 0.8 / len(vus)
    for i, v in enumerate(vus):
        ys = [niv[v]["docker_stats"].get(c, {}).get("cpu_pct_media", 0) for c in conts]
        ax.bar([x + i * w for x in range(len(conts))], ys, w, label=f"{v} VUs", color=PAL[i])
    ax.set_xticks([x + w for x in range(len(conts))])
    ax.set_xticklabels(conts, fontsize=8)
    ax.set_ylabel("CPU media (% de un núcleo)")
    ax.set_title("P6: CPU media por contenedor (docker stats)")
    ax.legend()
    guardar(fig, "p6_cpu_contenedores.png")


def p5():
    d = cargar(os.path.join(EV, "p5", "resumen_p5.json"))
    if not d:
        return
    r = d["resumen"]
    cfgs = [("C0_original_sin_compactar_1w", "C0 sin compactar\n1 worker"), ("C1_original_compactado_1w", "C1 compactado\n1 worker"),
            ("C2_final_1w", "C2 final\n1 worker"), ("C3_final_2w", "C3 final\n2 workers")]
    qs = ["q1", "q2", "q3", "q4", "q5"]
    fig, ax = plt.subplots(figsize=(9, 4))
    w = 0.8 / len(cfgs)
    for i, (k, nom) in enumerate(cfgs):
        ag = r.get(k) or {}
        ys = [(ag.get(q, {}).get("caliente") or {}).get("mediana_s") or 0 for q in qs]
        ax.bar([x + i * w for x in range(len(qs))], ys, w, label=nom.replace("\n", " "), color=PAL[i])
    ax.set_xticks([x + 1.5 * w for x in range(len(qs))])
    ax.set_xticklabels([q.upper() for q in qs])
    ax.set_ylabel("tiempo caliente, mediana (s)")
    ax.set_title("P5: Q1-Q5 sobre el conjunto principal (~800 000 eventos)")
    ax.legend(fontsize=8)
    guardar(fig, "p5_tiempos_caliente.png")
    fig, ax = plt.subplots(figsize=(9, 4))
    for i, (k, nom) in enumerate(cfgs):
        ag = r.get(k) or {}
        ys = [ag.get(q, {}).get("fria_s") or 0 for q in qs]
        ax.bar([x + i * w for x in range(len(qs))], ys, w, label=nom.replace("\n", " "), color=PAL[i])
    ax.set_xticks([x + 1.5 * w for x in range(len(qs))])
    ax.set_xticklabels([q.upper() for q in qs])
    ax.set_ylabel("primera ejecución, tras reiniciar (s)")
    ax.set_title("P5: ejecución fría")
    ax.legend(fontsize=8)
    guardar(fig, "p5_tiempos_frio.png")


def p8():
    d = cargar(os.path.join(EV, "p8", "operaciones.json"))
    if not d:
        return
    ops = d["ops"]
    fig, ax = plt.subplots(figsize=(10, 4))
    ok = [(o["t"], o["dur_ms"]) for o in ops if o["status"] in (200, 204)]
    mal = [(o["t"], o["dur_ms"]) for o in ops if o["status"] == 0 or o["status"] >= 500]
    c409 = [(o["t"], o["dur_ms"]) for o in ops if o["status"] == 409]
    ax.scatter(*zip(*ok), s=5, color=PAL[0], label="200/204", alpha=0.5)
    if mal:
        ax.scatter(*zip(*mal), s=12, color=PAL[4], label="503/error")
    if c409:
        ax.scatter(*zip(*c409), s=12, color=PAL[1], label="409")
    ax.set_yscale("log")
    for a in d["acciones"]:
        ax.axvline(a["t_rel"], color="#444", lw=0.7, ls="--")
        ax.text(a["t_rel"], ax.get_ylim()[1], a["accion"][:28], rotation=90, va="top", fontsize=6)
    ax.set_xlabel("segundos desde el inicio")
    ax.set_ylabel("latencia (ms, log)")
    ax.set_title("P8: operaciones de escritura durante las fallas")
    ax.legend(loc="lower right")
    guardar(fig, "p8_linea_de_tiempo.png")


def p2():
    d = ultimo("p2_concurrencia_*.json")
    if not d:
        return
    w = d["resultado"].get("w_majority_vs_w1")
    if not w:
        return
    fig, ax = plt.subplots(figsize=(6, 3.8))
    pcs = ["p50_ms", "p95_ms", "p99_ms"]
    for i, (m, col) in enumerate((("majority", PAL[0]), ("w1", PAL[1]))):
        ax.bar([x + i * 0.4 for x in range(3)], [w[m]["enviar"][p] for p in pcs], 0.4, label=f"w:{m if m == 'majority' else 1}", color=col)
    ax.set_xticks([0.2, 1.2, 2.2])
    ax.set_xticklabels(["p50", "p95", "p99"])
    ax.set_ylabel("latencia de enviar vía gateway (ms)")
    ax.set_title("P2: w:majority vs w:1 (media de rondas)")
    ax.legend()
    guardar(fig, "p2_write_concern.png")


def p4():
    d = ultimo("p4_historial_*.json")
    if not d:
        return
    paso = next((p for p in d["pasos"] if p["titulo"].startswith("Segundos entre HTTP 200")), None)
    if not paso:
        return
    t = paso["datos"]["trials"]
    fig, ax = plt.subplots(figsize=(6, 3.8))
    xs = [x["trial"] for x in t]
    ax.plot(xs, [x["s_hasta_archivo_en_disco"] for x in t], marker="o", color=PAL[1], label="archivo en disco")
    ax.plot(xs, [x["s_hasta_visible_en_spark"] for x in t], marker="o", color=PAL[0], label="visible en Spark")
    ax.set_xlabel("repetición")
    ax.set_ylabel("segundos desde el HTTP 200")
    ax.set_title("P4: del HTTP 200 al evento visible")
    ax.legend()
    guardar(fig, "p4_latencia_visibilidad.png")


if __name__ == "__main__":
    for f in (p2, p4, p5, p6, p8):
        try:
            f()
        except Exception as e:  # noqa: BLE001
            print("fallo", f.__name__, type(e).__name__, e)
