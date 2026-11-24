"""Genera las tablas LaTeX del informe (informe/tablas/*.tex) leyendo SOLO archivos de evidence/.
Uso: python pruebas/informe_tablas.py   (desde la raíz del repositorio)
Todas las cifras se copian de los JSON de evidencia; sólo se calculan los promedios simples
indicados en la propia tabla (p. ej. media de tres pares en w:majority vs w:1 directo)."""
import json
import os
import re
import statistics as st

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EV = os.path.join(RAIZ, "evidence")
OUT = os.path.join(RAIZ, "informe", "tablas")
os.makedirs(OUT, exist_ok=True)


def cargar(*partes):
    with open(os.path.join(EV, *partes), encoding="utf8") as fh:
        return json.load(fh)


def esc(s):
    s = str(s)
    for a, b in (("\\", r"\textbackslash{}"), ("&", r"\&"), ("%", r"\%"), ("_", r"\_"), ("#", r"\#"),
                 ("{", r"\{"), ("}", r"\}"), ("$", r"\$")):
        s = s.replace(a, b)
    return s


def tt(s):
    return r"\texttt{" + esc(s) + "}"


def escribir(nombre, lineas):
    with open(os.path.join(OUT, nombre), "w", encoding="utf8") as fh:
        fh.write("\n".join(lineas) + "\n")


def tabla(cols, cabecera, filas, nombre):
    L = [r"\begin{tabular}{" + cols + "}", r"\toprule", " & ".join(cabecera) + r" \\", r"\midrule"]
    L += [" & ".join(f) + r" \\" for f in filas]
    L += [r"\bottomrule", r"\end{tabular}"]
    escribir(nombre, L)


def num(x, d=None):
    if x is None:
        return "--"
    if d is not None:
        return f"{x:.{d}f}"
    return f"{x}"


def miles(n):
    return f"{n:,}".replace(",", r"\,")


# ---------------------------------------------------------------- P1
p1 = cargar("p1_api_20261030T2359.json")
esc_s = p1["resultado"]["escenarios_salud"]
nombres = {"degradado_analitica": "analitica detenida", "degradado_historial": "historial detenido",
           "degradado_spark_worker": "spark-worker-1 detenido", "degradado_mongo_secundario": "un secundario de Mongo detenido",
           "caido_valkey": "valkey detenido", "caido_mongo_sin_mayoria": "mongo2 y mongo3 detenidos (sin primario)",
           "caido_registro": "registro detenido", "caido_sesion": "sesion detenida"}
tabla("@{}lccr@{}", ["Escenario", "Estado", "HTTP", r"s hasta verlo"],
      [[esc(nombres[k]), v["estado"], str(v["http"]), num(v["segundos_hasta_ver_estado"], 1)] for k, v in esc_s.items()],
      "p1_salud.tex")
casos = [p for p in p1["pasos"] if p["titulo"].startswith("Tabla de rutas")][0]["datos"]
grupos = {}
for c in casos:
    r = re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "{id}", c["ruta"]).replace("/no-existe/", "/{id}/")
    grupos.setdefault((c["metodo"], r), []).append(c["obtenido"])
filas = []
for (m, r), v in grupos.items():
    filas.append([tt(f"{m} {r}"), str(len(v)), ", ".join(str(x) for x in sorted(set(v)))])
tabla("@{}lcl@{}", ["Ruta", "Casos", "Códigos HTTP observados"], filas, "p1_rutas.tex")
assert p1["resultado"]["casos_total"] == len(casos) == 40 and p1["resultado"]["casos_ok"] == 40

# ---------------------------------------------------------------- P2
p2 = cargar("p2_concurrencia_20261030T2333.json")
rs = p2["resultado"]
tabla("@{}cccc@{}", ["Ronda", r"Códigos HTTP", r"Dispersión del inicio (ms)", "Cumple"],
      [[str(r["ronda"]), esc(", ".join(f"{k}: {v}" for k, v in sorted(r["codigos"].items(), reverse=True))),
        num(r["dispersion_inicio_ms"], 1), "sí" if r["cumple"] else "no"] for r in rs["rondas_concurrentes"]],
      "p2_rondas.tex")
w = rs["w_majority_vs_w1"]
tabla("@{}lrrrrr@{}", ["Modo", "p50", "p95", "p99", "n", "ciclos/s"],
      [[("w:majority" if k == "majority" else "w:1"), num(w[k]["enviar"]["p50_ms"], 2), num(w[k]["enviar"]["p95_ms"], 2),
        num(w[k]["enviar"]["p99_ms"], 2), str(w[k]["enviar"]["n_total"]), num(w[k]["ciclos_por_s_media"], 1)]
       for k in ("majority", "w1")], "p2_w_gateway.tex")
dire = rs["w_directo_en_mongo"]
filas = []
for modo in ("majority", "1"):
    xs = [x for x in dire if x["w"] == modo]
    filas.append([("w:majority" if modo == "majority" else "w:1"), str(sum(x["n"] for x in xs)),
                  num(st.mean(x["media_ms"] for x in xs), 2), num(st.mean(x["p50_ms"] for x in xs), 2),
                  num(st.mean(x["p95_ms"] for x in xs), 2), num(st.mean(x["p99_ms"] for x in xs), 2)])
tabla("@{}lrrrrr@{}", ["Modo", "n", "media", "p50", "p95", "p99"], filas, "p2_w_directo.tex")
exp = [p for p in p2["pasos"] if p["titulo"].startswith("explain")][0]["datos"]
exp_old = [p for p in cargar("p2_concurrencia_20261030T2121.json")["pasos"] if p["titulo"].startswith("explain")][0]["datos"]
etiq = {
    "1_asignar_con_proyecto": ("Asignar (con proyecto)", "tareas_proyecto_estado_prioridad"),
    "2_asignar_sin_proyecto": ("Asignar (sin proyecto)", "tareas_estado_prioridad_expira"),
    "3_reanudar_tarea_abierta_del_anotador": ("Reanudar tarea abierta", "tareas_anotador_estado"),
    "4_tareas_de_un_batch": ("Tareas de un batch", "tareas_batch"),
    "5_proyectos_activos_del_cliente": ("Proyectos del cliente", "proyectos_cliente_estado"),
    "6_EXTRA_tareas_abiertas_vencidas": ("Abiertas vencidas (re-oferta)", "tareas_estado_prioridad_expira"),
    "7_exportar_aprobadas_paginado": ("Exportar aprobadas", "tareas_proyecto_estado_prioridad"),
}


def buscar(d, pref):
    return [v for k, v in d["explain"].items() if k.startswith(pref)][0]


filas = []
for pref, (nom, idx) in etiq.items():
    v = buscar(exp, pref)
    filas.append([esc(nom), tt(idx), str(v["keysExaminadas"]), str(v["docsExaminados"]), str(v["ms"])])
tabla("@{}llrrr@{}", ["Consulta", r"Índice usado", "Claves", "Docs", "ms"], filas, "p2_explain.tex")
v1 = buscar(exp, "1b")
v6 = buscar(exp, "6b")
v2 = buscar(exp, "2_")
v2o = buscar(exp_old, "2_")
escribir("p2_explain_antes_despues.tex", [
    r"\begin{tabular}{@{}p{8.6cm}rrr@{}}", r"\toprule",
    r"Consulta / plan & Claves & Docs & ms \\", r"\midrule",
    f"Asignar sin proyecto, índice previo \\texttt{{tareas\\_estado\\_expira}} & {v2o['keysExaminadas']} & {v2o['docsExaminados']} & {v2o['ms']} \\\\",
    f"Asignar sin proyecto, índice final \\texttt{{tareas\\_estado\\_prioridad\\_expira}} & {v2['keysExaminadas']} & {v2['docsExaminados']} & {v2['ms']} \\\\",
    f"Asignar con proyecto, COLLSCAN forzado ({exp['total_tareas']} tareas) & {v1['keysExaminadas']} & {v1['docsExaminados']} & {v1['ms']} \\\\",
    f"Re-oferta de vencidas, COLLSCAN forzado ({exp['total_tareas']} tareas) & {v6['keysExaminadas']} & {v6['docsExaminados']} & {v6['ms']} \\\\",
    r"\bottomrule", r"\end{tabular}"])

# ---------------------------------------------------------------- P3
p3 = cargar("p3_sesion_20261030T2331.json")
inter = [p for p in p3["pasos"] if p["titulo"].startswith("Interrupción")]
tabla("@{}crrrc@{}", ["Intento", r"Edición antes de matar (s)", r"Borradores confirmados", r"Edad reportada (ms)", r"$\le$ 5 s"],
      [[str(p["datos"]["trial"]), num(p["datos"]["edicion_antes_de_matar_s"], 2), str(p["datos"]["borradores_confirmados"]),
        str(p["datos"]["edad_borrador_ms_reportada"]), "sí" if p["datos"]["cumple_edad_max_5s"] else "no"] for p in inter],
      "p3_borrador.tex")
ab = [p for p in p3["pasos"] if p["titulo"].startswith("Abandono")][0]["datos"]
tabla("@{}rc@{}", [r"s desde el último latido de A", r"HTTP de B"],
      [[num(s, 1), str(h)] for s, h in ab["sondeos_de_B [s desde el ultimo latido de A, http]"]], "p3_reoferta.tex")

# ---------------------------------------------------------------- P4
p4 = cargar("p4_historial_20261030T2216.json")
vis = [p for p in p4["pasos"] if p["titulo"].startswith("Segundos entre HTTP 200")][0]["datos"]["trials"]
tabla("@{}crrr@{}", ["Intento", r"HTTP enviar", r"s hasta archivo", r"s hasta visible en Spark"],
      [[str(t["trial"]), str(t["http_enviar"]), num(t["s_hasta_archivo_en_disco"], 2), num(t["s_hasta_visible_en_spark"], 2)] for t in vis],
      "p4_visibilidad.tex")

p4f = cargar("p4_frescura_q1_20261031T0124.json")
mf = [p for p in p4f["pasos"] if p["titulo"].startswith("Resultado por muestra")][0]["datos"]["muestras"]
tabla("@{}crrrr@{}", ["Muestra", r"s hasta archivo", r"s hasta Q1 visible", r"Consultas de Q1", r"Duración de la consulta (s)"],
      [[str(t["muestra"]), num(t["s_hasta_archivo_en_disco"], 2), num(t["s_hasta_q1_visible"], 2), str(t["consultas_q1"]), num(t["tiempo_s_q1_ultima"], 2)] for t in mf],
      "p4_frescura_q1.tex")

# ---------------------------------------------------------------- P5
p5d = cargar("p5_analitica_20261030T2150.json")
p5 = p5d["resultado"]
desc = [("C0", "C0 sin compactar (272 archivos), 1 worker, código original"),
        ("C1", "C1 compactado (12 archivos), 1 worker, código original"),
        ("C2", "C2 compactado, 1 worker, filtro antes de deduplicar"),
        ("C3", "C3 compactado, 2 workers, filtro antes de deduplicar")]


def celda(c, q):
    return f"{c[q]['fria_s']} / {c[q]['caliente']['mediana_s']}"


filas = [[esc(t), *[celda(p5[k], q) for q in ("q1", "q2", "q3", "q4", "q5")], str(p5[k]["q1"]["caliente"]["n"])] for k, t in desc]
tabla("@{}p{3.7cm}rrrrrr@{}", ["Configuración (conjunto principal)", "Q1", "Q2", "Q3", "Q4", "Q5", r"$n_{cal}$"], filas, "p5_principal.tex")
claves_p5 = list(p5.keys())
print("claves P5:", claves_p5)
grande = {}
for p in p5d["pasos"]:
    if p["titulo"].startswith("G1 resultados") or p["titulo"].startswith("G2 resultados"):
        grande[p["titulo"][:2]] = p["datos"]["filas"]
filas = []
for k, t in (("G1", "G1 grande 10 M, 1 worker"), ("G2", "G2 grande 10 M, 2 workers")):
    fs = grande[k]
    cel = []
    for q in ("q1", "q2", "q3", "q4", "q5"):
        xs = [f for f in fs if f["q"] == q]
        fria = [f for f in xs if f["ronda"] == 1][0]["tiempo_s"]
        cal = [f["tiempo_s"] for f in xs if f["ronda"] != 1]
        cel.append(f"{fria} / {st.median(cal)}")
    filas.append([esc(t), *cel, str(len(cal))])
tabla("@{}p{3.7cm}rrrrrr@{}", ["Configuración (conjunto grande)", "Q1", "Q2", "Q3", "Q4", "Q5", r"$n_{cal}$"], filas, "p5_grande.tex")
nf = {}
for p in p5d["pasos"]:
    if p["titulo"] == "C2 resultados":
        for f in p["datos"]["filas"]:
            nf[f["q"]] = f["n_filas"]
escribir("p5_nfilas.tex", [", ".join(f"{q.upper()}: {nf[q]}" for q in sorted(nf))])

# ---------------------------------------------------------------- P5b (resultados completos del conjunto principal)
import glob as _glob
_f = sorted(_glob.glob(os.path.join(EV, "p5_resultados_[0-9]*.json")))
if _f:
    pr = cargar(os.path.basename(_f[-1]))["resultado"]
    tm = pr["tiempos"]
    lg = pr["lago_al_medir"]
    desc_q = {"q1": "aprobadas por día y modalidad (90 días)", "q2": "proporción aprobadas por equipo y mes",
              "q3": "duración media y p95 por modalidad y tipo", "q4": "proporción rechazadas por cliente y proyecto",
              "q5": "p50/p95 y \\% SLA incumplido por cliente"}
    tabla("@{}llrrr@{}", ["Q", "Contenido", "Filas", "tiempo (s)", "Pared cliente (ms)"],
          [[q.upper(), desc_q[q], str(tm[q]["n_filas"]), str(tm[q]["tiempo_s"]), str(tm[q]["pared_cliente_ms"])] for q in ("q1", "q2", "q3", "q4", "q5")],
          "p5_res_tiempos.tex")
    tabla("@{}llrrr@{}", ["Modalidad", "Tipo de tarea", "$n$", "Media (s)", "p95 (s)"],
          [[x["modalidad"], tt(x["tipo_tarea"]), miles(x["n"]), f"{x['dur_media_s']:.1f}", f"{x['dur_p95_s']:.1f}"]
           for x in pr["resumen"]["q3"]["filas"]], "p5_res_q3.tex")
    q5 = pr["resumen"]["q5"]
    tabla("@{}lrrrr@{}", ["Cliente (8 primeros caracteres)", "$n$", "p50 (s)", "p95 (s)", r"\% SLA incumplido"],
          [[tt(x["cliente_id"][:8]), miles(x["n"]), f"{x['p50_s']:.0f}", f"{x['p95_s']:.0f}", f"{x['pct_incumple_sla']:.2f}"]
           for x in q5["mayor_incumplimiento"]], "p5_res_q5.tex")
    escribir("p5_res_lago.tex", [miles(lg["filas"]) + " filas crudas, " + miles(lg["event_id_distintos"]) + " " + tt("event_id")
                                 + " distintos, " + str(lg["archivos"]) + " archivos (" + str(lg["bytes_MiB"]) + " MiB)"])

# ---------------------------------------------------------------- P6
EP = ["POST /tareas/siguiente", "PUT /tareas/{id}/borrador", "POST /tareas/{id}/enviar", "POST /tareas/{id}/revision",
      "POST /tareas/{id}/liberar", "POST /sesiones/recuperar", "GET /proyectos/{id}/exportar", "GET /salud"]


def p6_tablas(carpeta, pref):
    r = cargar(carpeta, "resumen_p6.json")["niveles"]
    vus = ["10", "40", "100"]
    tabla("@{}rrrrrr@{}", ["VUs", r"envíos/min (media; mín--máx)", "req/s", "peticiones", r"errores 5xx/red", r"error \%"],
          [[v, f"{r[v]['envios_por_min']['media']} ({r[v]['envios_por_min']['min']}--{r[v]['envios_por_min']['max']})",
            num(r[v]["rps_medio"], 1), miles(r[v]["n_peticiones"]), str(r[v]["errores"]), num(r[v]["tasa_error_pct"], 3)] for v in vus],
          pref + "_resumen.tex")
    filas = []
    for ep in EP:
        fila = [tt(ep)]
        for v in vus:
            e = r[v]["por_endpoint"][ep]
            fila.append(f"{e['p50']} / {e['p95']} / {e['p99']} ({e['n']})")
        filas.append(fila)
    tabla("@{}p{3.9cm}p{3.7cm}p{3.7cm}p{4.2cm}@{}", ["Endpoint", "10 VUs", "40 VUs", "100 VUs"], filas, pref + "_latencias.tex")
    cs = ["gateway", "registro", "sesion", "valkey", "mongo1", "mongo2", "mongo3", "historial", "herramientas"]
    filas = []
    for c in cs:
        fila = [tt(c)]
        for v in vus:
            d = r[v]["docker_stats"].get(c)
            fila.append(f"{d['cpu_pct_media']} ; {d['cpu_pct_max']} ; {d['mem_mib_max']}" if d else "--")
        filas.append(fila)
    tabla("@{}lrrr@{}", ["Contenedor", "10 VUs", "40 VUs", "100 VUs"], filas, pref + "_stats.tex")


p6_tablas("p6", "p6")
p6_tablas("p6_host", "p6h")
prev = cargar("p6_red_previo", "resumen_p6.json")["niveles"]
escribir("p6_previo.tex", [", ".join(f"{v} VUs: {prev[v]['n_peticiones']} peticiones, {prev[v]['errores']} errores" for v in ("10", "40", "100"))])

# ---------------------------------------------------------------- P8
p8d = cargar("p8_fallas_20261030T2319.json")
p8 = p8d["resultado"]
esc8 = p8["escenarios"]


def cods(v):
    return esc(", ".join(f"{k}: {n}" for k, n in sorted(v["status"].items())))


filas = []
orden = [("S0_linea_base", "S0 línea base (15 s)"), ("S1_secundario_detenido", "S1 secundario detenido"),
         ("S2a_primario_stop_ordenado", "S2a primario detenido (stop)"), ("S2b_primario_kill", "S2b primario muerto (kill)")]
for k, t in orden:
    s = esc8[k]
    rto = s.get("rto")
    filas.append([esc(t), cods(s["ventana"]["siguiente"]), cods(s["ventana"]["enviar"]),
                  (f"{rto['rto_s']} / {rto['ventana_sin_escrituras_s']} / {rto['fallos']}" if rto else "--"), esc(s.get("nuevo_primario", "--"))])
s3 = dict(esc8["S3_sin_mayoria"])
s3.update([p for p in p8d["pasos"] if p["titulo"].startswith("S3 sin mayoría")][0]["datos"])
filas.append(["S3 sin mayoría (mongo2, mongo3)", cods(s3["ventana_escritor"]["siguiente"]), cods(s3["ventana_escritor"]["enviar"]),
              f"{s3['rto_escritor']['rto_s']} / {s3['rto_escritor']['ventana_sin_escrituras_s']} / {s3['rto_escritor']['fallos']}", "mongo1"])
s4 = dict(esc8["S4_valkey_restart"])
s4.update([p for p in p8d["pasos"] if p["titulo"].startswith("S4 reinicio")][0]["datos"])
filas.append(["S4 reinicio de Valkey", cods(s4["ventana_escritor_durante_y_despues"]["siguiente"]), cods(s4["ventana_escritor_durante_y_despues"]["enviar"]),
              f"{s4['rto_escritor']['rto_s']} / {s4['rto_escritor']['ventana_sin_escrituras_s']} / {s4['rto_escritor']['fallos']}", "--"])
tabla("@{}p{3.9cm}p{2.5cm}p{2.5cm}p{3.1cm}l@{}", ["Escenario", "siguiente (códigos)", "enviar (códigos)", "RTO / ventana / fallos (s, s, n)", "Nuevo primario"],
      filas, "p8_escenarios.tex")
sd = s3["sondas_durante"]
filas = []
for k, v in sd.items():
    ruta, _, nota = k.partition(" (")
    filas.append([tt(ruta) + (" (" + esc(nota) if nota else ""), str(v["http"]), str(v["ms"])])
tabla("@{}p{9cm}rr@{}", ["Ruta durante la falta de mayoría", "HTTP", "ms"], filas, "p8_sondas.tex")
rpo = p8["rpo"]
tabla("@{}p{11cm}r@{}", ["Verificación (consistencia tras todas las fallas)", "Valor"],
      [[esc(k), str(v) if not isinstance(v, (list, dict)) else ("vacío" if not v else esc(json.dumps(v)))] for k, v in rpo.items()], "p8_rpo.tex")
print("tablas generadas en", OUT, sorted(os.listdir(OUT)))
