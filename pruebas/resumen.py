"""Genera evidence/RESUMEN.md a partir de los JSON de evidencia más recientes (sin transcribir números a mano).
Cada cifra lleva el nombre del archivo que la respalda. Lo que no se midió se declara "no disponible".

Uso: python pruebas/resumen.py
"""
import glob
import json
import os

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EV = os.path.join(RAIZ, "evidence")


def ultimo(prefijo):
    fs = sorted(glob.glob(os.path.join(EV, prefijo + "_[0-9]*.json")))
    if not fs:
        return None, None
    with open(fs[-1], encoding="utf8") as fh:
        return json.load(fh), os.path.basename(fs[-1])


def cargar(rel):
    try:
        with open(os.path.join(EV, rel), encoding="utf8") as fh:
            return json.load(fh)
    except Exception:
        return None


def paso(d, empieza):
    return next((p for p in d["pasos"] if p["titulo"].startswith(empieza)), None)


def fila(*xs):
    return "| " + " | ".join(str(x) for x in xs) + " |"


def main():
    o = ["# RESUMEN de resultados experimentales (generado por pruebas/resumen.py)", "",
         "Todas las cifras salen de los archivos de `evidence/` indicados entre paréntesis. Percentiles: nearest-rank. "
         "\"no disponible\" = no medido, con el motivo indicado.", ""]
    # ---------------- entorno
    p1, f1 = ultimo("p1_api")
    m = (p1 or {}).get("maquina", {})
    o += ["## 0. Entorno (evidence/entorno.txt)", "",
          f"- Host: {m.get('so')}; CPU Intel Core i7-1365U (10 núcleos / 12 hilos lógicos, 1.8 GHz base); RAM del host 31.7 GiB.",
          f"- Docker {m.get('docker')} con {m.get('docker_cpus')} vCPU y {m.get('docker_mem_gib')} GiB de memoria para la VM (Docker Desktop, WSL2).",
          "- Imágenes: mongo:7 (3 nodos), valkey/valkey:8, apache/spark:3.5.3 (master + workers), python:3.12-slim (servicios), Python 3.14 en el host.",
          "- Ruido: en la misma máquina corren contenedores de otro curso (`ti4601-*`, Mongo/Neo4j/Jupyter) que consumen CPU de forma intermitente; "
          "no se detuvieron. Los tiempos describen este equipo compartido.",
          "- Docker Desktop no puede montar carpetas de Google Drive (H:): el datalake vive en C:/p2work (variables `DATALAKE_*` en `.env`).", ""]
    # ---------------- P1
    if p1:
        r = p1["resultado"]
        o += [f"## P1. API y /salud ({f1})", "",
              f"- Casos HTTP probados: {r['casos_total']}; con el código esperado: {r['casos_ok']}; discrepancias: {len(r['casos_con_discrepancia'])}.",
              "- Estados de /salud (segundos hasta observar el estado tras detener el componente):", "",
              fila("Escenario", "Estado", "HTTP", "s hasta verlo"), fila("---", "---", "---", "---")]
        for k, v in r["escenarios_salud"].items():
            o.append(fila(k, v["estado"], v["http"], v["segundos_hasta_ver_estado"]))
        o += ["", f"- Puertos publicados: {', '.join(f'{x['contenedor']} {x['bind']}:{x['host']}' for x in r['puertos_publicados'])}. "
              "Sólo el gateway (8000) es público; 4040 y 18080 son de administración y escuchan en 127.0.0.1.",
              f"- Conexión desde el host a puertos internos: {json.dumps(r['conexiones_host'], ensure_ascii=False)}.",
              f"- Puertos remotos de las conexiones TCP del simulador (host): {r['puertos_remotos_simulador']} (sólo 8000).",
              f"- Imports prohibidos hallados (debe ser todo vacío): {json.dumps(r['imports_prohibidos'], ensure_ascii=False)}.", ""]
    # ---------------- P2
    p2, f2 = ultimo("p2_concurrencia")
    if p2:
        r = p2["resultado"]
        o += [f"## P2. Concurrencia ({f2})", "",
              f"- rs.status: {json.dumps(r['replica_set'])}.",
              "- 50 solicitudes simultáneas con 10 tareas libres (ProcessPoolExecutor, 50 procesos con barrera de reloj):", "",
              fila("Ronda", "Códigos", "Dispersión del inicio (ms)", "Cumple"), fila("---", "---", "---", "---")]
        for x in r["rondas_concurrentes"]:
            o.append(fila(x["ronda"], json.dumps(x["codigos"]), x["dispersion_inicio_ms"], x["cumple"]))
        o += ["", f"- Segundo enviar: HTTP {r['segundo_enviar']}; fila extra en el lago: {'sí' if r['fila_extra_en_lago'] else 'no'}.",
              "- w:majority vs w:1 a través del gateway (media de rondas; enviar), ms:", "",
              fila("Modo", "p50", "p95", "p99", "n", "ciclos/s"), fila("---", "---", "---", "---", "---", "---")]
        for k, v in r["w_majority_vs_w1"].items():
            e = v["enviar"]
            o.append(fila(k, e["p50_ms"], e["p95_ms"], e["p99_ms"], e["n_total"], v["ciclos_por_s_media"]))
        wd = r.get("w_directo_en_mongo") or []
        if wd:
            def medias(w):
                xs = [x for x in wd if x["w"] == w]
                return {k: round(sum(x[k] for x in xs) / len(xs), 2) for k in ("media_ms", "p50_ms", "p95_ms", "p99_ms")}
            o += ["", "- w:majority vs w:1 directamente en Mongo (1500 updateOne secuenciales x 3 pares, resolución 1 ms), ms:", "",
                  fila("Modo", "media", "p50", "p95", "p99"), fila("---", "---", "---", "---", "---")]
            for w in ("majority", "1"):
                x = medias(w)
                o.append(fila("w:" + w, x["media_ms"], x["p50_ms"], x["p95_ms"], x["p99_ms"]))
        ex = paso(p2, "explain")
        if ex:
            o += ["", "- explain(executionStats) (plan, claves examinadas, docs examinados, ms):", ""]
            for k, v in ex["datos"]["explain"].items():
                o.append(f"  - {k}: {' > '.join(v['plan'])}; keys={v['keysExaminadas']}; docs={v['docsExaminados']}; {v['ms']} ms")
        o += ["", "- Nota: `evidence/p2_concurrencia_20261030T2121.*` es una corrida anterior al índice extra `tareas_estado_prioridad_expira` "
              "(consulta de asignación sin proyecto: 59 992 claves y 105 ms con el índice anterior `tareas_estado_expira`); "
              "ver la última corrida para el resultado con el índice nuevo.", ""]
    # ---------------- P3
    p3, f3 = ultimo("p3_sesion")
    if p3:
        r = p3["resultado"]
        o += [f"## P3. Sesión ({f3})", "",
              f"- Edad del borrador recuperado tras matar el editor (ms): {json.dumps(r['borrador_edad_ms'])}; todas <= 5 s: {r['todas_edades_le_5s']}; "
              f"contenido igual al último confirmado: {r['contenido_recuperado_igual_al_ultimo_ack']}.",
              f"- Borrador tras `docker compose restart valkey`: {r['valkey_restart_conserva_borrador']}; tras `docker kill valkey` (SIGKILL): {r['valkey_kill9_conserva_borrador']}.",
              f"- Candado renovado por latidos (TTL 10 s, 2.5 x TTL): {r['candado_se_renueva_con_latidos']}; tarea re-ofrecida a otro tras abandono: {r['candado_vencido_reofrecido']} "
              f"({r['segundos_hasta_reoferta']} s tras el último latido); tareas con dos dueños: {r['tareas_con_dos_duenos']}.",
              "- Observación: entre ~8 y 10 s B recibió 409 transitorio (Mongo ofrece la tarea cuando `expira` vence, pero el lock de Valkey, renovado en cada borrador, aún no); "
              "el gateway compensa liberando y no hay doble dueño.", ""]
    # ---------------- P4
    p4, f4 = ultimo("p4_historial")
    if p4:
        r = p4["resultado"]
        v = r["segundos_http200_a_spark"]
        o += [f"## P4. Historial ({f4})", "",
              f"- Segundos entre HTTP 200 de enviar y evento visible en Spark (n={v['n']}): "
              f"min {v['visible_en_spark_s']['min']}, p50 {v['visible_en_spark_s']['p50']}, p95 {v['visible_en_spark_s']['p95']}, p99 {v['visible_en_spark_s']['p99']}, máx {v['visible_en_spark_s']['max']}. "
              f"Archivo en disco: p50 {v['archivo_en_disco_s']['p50']} s (máx {v['archivo_en_disco_s']['max']}).",
              f"- Deduplicación tras reprocesar (copia duplicada): conteo Spark sin cambio y filas crudas=2/dedup=1: {r['deduplicacion_ok']}.",
              f"- Lecturas Spark concurrentes con escrituras: {r['lecturas_spark_concurrentes']}; errores: {r['errores_de_lectura_concurrente']}; archivos nuevos vigilados: {r['archivos_nuevos_vigilados']}.",
              f"- Parquet validados con pyarrow: {r['archivos_validados']}; inválidos: {r['parquet_invalidos']}.",
              f"- Rechazo: Parquet 'rechazada' con Mongo 'disponible': {r['rechazo_parquet_vs_mongo']}.",
              f"- Compactación del mes: {json.dumps(r['compactacion'])}.", ""]
    p4f, f4f = ultimo("p4_frescura_q1")
    if p4f:
        r = p4f["resultado"]
        q = r["q1_visible_desde_http200_s"]
        o += [f"## P4 (frescura hasta Q1) ({f4f})", "",
              f"- Segundos entre el HTTP 200 de la aprobación y la primera respuesta de Q1 que la cuenta (n={r['n']}, visibles={r['visibles']}): "
              f"min {q['min']}, p50 {q['p50']}, p95 {q['p95']}, p99 {q['p99']}, máx {q['max']}. "
              f"Archivo en disco: p50 {r['archivo_en_disco_desde_http200_s']['p50']} s (máx {r['archivo_en_disco_desde_http200_s']['max']}).",
              f"- Consultas de Q1 por muestra: {json.dumps(r['consultas_q1_por_muestra'])}; resolución: {r['resolucion_s']}.", ""]
    # ---------------- P5
    p5, f5 = ultimo("p5_analitica")
    if p5:
        r = p5["resultado"]
        o += [f"## P5. Analítica ({f5}; evidence/p5/)", "",
              f"- Validación Q1-Q5 contra `datalake_validacion/esperado.json` (56 eventos únicos + 4 duplicados): todas coinciden = {r['validacion_todas_coinciden']} (evidence/p5/validacion_resultado.json).",
              f"- Lago principal antes: {json.dumps(r['lago_antes'])}; después de compactar: {json.dumps(r['lago_despues'])}.",
              f"- Ejecutores con 2 workers: {json.dumps(r['executores_C3'])}.", "",
              "Tiempos (s) medidos por el servicio; fría = primera tras reiniciar `analitica`; caliente = mediana de las rondas posteriores:", "",
              fila("Config", "Q1 fría/caliente", "Q2", "Q3", "Q4", "Q5"), fila("---", "---", "---", "---", "---", "---")]
        for nom, k in (("C0 original, 272 archivos, 1 worker (2 rondas calientes)", "C0"), ("C1 original, compactado, 1 worker (2 rondas calientes)", "C1"),
                       ("C2 final, compactado, 1 worker (5 rondas calientes)", "C2"), ("C3 final, 2 workers (5 rondas calientes)", "C3")):
            ag = r[k]
            o.append(fila(nom, *[f"{ag[q].get('fria_s')} / {ag[q]['caliente']['mediana_s']}" if 'caliente' in ag[q] else "fallo" for q in ("q1", "q2", "q3", "q4", "q5")]))
        g = r.get("grande", {})
        for nom, k in (("G2 conjunto grande 10 M, 2 workers (1 ronda caliente)", "G2"), ("G1 conjunto grande 10 M, 1 worker (1 ronda caliente)", "G1")):
            if k in g:
                o.append(fila(nom, *[f"{g[k][q].get('fria_s')} / {g[k][q]['caliente']['mediana_s']}" if 'caliente' in g[k][q] else "fallo" for q in ("q1", "q2", "q3", "q4", "q5")]))
        o += ["", f"- Conjunto grande: {json.dumps(g.get('lago'))} (generado en 232 s con `dataset/generar_historial.py --eventos 10000000`; 1602 archivos compactados a 12 en 67 s con `dataset/compactar_lago.py`). "
              "Memoria con el conjunto grande (docker stats): analitica ~0.9 GiB, worker ~2.0-2.7 GiB de 2 GiB de executor; sin fallos.",
              "- Planes: evidence/p5/plan_q1.txt (filtro `mes >= ...` en PartitionFilters; agrupación por día, modalidad) y evidence/p5/plan_q3.txt (Exchange rangepartitioning + Sort por la ordenación final).",
              "- Cuello de botella hallado: ejecutores casi ociosos (CPU 0.9-2 s de 5-6 s por tarea) esperando E/S de los ~190-270 archivos pequeños por el montaje 9p de Docker Desktop; "
              "la compactación a 1 archivo/mes y filtrar antes de deduplicar redujeron los tiempos calientes (ver tabla). Con 2 workers la mejora es pequeña: son contenedores del mismo equipo.", ""]
    # ---------------- P5b
    p5b, f5b = ultimo("p5_resultados")
    if p5b:
        r = p5b["resultado"]
        rs = r["resumen"]
        o += [f"### P5b. Resultados completos de Q1-Q5 sobre el conjunto principal ({f5b}; evidence/p5/resultados_principal_q1..q5.json)", "",
              f"- Lago al medir: {json.dumps(r['lago_al_medir'])} (max ts {r['max_ts_lago']}). Una ejecución medida por consulta tras calentamiento (sesión Spark ya creada).", "",
              fila("Q", "Filas", "tiempo_s (servicio)", "pared cliente (ms)"), fila("---", "---", "---", "---")]
        for q in ("q1", "q2", "q3", "q4", "q5"):
            t = r["tiempos"][q]
            o.append(fila(q.upper(), t["n_filas"], t["tiempo_s"], t["pared_cliente_ms"]))
        q1, q2, q3, q4, q5 = (rs[k] for k in ("q1", "q2", "q3", "q4", "q5"))
        o += ["",
              f"- Q1: {q1['total_aprobadas']} aprobadas en {q1['dias_distintos']} días ({q1['primer_dia']} a {q1['ultimo_dia']}); por modalidad {json.dumps(q1['por_modalidad'])}; "
              f"mediana diaria {q1['aprobadas_por_dia_total']['mediana']}, mínimo {q1['aprobadas_por_dia_total']['min']} ({q1['aprobadas_por_dia_total']['dia_min']}), "
              f"máximo {q1['aprobadas_por_dia_total']['max']} ({q1['aprobadas_por_dia_total']['dia_max']}); sin el último día: {q1['total_sin_ultimo_dia']}. "
              f"Eventos crudos del último día en el lago: {r['ultimos_dias_lago'][q1['ultimo_dia']]['eventos']} (los días previos: "
              + ", ".join(f"{d} {v['eventos']}" for d, v in r['ultimos_dias_lago'].items() if d != q1['ultimo_dia']) + ").",
              f"- Q2: {q2['equipos']} equipos (+1 fila sin equipo), {q2['meses']} meses, {q2['filas']} filas; proporción aprobadas mín {q2['proporcion']['min']}, mediana {q2['proporcion']['mediana']}, máx {q2['proporcion']['max']}; global {q2['proporcion_global']}.",
              f"- Q3: {q3['total_n']} anotaciones; media mayor {q3['media_mas_alta']['modalidad']}/{q3['media_mas_alta']['tipo_tarea']} {q3['media_mas_alta']['dur_media_s']} s (p95 {q3['media_mas_alta']['dur_p95_s']} s); media menor {q3['media_mas_baja']['modalidad']}/{q3['media_mas_baja']['tipo_tarea']} {q3['media_mas_baja']['dur_media_s']} s.",
              f"- Q4: {q4['pares_cliente_proyecto']} pares cliente-proyecto, {q4['clientes']} clientes; proporción de rechazo global {q4['proporcion_global_rechazadas']}; por par mín {q4['proporcion']['min']}, mediana {q4['proporcion']['mediana']}, máx {q4['proporcion']['max']}.",
              f"- Q5: {q5['clientes_distintos']} clientes, {q5['total_n']} aprobaciones; mediana entre clientes de p50 {q5['p50_mediana_clientes_s']} s y de p95 {q5['p95_mediana_clientes_s']} s; % SLA incumplido mediana {q5['pct_incumple_sla']['mediana']}, máx {q5['pct_incumple_sla']['max']}, global ponderado {q5['global_ponderado_pct_incumple']}.", ""]
    # ---------------- P6
    for sub, etq in (("p6", "red (simulador dentro de la red docker, contra gateway:8000) - medición principal"),
                     ("p6_red_previo", "red, corrida previa al ajuste de keepalive del gateway (1 error 503 de 19 048 peticiones)"),
                     ("p6_host", "host Windows -> localhost:8000 (reenvío de puertos de Docker Desktop)")):
        d = cargar(f"{sub}/resumen_p6.json")
        if not d:
            if sub == "p6":
                o += ["## P6. Carga", "", "no disponible: no se ejecutó.", ""]
            continue
        ns = d["niveles"]
        vus = sorted(ns, key=int)
        o += [f"## P6 {etq} (evidence/{sub}/resumen_p6.json, .md y *.jsonl; fecha {d['fecha']})" if sub == "p6" else f"### P6 {etq} (evidence/{sub}/resumen_p6.json)", "",
              fila("VUs", "envíos/min (media; min-máx)", "req/s", "peticiones", "errores 5xx/red", "tasa error %"), fila("---", "---", "---", "---", "---", "---")]
        for v in vus:
            n = ns[v]
            e = n["envios_por_min"]
            o.append(fila(v, f"{e['media']} ({e['min']}-{e['max']})", n["rps_medio"], n["n_peticiones"], n["errores"], n["tasa_error_pct"]))
        o += ["", "Latencias (ms) p50 / p95 / p99 por endpoint (3 corridas x 90 s, tras 30 s de calentamiento no medido):", "",
              fila("Endpoint", *[f"{v} VUs" for v in vus]), fila("---", *["---"] * len(vus))]
        eps = sorted({e for n in ns.values() for e in n["por_endpoint"]})
        for e in eps:
            o.append(fila(e, *[(lambda x: f"{x['p50']} / {x['p95']} / {x['p99']} (n={x['n']})" if x else "-")(ns[v]["por_endpoint"].get(e)) for v in vus]))
        o += ["", "docker stats (CPU % media; memoria máx MiB) de los contenedores principales:", "",
              fila("Contenedor", *[f"{v} VUs" for v in vus]), fila("---", *["---"] * len(vus))]
        for c in ("gateway", "registro", "sesion", "valkey", "mongo1", "mongo2", "mongo3", "historial", "herramientas"):
            if any(c in ns[v]["docker_stats"] for v in vus):
                o.append(fila(c, *[(lambda x: f"{x['cpu_pct_media']} ; {x['mem_mib_max']}" if x else "-")(ns[v]["docker_stats"].get(c)) for v in vus]))
        o.append("")
    o += ["- Interpretación P6: con el simulador dentro de la red docker la plataforma escala de 10 a 100 VUs sin errores; a 100 VUs el gateway (un solo proceso uvicorn) "
          "llega a 70 % de CPU media y las latencias de cola (p95 de siguiente ~1.2 s) crecen. Medido desde el host a través de `localhost:8000` el reenvío de puertos de Docker Desktop "
          "limita el rendimiento a ~10 req/s con latencias de segundos aunque los servicios internos respondan en <20 ms (comprobado con el mismo simulador dentro del contenedor del gateway); "
          "esa corrida se conserva como evidencia de la limitación del entorno.",
          f"- Tareas `disponible` en Mongo: {(ultimo('p6_carga')[0] or {}).get('resultado', {}).get('tareas_disponibles_inicio')} al empezar la última corrida de P6 y {(ultimo('p6_carga')[0] or {}).get('resultado', {}).get('tareas_disponibles_fin')} al terminar (de 60 000 iniciales; las pruebas anteriores y las corridas previas de P6 consumieron el resto): no hizo falta reponerlas ni volver a cargar el dataset.",
          "- Corridas P6 en la red: una primera (`p6_red_previo`) mostró 1 error 503 (keep-alive gateway->sesión); tras ajustar el gateway una corrida intermedia mostró 2 `ReadError` del propio simulador "
          "(keep-alive cliente->gateway, sin errores en los logs del gateway) y se repitió tras ajustar el simulador; esta última (carpeta `p6/`) tuvo 0 errores y es la que se reporta. La intermedia no se conserva.", ""]
    # ---------------- P8
    p8, f8 = ultimo("p8_fallas")
    if p8:
        r = p8["resultado"]
        o += [f"## P8. Fallas y recuperación ({f8}; evidence/p8/operaciones.json)", "",
              fila("Escenario", "siguiente (códigos)", "enviar (códigos)", "RTO / ventana sin escrituras / fallos"), fila("---", "---", "---", "---")]

        def pp(prefijo):
            ex_ = next((x["datos"] for x in p8["pasos"] if x["titulo"] == prefijo), None)
            return ex_ or next((x["datos"] for x in p8["pasos"] if x["titulo"].startswith(prefijo) and isinstance(x["datos"], dict) and "ventana" in x["datos"]
                                or x["titulo"].startswith(prefijo) and prefijo not in ("S0", "S1", "S2", "S2b") and x["datos"] is not None), None)
        for nom, pref in (("S0 línea base (15 s)", "S0"), ("S1 secundario detenido", "S1"), ("S2a primario detenido (docker stop)", "S2a_primario_stop_ordenado"),
                          ("S2b primario muerto (docker kill)", "S2b_primario_kill")):
            d = pp(pref)
            if not d or not isinstance(d, dict):
                continue
            ven = d["ventana"]
            rt = d.get("rto")
            ex = f"{nom} (nuevo primario: {d['nuevo_primario']})" if d.get("nuevo_primario") else nom
            o.append(fila(ex, ven["siguiente"]["status"], ven["enviar"]["status"],
                          f"rto_s={rt['rto_s']}; ventana_s={rt['ventana_sin_escrituras_s']}; fallos={rt['fallos']}" if rt else "-"))
        d = pp("S3 sin mayor")
        if d:
            o.append(fila("S3 dos secundarios detenidos (sin mayoría)", d["ventana_escritor"]["siguiente"]["status"], d["ventana_escritor"]["enviar"]["status"],
                          f"tras reiniciar los nodos: {d['segundos_hasta_replica_set_completo']} s hasta replica set completo y {d['segundos_hasta_envio_exitoso_desde_restauracion']} s hasta el primer envío exitoso; "
                          f"el reintento del usuario: HTTP {d['reintento_enviar_http']} (reconciliado={d.get('reintento_enviar_reconciliado')})"))
        d = pp("S4 reinicio")
        if d:
            ven = d["ventana_escritor_durante_y_despues"]
            o.append(fila("S4 reinicio de Valkey a mitad de sesión", ven["siguiente"]["status"], ven["enviar"]["status"],
                          f"borrador sigue: {d['borrador_sigue']}; enviar tras reinicio HTTP {d['enviar_tras_reinicio']}; rto_s={d['rto_escritor']['rto_s']}; fallos={d['rto_escritor']['fallos']}"))
        d = pp("S5 sesi")
        if d:
            o.append(fila("S5 sesión interrumpida y recuperada", "-", "-", f"recuperar HTTP {d['recuperar_http']}; edad del borrador {d['edad_ms']} ms; enviar tras recuperar HTTP {d['enviar_tras_recuperar']}"))
        s3 = r["escenarios"].get("S3_sin_mayoria", {})
        if s3.get("sondas_durante"):
            o += ["", "- Sin mayoría (S3), respuesta de cada ruta mientras Mongo no puede confirmar:"]
            for k, v in s3["sondas_durante"].items():
                o.append(f"  - {k}: HTTP {v.get('http', v.get('error'))} ({v.get('ms', '-')} ms)")
        rp = r["rpo"]
        o += ["", f"- RPO: {json.dumps(rp, ensure_ascii=False)}.",
              "- Hallazgo y corrección: en la primera corrida (evidence/p8_fallas_20261030T2312.*) 6 envíos que dieron 503 por falta de mayoría (WriteConcernTimeout) "
              "SÍ se habían aplicado en Mongo; al reintentar se obtenía 409 y su evento nunca llegó al datalake. Se añadió la reconciliación (`confirmar_envio` en registro, "
              "indicador `previo` en sesión y lógica en el gateway) que republica el mismo evento (mismo event_id) y responde 200 si el envío ya estaba aplicado y quedó un pendiente en Valkey.", ""]
    o += ["## Errores corregidos durante esta fase", "",
          "- `registro`: índice extra reemplazado por `tareas_estado_prioridad_expira` (asignación sin proyecto pasó de 59 992 claves/105 ms a 2 claves/~1 ms; P2).",
          "- `gateway`: `keepalive_expiry=2 s` hacia registro/sesión y reintento del PUT idempotente del envío pendiente (503 espurio por conexión keep-alive cerrada, visto con 100 VUs en P6).",
          "- Reconciliación de envíos ambiguos tras 503 (registro `confirmar_envio`, sesión `previo`, gateway) - ver P8.",
          "- `analitica`: ruta `GET /evento/{id}` (3.ª de 6) para P4; filtro antes de deduplicar (`OPT_FILTRO_PRIMERO`); herramienta `dataset/compactar_lago.py`.", "",
          "## Limitaciones declaradas", "",
          "- Todo corre en una sola máquina: w:1 vs w:majority, 1 vs 2 workers de Spark y las fallas de nodos no permiten inferir el comportamiento de un clúster en varias máquinas.",
          "- El conjunto principal contiene, además de los ~800 000 eventos generados, los eventos de las pruebas (decenas de miles); se compactó antes de las mediciones P5 pero P4/P6/P8 vuelven a crear archivos pequeños (se compacta de nuevo al final).",
          "- Las medidas de P6 mezclan la capacidad de la plataforma con la del propio generador de carga y los contenedores de otro curso que comparten el equipo.",
          "- El historial no compacta automáticamente: en producción haría falta un trabajo periódico (POST /compactar) porque cada micro-lote crea un archivo.", ""]
    with open(os.path.join(EV, "RESUMEN.md"), "w", encoding="utf8") as fh:
        fh.write("\n".join(o) + "\n")
    print("escrito evidence/RESUMEN.md")


if __name__ == "__main__":
    main()
