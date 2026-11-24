"""Genera evidence/README.md: índice de los archivos de evidencia -> prueba / afirmación que respaldan.
Uso: python pruebas/indice.py
"""
import glob
import json
import os

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EV = os.path.join(RAIZ, "evidence")

NOTAS = {
    "p1_api": "Rutas del gateway con códigos 200/201/204/404/409/422/503, /salud normal/degradado/caído por capa, puertos publicados, conexiones del simulador y aislamiento modular.",
    "p2_concurrencia": "rs.status, 50 solicitudes simultáneas con 10 tareas (10x200 + 40x204), ninguna tarea con dos dueños, segundo enviar 409 sin fila extra, w:majority vs w:1, explain de índices.",
    "p3_sesion": "Borrador recuperable (<= 5 s) tras matar al editor, Valkey con AOF (restart y SIGKILL), vencimiento del candado con TTL de 10 s.",
    "p4_frescura_q1": "Frescura medida: segundos desde el HTTP 200 de la aprobación hasta que Q1 (por el gateway) la cuenta; archivo en disco; tiempo de cada consulta de Q1.",
    "p4_historial": "Latencia HTTP 200 -> visible en Spark, reproceso de micro-lote y deduplicación, lecturas concurrentes sin archivos a medias, validez de Parquet, rechazada vs disponible, compactación.",
    "p5_analitica": "Validación Q1-Q5, frío vs caliente, 1 vs 2 workers, planes de Q1/Q3, mejoras (compactación, filtro antes de dedup), conjunto grande de 10 M.",
    "p5_resultados": "Cuerpos completos de Q1-Q5 sobre el conjunto principal (evidence/p5/resultados_principal_q*.json), tiempos, estado del lago y estadísticos para la interpretación.",
    "p6_carga": "Carga con 10/40/100 VUs dentro de la red docker (medición principal): p50/p95/p99 por endpoint, envíos/min, errores, docker stats.",
    "p6_carga_red_previo": "Misma prueba P6 (red) antes del ajuste de keepalive del gateway (1 error 503 en 19 048 peticiones).",
    "p6_carga_host": "Misma prueba P6 con el simulador en el host Windows (localhost:8000): limitada por el reenvío de puertos de Docker Desktop.",
    "p8_fallas": "Fallas bajo escritura: secundario, primario (stop y kill), sin mayoría, Valkey; RTO, 503 durante la elección, RPO = 0.",
}
SUBDIRS = {
    "p5": "P5: resultados_principal_q1..q5.json (cuerpos completos de Q1-Q5, conjunto principal), plan_q1.txt / plan_q3.txt (explain formatted), validacion_*.json/txt (Q1-Q5 vs esperado.json), resumen_p5.json/md (tablas de tiempos).",
    "p6": "P6 (red): sim_<N>vu_run<k>.jsonl (un registro por petición: ts, endpoint, status, ms), resumen_*.json, docker_stats.csv, resumen_p6.md/json.",
    "p6_red_previo": "P6 (red) corrida previa al ajuste de keepalive: mismos tipos de archivo que p6/.",
    "p6_host": "P6 (host): mismos tipos de archivo que p6/, medidos vía localhost:8000.",
    "p8": "P8: operaciones.json (todas las operaciones del escritor con estado y latencia + acciones de recuperación), operaciones_<fecha>.json de la corrida anterior.",
    "graficas": "PNG generados por pruebas/graficas.py (latencias P6, envíos/min, CPU, tiempos P5, w:majority vs w:1, P4, línea de tiempo de P8).",
}


def main():
    o = ["# Índice de evidencia", "",
         "Cada prueba genera un par `<prueba>_<AAAAMMDDThhmm>.json` / `.md` con fecha y hora, objetivo, configuración, comando ejecutado, "
         "resultado observado, interpretación y los archivos que la respaldan. Latencias: p50/p95/p99 con n de muestras; máquina: `entorno.txt`. "
         "`RESUMEN.md` reúne las cifras clave (generado por `pruebas/resumen.py`).", "",
         "| Archivo | Prueba | Qué respalda | Fecha |", "|---|---|---|---|",
         "| `entorno.txt` | todas | CPU, RAM, versiones de Docker/imagenes, `docker info`, docker stats en reposo | - |",
         "| `p4_sim_resumen.json` | P4 | Resumen del simulador (25 VUs, 55 s) que escribía mientras Spark leía | - |",
         "| `compactacion_posterior_a_P6_P8.json` | P4/P6/P8 | Salida de la compactación del mes tras P6/P8 (576 archivos -> 1; 77 duplicados por event_id eliminados) | - |",
         "| `RESUMEN.md` | todas | Cifras clave con el archivo que las respalda, errores corregidos y limitaciones | - |"]
    for f in sorted(glob.glob(os.path.join(EV, "p*_[0-9]*.json"))):
        d = json.load(open(f, encoding="utf8"))
        base = os.path.basename(f)[:-5]
        pref = base.rsplit("_", 1)[0]
        o.append(f"| `{base}.json`, `{base}.md` | {d['titulo'].split('.')[0]} | {NOTAS.get(pref, d['objetivo'][:120])} | {d['fecha_hora']} |")
    o += ["", "## Carpetas", ""]
    for k, v in SUBDIRS.items():
        if os.path.isdir(os.path.join(EV, k)):
            n = len(os.listdir(os.path.join(EV, k)))
            o.append(f"- `{k}/` ({n} archivos): {v}")
    o += ["", "## Corridas históricas conservadas", "",
          "- `p2_concurrencia_20261030T2121.*`: P2 antes del índice extra `tareas_estado_prioridad_expira` (explain con el índice anterior; las 50 solicitudes salieron con una dispersión de inicio de 3-6 s porque el pool de procesos se creaba dentro de la ronda).",
          "- `p8_fallas_20261030T2312.*`: P8 antes de la reconciliación de envíos ambiguos (6 tareas anotadas en Mongo sin evento en el lago tras un 503 por falta de mayoría).",
          "- `p6_carga_red_previo_*` y `p6_carga_host_*`: ver arriba.", "",
          "## Cómo reproducir", "", "`pruebas/run_all.ps1` (orden: entorno, P1, P2, P3, P5, P5b resultados, P4, P6, P8, gráficas); luego `python pruebas/resumen.py` y `python pruebas/indice.py`.", ""]
    with open(os.path.join(EV, "README.md"), "w", encoding="utf8") as fh:
        fh.write("\n".join(o) + "\n")
    print("escrito evidence/README.md")


if __name__ == "__main__":
    main()
