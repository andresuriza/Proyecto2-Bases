# Índice de evidencia

Cada prueba genera un par `<prueba>_<AAAAMMDDThhmm>.json` / `.md` con fecha y hora, objetivo, configuración, comando ejecutado, resultado observado, interpretación y los archivos que la respaldan. Latencias: p50/p95/p99 con n de muestras; máquina: `entorno.txt`. `RESUMEN.md` reúne las cifras clave (generado por `pruebas/resumen.py`).

| Archivo | Prueba | Qué respalda | Fecha |
|---|---|---|---|
| `entorno.txt` | todas | CPU, RAM, versiones de Docker/imagenes, `docker info`, docker stats en reposo | - |
| `p4_sim_resumen.json` | P4 | Resumen del simulador (25 VUs, 55 s) que escribía mientras Spark leía | - |
| `compactacion_posterior_a_P6_P8.json` | P4/P6/P8 | Salida de la compactación del mes tras P6/P8 (576 archivos -> 1; 77 duplicados por event_id eliminados) | - |
| `RESUMEN.md` | todas | Cifras clave con el archivo que las respalda, errores corregidos y limitaciones | - |
| `p1_api_20261030T2359.json`, `p1_api_20261030T2359.md` | P1 | Rutas del gateway con códigos 200/201/204/404/409/422/503, /salud normal/degradado/caído por capa, puertos publicados, conexiones del simulador y aislamiento modular. | 2026-10-30T23:59:03 |
| `p2_concurrencia_20261030T2121.json`, `p2_concurrencia_20261030T2121.md` | P2 | rs.status, 50 solicitudes simultáneas con 10 tareas (10x200 + 40x204), ninguna tarea con dos dueños, segundo enviar 409 sin fila extra, w:majority vs w:1, explain de índices. | 2026-10-30T21:21:52 |
| `p2_concurrencia_20261030T2141.json`, `p2_concurrencia_20261030T2141.md` | P2 | rs.status, 50 solicitudes simultáneas con 10 tareas (10x200 + 40x204), ninguna tarea con dos dueños, segundo enviar 409 sin fila extra, w:majority vs w:1, explain de índices. | 2026-10-30T21:41:11 |
| `p2_concurrencia_20261030T2333.json`, `p2_concurrencia_20261030T2333.md` | P2 | rs.status, 50 solicitudes simultáneas con 10 tareas (10x200 + 40x204), ninguna tarea con dos dueños, segundo enviar 409 sin fila extra, w:majority vs w:1, explain de índices. | 2026-10-30T23:33:02 |
| `p3_sesion_20261030T2331.json`, `p3_sesion_20261030T2331.md` | P3 | Borrador recuperable (<= 5 s) tras matar al editor, Valkey con AOF (restart y SIGKILL), vencimiento del candado con TTL de 10 s. | 2026-10-30T23:31:10 |
| `p4_frescura_q1_20261031T0124.json`, `p4_frescura_q1_20261031T0124.md` | P4 (frescura) | Frescura medida: segundos desde el HTTP 200 de la aprobación hasta que Q1 (por el gateway) la cuenta; archivo en disco; tiempo de cada consulta de Q1. | 2026-10-31T01:24:39 |
| `p4_historial_20261030T2216.json`, `p4_historial_20261030T2216.md` | P4 | Latencia HTTP 200 -> visible en Spark, reproceso de micro-lote y deduplicación, lecturas concurrentes sin archivos a medias, validez de Parquet, rechazada vs disponible, compactación. | 2026-10-30T22:16:55 |
| `p5_analitica_20261030T2150.json`, `p5_analitica_20261030T2150.md` | P5 | Validación Q1-Q5, frío vs caliente, 1 vs 2 workers, planes de Q1/Q3, mejoras (compactación, filtro antes de dedup), conjunto grande de 10 M. | 2026-10-30T21:50:29 |
| `p5_resultados_20261031T0116.json`, `p5_resultados_20261031T0116.md` | P5b | Cuerpos completos de Q1-Q5 sobre el conjunto principal (evidence/p5/resultados_principal_q*.json), tiempos, estado del lago y estadísticos para la interpretación. | 2026-10-31T01:16:10 |
| `p6_carga_20261031T0002.json`, `p6_carga_20261031T0002.md` | P6 | Carga con 10/40/100 VUs dentro de la red docker (medición principal): p50/p95/p99 por endpoint, envíos/min, errores, docker stats. | 2026-10-31T00:02:57 |
| `p6_carga_host_20261030T2221.json`, `p6_carga_host_20261030T2221.md` | P6 | Misma prueba P6 con el simulador en el host Windows (localhost:8000): limitada por el reenvío de puertos de Docker Desktop. | 2026-10-30T22:21:09 |
| `p6_carga_red_previo_20261030T2249.json`, `p6_carga_red_previo_20261030T2249.md` | P6 | Misma prueba P6 (red) antes del ajuste de keepalive del gateway (1 error 503 en 19 048 peticiones). | 2026-10-30T22:49:04 |
| `p8_fallas_20261030T2312.json`, `p8_fallas_20261030T2312.md` | P8 | Fallas bajo escritura: secundario, primario (stop y kill), sin mayoría, Valkey; RTO, 503 durante la elección, RPO = 0. | 2026-10-30T23:12:02 |
| `p8_fallas_20261030T2319.json`, `p8_fallas_20261030T2319.md` | P8 | Fallas bajo escritura: secundario, primario (stop y kill), sin mayoría, Valkey; RTO, 503 durante la elección, RPO = 0. | 2026-10-30T23:19:49 |

## Carpetas

- `p5/` (12 archivos): P5: resultados_principal_q1..q5.json (cuerpos completos de Q1-Q5, conjunto principal), plan_q1.txt / plan_q3.txt (explain formatted), validacion_*.json/txt (Q1-Q5 vs esperado.json), resumen_p5.json/md (tablas de tiempos).
- `p6/` (21 archivos): P6 (red): sim_<N>vu_run<k>.jsonl (un registro por petición: ts, endpoint, status, ms), resumen_*.json, docker_stats.csv, resumen_p6.md/json.
- `p6_red_previo/` (21 archivos): P6 (red) corrida previa al ajuste de keepalive: mismos tipos de archivo que p6/.
- `p6_host/` (21 archivos): P6 (host): mismos tipos de archivo que p6/, medidos vía localhost:8000.
- `p8/` (2 archivos): P8: operaciones.json (todas las operaciones del escritor con estado y latencia + acciones de recuperación), operaciones_<fecha>.json de la corrida anterior.
- `graficas/` (10 archivos): PNG generados por pruebas/graficas.py (latencias P6, envíos/min, CPU, tiempos P5, w:majority vs w:1, P4, línea de tiempo de P8).

## Corridas históricas conservadas

- `p2_concurrencia_20261030T2121.*`: P2 antes del índice extra `tareas_estado_prioridad_expira` (explain con el índice anterior; las 50 solicitudes salieron con una dispersión de inicio de 3-6 s porque el pool de procesos se creaba dentro de la ronda).
- `p8_fallas_20261030T2312.*`: P8 antes de la reconciliación de envíos ambiguos (6 tareas anotadas en Mongo sin evento en el lago tras un 503 por falta de mayoría).
- `p6_carga_red_previo_*` y `p6_carga_host_*`: ver arriba.

## Cómo reproducir

`pruebas/run_all.ps1` (orden: entorno, P1, P2, P3, P5, P5b resultados, P4, P6, P8, gráficas); luego `python pruebas/resumen.py` y `python pruebas/indice.py`.

