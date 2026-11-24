# RESUMEN de resultados experimentales (generado por pruebas/resumen.py)

Todas las cifras salen de los archivos de `evidence/` indicados entre paréntesis. Percentiles: nearest-rank. "no disponible" = no medido, con el motivo indicado.

## 0. Entorno (evidence/entorno.txt)

- Host: Windows-11-10.0.26200-SP0; CPU Intel Core i7-1365U (10 núcleos / 12 hilos lógicos, 1.8 GHz base); RAM del host 31.7 GiB.
- Docker 29.9.2 con 12 vCPU y 15.5 GiB de memoria para la VM (Docker Desktop, WSL2).
- Imágenes: mongo:7 (3 nodos), valkey/valkey:8, apache/spark:3.5.3 (master + workers), python:3.12-slim (servicios), Python 3.14 en el host.
- Ruido: en la misma máquina corren contenedores de otro curso (`ti4601-*`, Mongo/Neo4j/Jupyter) que consumen CPU de forma intermitente; no se detuvieron. Los tiempos describen este equipo compartido.
- Docker Desktop no puede montar carpetas de Google Drive (H:): el datalake vive en C:/p2work (variables `DATALAKE_*` en `.env`).

## P1. API y /salud (p1_api_20261030T2359.json)

- Casos HTTP probados: 40; con el código esperado: 40; discrepancias: 0.
- Estados de /salud (segundos hasta observar el estado tras detener el componente):

| Escenario | Estado | HTTP | s hasta verlo |
| --- | --- | --- | --- |
| degradado_analitica | degradado | 200 | 2.5 |
| degradado_historial | degradado | 200 | 2.3 |
| degradado_spark_worker | degradado | 200 | 1.2 |
| degradado_mongo_secundario | degradado | 200 | 15.1 |
| caido_valkey | caido | 503 | 3.1 |
| caido_mongo_sin_mayoria | caido | 503 | 16.1 |
| caido_registro | caido | 503 | 2.5 |
| caido_sesion | caido | 503 | 2.2 |

- Puertos publicados: gateway 0.0.0.0:8000, gateway [::]:8000, analitica 127.0.0.1:4040, spark-master 127.0.0.1:18080. Sólo el gateway (8000) es público; 4040 y 18080 son de administración y escuchan en 127.0.0.1.
- Conexión desde el host a puertos internos: {"127.0.0.1:27017 (mongo1)": "NO CONECTA (TimeoutError)", "127.0.0.1:6379 (valkey)": "NO CONECTA (TimeoutError)", "127.0.0.1:8001 (registro)": "NO CONECTA (TimeoutError)", "127.0.0.1:8002 (sesion)": "NO CONECTA (TimeoutError)", "127.0.0.1:8003 (historial)": "NO CONECTA (TimeoutError)", "127.0.0.1:8004 (analitica)": "NO CONECTA (TimeoutError)", "127.0.0.1:7077 (spark-master)": "NO CONECTA (TimeoutError)", "127.0.0.1:8000 (gateway (debe abrir))": "CONECTA"}.
- Puertos remotos de las conexiones TCP del simulador (host): [8000] (sólo 8000).
- Imports prohibidos hallados (debe ser todo vacío): {"registro importa redis/valkey": [], "sesion importa pymongo": [], "analitica importa pymongo": [], "simulador importa pymongo/redis": [], "pruebas importan pymongo/redis": []}.

## P2. Concurrencia (p2_concurrencia_20261030T2333.json)

- rs.status: {"mongo1:27017": "PRIMARY", "mongo2:27017": "SECONDARY", "mongo3:27017": "SECONDARY"}.
- 50 solicitudes simultáneas con 10 tareas libres (ProcessPoolExecutor, 50 procesos con barrera de reloj):

| Ronda | Códigos | Dispersión del inicio (ms) | Cumple |
| --- | --- | --- | --- |
| 1 | {"204": 40, "200": 10} | 27.0 | True |
| 2 | {"204": 40, "200": 10} | 25.3 | True |
| 3 | {"200": 10, "204": 40} | 11.1 | True |

- Segundo enviar: HTTP 409; fila extra en el lago: no.
- w:majority vs w:1 a través del gateway (media de rondas; enviar), ms:

| Modo | p50 | p95 | p99 | n | ciclos/s |
| --- | --- | --- | --- | --- | --- |
| majority | 174.24 | 300.54 | 365.97 | 1800 | 20.8 |
| w1 | 164.82 | 265.06 | 327.01 | 1800 | 22.5 |

- w:majority vs w:1 directamente en Mongo (1500 updateOne secuenciales x 3 pares, resolución 1 ms), ms:

| Modo | media | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| w:majority | 15.27 | 14.33 | 22.67 | 29.0 |
| w:1 | 4.66 | 4.0 | 7.33 | 10.33 |

- explain(executionStats) (plan, claves examinadas, docs examinados, ms):

  - 1_asignar_con_proyecto (tareas_proyecto_estado_prioridad + tareas_estado_expira): LIMIT > FETCH > SORT_MERGE > IXSCAN(tareas_proyecto_estado_prioridad) > FETCH > IXSCAN(tareas_proyecto_estado_prioridad); keys=1; docs=1; 1 ms
  - 1b_asignar_con_proyecto_forzando_COLLSCAN (comparacion): SORT > COLLSCAN; keys=0; docs=81740; 219 ms
  - 2_asignar_sin_proyecto (global): SUBPLAN > LIMIT > FETCH > SORT_MERGE > IXSCAN(tareas_estado_prioridad_expira) > IXSCAN(tareas_estado_prioridad_expira); keys=2; docs=1; 1 ms
  - 3_reanudar_tarea_abierta_del_anotador (tareas_anotador_estado): LIMIT > FETCH > IXSCAN(tareas_anotador_estado); keys=1; docs=1; 1 ms
  - 4_tareas_de_un_batch (tareas_batch): FETCH > IXSCAN(tareas_batch); keys=100; docs=100; 0 ms
  - 5_proyectos_activos_del_cliente (proyectos_cliente_estado): FETCH > IXSCAN(proyectos_cliente_estado); keys=1; docs=1; 0 ms
  - 6_EXTRA_tareas_abiertas_vencidas (tareas_estado_expira): FETCH > IXSCAN(tareas_estado_prioridad_expira); keys=101; docs=100; 1 ms
  - 6b_EXTRA_sin_indice_forzando_COLLSCAN (comparacion): COLLSCAN; keys=0; docs=81740; 50 ms
  - 7_exportar_aprobadas_paginado: SORT > FETCH > IXSCAN(tareas_proyecto_estado_prioridad); keys=22; docs=22; 2 ms

- Nota: `evidence/p2_concurrencia_20261030T2121.*` es una corrida anterior al índice extra `tareas_estado_prioridad_expira` (consulta de asignación sin proyecto: 59 992 claves y 105 ms con el índice anterior `tareas_estado_expira`); ver la última corrida para el resultado con el índice nuevo.

## P3. Sesión (p3_sesion_20261030T2331.json)

- Edad del borrador recuperado tras matar el editor (ms): {"n": 5, "min": 572, "max": 1801, "mediana": 883}; todas <= 5 s: True; contenido igual al último confirmado: True.
- Borrador tras `docker compose restart valkey`: True; tras `docker kill valkey` (SIGKILL): True.
- Candado renovado por latidos (TTL 10 s, 2.5 x TTL): True; tarea re-ofrecida a otro tras abandono: True (10.6 s tras el último latido); tareas con dos dueños: 0.
- Observación: entre ~8 y 10 s B recibió 409 transitorio (Mongo ofrece la tarea cuando `expira` vence, pero el lock de Valkey, renovado en cada borrador, aún no); el gateway compensa liberando y no hay doble dueño.

## P4. Historial (p4_historial_20261030T2216.json)

- Segundos entre HTTP 200 de enviar y evento visible en Spark (n=10): min 5.53, p50 5.9, p95 8.1, p99 8.1, máx 8.1. Archivo en disco: p50 2.32 s (máx 2.61).
- Deduplicación tras reprocesar (copia duplicada): conteo Spark sin cambio y filas crudas=2/dedup=1: True.
- Lecturas Spark concurrentes con escrituras: 4; errores: 0; archivos nuevos vigilados: 23.
- Parquet validados con pyarrow: 46; inválidos: 0.
- Rechazo: Parquet 'rechazada' con Mongo 'disponible': True.
- Compactación del mes: {"http": 200, "antes": {"archivos_mes": 36, "filas_crudas": 813026}, "despues": {"archivos_mes": 1, "filas_crudas": 813024}, "dedup_sin_cambio": true}.

## P4 (frescura hasta Q1) (p4_frescura_q1_20261031T0124.json)

- Segundos entre el HTTP 200 de la aprobación y la primera respuesta de Q1 que la cuenta (n=10, visibles=10): min 6.08, p50 9.5, p95 11.75, p99 11.75, máx 11.75. Archivo en disco: p50 2.49 s (máx 2.74).
- Consultas de Q1 por muestra: {"n": 10, "min": 1, "p50": 1, "p95": 1, "p99": 1, "max": 1}; resolución: una consulta de Q1 (p50 9.436 s, max 11.693 s).

## P5. Analítica (p5_analitica_20261030T2150.json; evidence/p5/)

- Validación Q1-Q5 contra `datalake_validacion/esperado.json` (56 eventos únicos + 4 duplicados): todas coinciden = True (evidence/p5/validacion_resultado.json).
- Lago principal antes: {"archivos": 272, "filas": 812244, "event_id_distintos": 812244, "bytes_MiB": 50.8}; después de compactar: {"archivos": 12, "filas": 812244, "event_id_distintos": 812244, "bytes_MiB": 47.8}.
- Ejecutores con 2 workers: {"app_id": "app-20261031040527-0007", "ejecutores_activos": 2, "cores_totales_ejecutores": 4, "hosts": ["172.19.0.11", "172.19.0.12"], "workers_vivos_en_master": 2, "cores_cluster": 4}.

Tiempos (s) medidos por el servicio; fría = primera tras reiniciar `analitica`; caliente = mediana de las rondas posteriores:

| Config | Q1 fría/caliente | Q2 | Q3 | Q4 | Q5 |
| --- | --- | --- | --- | --- | --- |
| C0 original, 272 archivos, 1 worker (2 rondas calientes) | 64.216 / 44.26 | 26.518 / 23.606 | 32.52 / 25.413 | 25.271 / 23.847 | 28.013 / 24.334 |
| C1 original, compactado, 1 worker (2 rondas calientes) | 34.841 / 6.587 | 15.429 / 4.544 | 10.414 / 5.332 | 5.801 / 4.595 | 7.758 / 5.075 |
| C2 final, compactado, 1 worker (5 rondas calientes) | 22.804 / 3.886 | 6.645 / 3.432 | 7.016 / 3.529 | 4.799 / 3.575 | 5.408 / 3.425 |
| C3 final, 2 workers (5 rondas calientes) | 28.29 / 3.59 | 7.46 / 3.163 | 8.083 / 3.978 | 5.939 / 2.968 | 6.294 / 4.574 |
| G2 conjunto grande 10 M, 2 workers (1 ronda caliente) | 35.106 / 7.014 | 15.338 / 9.179 | 19.127 / 14.078 | 15.425 / 11.84 | 18.469 / 15.372 |
| G1 conjunto grande 10 M, 1 worker (1 ronda caliente) | 34.712 / 6.446 | 17.517 / 12.322 | 28.939 / 15.546 | 17.456 / 10.423 | 15.47 / 12.712 |

- Conjunto grande: {"archivos": 12, "filas": 10000000, "event_id_distintos": 10000000, "bytes_MiB": 549.9} (generado en 232 s con `dataset/generar_historial.py --eventos 10000000`; 1602 archivos compactados a 12 en 67 s con `dataset/compactar_lago.py`). Memoria con el conjunto grande (docker stats): analitica ~0.9 GiB, worker ~2.0-2.7 GiB de 2 GiB de executor; sin fallos.
- Planes: evidence/p5/plan_q1.txt (filtro `mes >= ...` en PartitionFilters; agrupación por día, modalidad) y evidence/p5/plan_q3.txt (Exchange rangepartitioning + Sort por la ordenación final).
- Cuello de botella hallado: ejecutores casi ociosos (CPU 0.9-2 s de 5-6 s por tarea) esperando E/S de los ~190-270 archivos pequeños por el montaje 9p de Docker Desktop; la compactación a 1 archivo/mes y filtrar antes de deduplicar redujeron los tiempos calientes (ver tabla). Con 2 workers la mejora es pequeña: son contenedores del mismo equipo.

### P5b. Resultados completos de Q1-Q5 sobre el conjunto principal (p5_resultados_20261031T0116.json; evidence/p5/resultados_principal_q1..q5.json)

- Lago al medir: {"archivos": 12, "filas": 900941, "event_id_distintos": 900941, "bytes_MiB": 53.2} (max ts 2026-10-31T06:19:27.898000+00:00). Una ejecución medida por consulta tras calentamiento (sesión Spark ya creada).

| Q | Filas | tiempo_s (servicio) | pared cliente (ms) |
| --- | --- | --- | --- |
| Q1 | 364 | 5.179 | 5223 |
| Q2 | 361 | 4.246 | 4334 |
| Q3 | 5 | 4.3 | 4330 |
| Q4 | 185 | 4.346 | 4399 |
| Q5 | 64 | 4.405 | 4436 |

- Q1: 34745 aprobadas en 91 días (2026-08-02 a 2026-10-31); por modalidad {"imagen": 10529, "lidar": 6780, "texto": 10060, "video": 7376}; mediana diaria 280, mínimo 126 (2026-09-27), máximo 10843 (2026-10-31); sin el último día: 23902. Eventos crudos del último día en el lago: 100941 (los días previos: 2026-10-28 2033, 2026-10-29 2158, 2026-10-30 2111).
- Q2: 30 equipos (+1 fila sin equipo), 12 meses, 361 filas; proporción aprobadas mín 0.2831, mediana 0.6835, máx 0.9592; global 0.669.
- Q3: 187483 anotaciones; media mayor lidar/cuboide_3d 4140.953623 s (p95 11776.701 s); media menor imagen/bbox 250.25281 s.
- Q4: 185 pares cliente-proyecto, 65 clientes; proporción de rechazo global 0.331; por par mín 0.125, mediana 0.3233, máx 1.0.
- Q5: 64 clientes, 111411 aprobaciones; mediana entre clientes de p50 12356.064 s y de p95 99618.398 s; % SLA incumplido mediana 1.679, máx 22.0174, global ponderado 3.1864.

## P6 red (simulador dentro de la red docker, contra gateway:8000) - medición principal (evidence/p6/resumen_p6.json, .md y *.jsonl; fecha 2026-10-31T00:02:57)

| VUs | envíos/min (media; min-máx) | req/s | peticiones | errores 5xx/red | tasa error % |
| --- | --- | --- | --- | --- | --- |
| 10 | 65.1 (62.7-66.7) | 8.8 | 2379 | 0 | 0.0 |
| 40 | 250.0 (248.0-251.3) | 34.8 | 9400 | 0 | 0.0 |
| 100 | 595.3 (583.3-605.3) | 83.2 | 22464 | 0 | 0.0 |

Latencias (ms) p50 / p95 / p99 por endpoint (3 corridas x 90 s, tras 30 s de calentamiento no medido):

| Endpoint | 10 VUs | 40 VUs | 100 VUs |
| --- | --- | --- | --- |
| GET /proyectos/{id}/exportar | 20.42 / 33.89 / 39.98 (n=136) | 20.67 / 36.41 / 45.96 (n=516) | 38.36 / 107.35 / 165.78 (n=1241) |
| GET /salud | 15.55 / 21.78 / 26.55 (n=36) | 16.57 / 36.25 / 46.52 (n=164) | 38.69 / 140.71 / 235.92 (n=376) |
| POST /sesiones/recuperar | 15.32 / 23.86 / 23.86 (n=8) | 14.62 / 29.19 / 31.88 (n=30) | 39.72 / 421.97 / 591.34 (n=71) |
| POST /tareas/siguiente | 31.23 / 131.4 / 184.07 (n=329) | 31.4 / 444.68 / 558.25 (n=1258) | 69.89 / 1241.17 / 2097.63 (n=3016) |
| POST /tareas/{id}/enviar | 32.69 / 47.11 / 58.77 (n=293) | 30.78 / 52.7 / 77.4 (n=1125) | 66.87 / 300.48 / 693.26 (n=2679) |
| POST /tareas/{id}/liberar | 23.56 / 43.63 / 46.05 (n=36) | 54.37 / 228.96 / 252.42 (n=133) | 292.97 / 870.84 / 995.93 (n=337) |
| POST /tareas/{id}/revision | 20.65 / 29.37 / 33.2 (n=496) | 20.04 / 33.91 / 50.84 (n=2022) | 37.54 / 135.37 / 327.85 (n=4858) |
| PUT /tareas/{id}/borrador | 10.3 / 16.24 / 25.1 (n=1045) | 9.61 / 18.87 / 28.47 (n=4152) | 21.52 / 119.83 / 290.26 (n=9886) |

docker stats (CPU % media; memoria máx MiB) de los contenedores principales:

| Contenedor | 10 VUs | 40 VUs | 100 VUs |
| --- | --- | --- | --- |
| gateway | 8.6 ; 59.4 | 22.9 ; 61.9 | 70.4 ; 60.2 |
| registro | 5.1 ; 54.0 | 10.2 ; 47.6 | 28.4 ; 57.2 |
| sesion | 4.7 ; 45.3 | 11.0 ; 47.4 | 28.1 ; 41.6 |
| valkey | 2.7 ; 56.3 | 3.5 ; 60.1 | 9.6 ; 67.6 |
| mongo1 | 22.5 ; 505.1 | 26.1 ; 505.4 | 54.5 ; 530.8 |
| mongo2 | 17.6 ; 383.2 | 23.8 ; 404.9 | 41.2 ; 434.1 |
| mongo3 | 19.1 ; 373.8 | 22.5 ; 429.5 | 44.0 ; 444.8 |
| historial | 1.2 ; 47.4 | 2.0 ; 49.1 | 4.6 ; 49.7 |
| herramientas | 3.6 ; 24.9 | 11.3 ; 29.7 | 37.9 ; 37.3 |

### P6 red, corrida previa al ajuste de keepalive del gateway (1 error 503 de 19 048 peticiones) (evidence/p6_red_previo/resumen_p6.json)

| VUs | envíos/min (media; min-máx) | req/s | peticiones | errores 5xx/red | tasa error % |
| --- | --- | --- | --- | --- | --- |
| 10 | 64.0 (61.3-66.7) | 8.4 | 2280 | 0 | 0.0 |
| 40 | 249.8 (248.0-251.3) | 33.8 | 9124 | 0 | 0.0 |
| 100 | 524.9 (478.0-554.7) | 70.6 | 19048 | 1 | 0.005 |

Latencias (ms) p50 / p95 / p99 por endpoint (3 corridas x 90 s, tras 30 s de calentamiento no medido):

| Endpoint | 10 VUs | 40 VUs | 100 VUs |
| --- | --- | --- | --- |
| GET /proyectos/{id}/exportar | 16.65 / 31.41 / 51.76 (n=67) | 16.41 / 35.61 / 44.61 (n=237) | 97.08 / 495.21 / 1425.27 (n=678) |
| GET /salud | 16.3 / 27.02 / 27.2 (n=26) | 18.45 / 37.58 / 47.66 (n=165) | 126.57 / 542.98 / 901.94 (n=346) |
| POST /sesiones/recuperar | 12.13 / 25.52 / 25.52 (n=11) | 17.57 / 41.75 / 107.91 (n=34) | 166.06 / 1102.62 / 2449.59 (n=74) |
| POST /tareas/siguiente | 27.24 / 86.08 / 179.95 (n=318) | 33.13 / 326.79 / 493.3 (n=1264) | 282.29 / 2188.39 / 2945.9 (n=2683) |
| POST /tareas/{id}/enviar | 27.07 / 48.45 / 97.73 (n=288) | 31.88 / 54.55 / 82.88 (n=1124) | 251.5 / 1284.92 / 2382.16 (n=2363) |
| POST /tareas/{id}/liberar | 29.6 / 37.88 / 38.39 (n=30) | 56.54 / 328.81 / 391.88 (n=140) | 608.08 / 1588.98 / 1812.32 (n=318) |
| POST /tareas/{id}/revision | 18.94 / 30.37 / 46.37 (n=508) | 20.52 / 38.05 / 71.33 (n=2022) | 117.01 / 514.99 / 1421.56 (n=4334) |
| PUT /tareas/{id}/borrador | 9.44 / 17.17 / 26.12 (n=1032) | 10.38 / 21.75 / 38.71 (n=4138) | 94.31 / 659.92 / 1433.24 (n=8252) |

docker stats (CPU % media; memoria máx MiB) de los contenedores principales:

| Contenedor | 10 VUs | 40 VUs | 100 VUs |
| --- | --- | --- | --- |
| gateway | 7.6 ; 66.2 | 22.9 ; 67.6 | 86.8 ; 68.4 |
| registro | 5.6 ; 55.6 | 9.3 ; 55.5 | 29.8 ; 55.8 |
| sesion | 6.2 ; 49.9 | 9.8 ; 39.5 | 32.4 ; 48.0 |
| valkey | 2.1 ; 21.5 | 2.7 ; 23.4 | 8.5 ; 31.3 |
| mongo1 | 16.9 ; 465.1 | 26.8 ; 467.5 | 58.2 ; 506.3 |
| mongo2 | 16.5 ; 424.1 | 24.8 ; 447.6 | 50.3 ; 477.5 |
| mongo3 | 14.1 ; 425.4 | 24.4 ; 431.3 | 49.1 ; 466.1 |
| historial | 1.4 ; 372.3 | 2.2 ; 372.6 | 4.9 ; 372.7 |
| herramientas | 3.1 ; 24.6 | 11.7 ; 29.0 | 50.2 ; 37.0 |

### P6 host Windows -> localhost:8000 (reenvío de puertos de Docker Desktop) (evidence/p6_host/resumen_p6.json)

| VUs | envíos/min (media; min-máx) | req/s | peticiones | errores 5xx/red | tasa error % |
| --- | --- | --- | --- | --- | --- |
| 10 | 56.4 (55.3-58.0) | 7.6 | 2052 | 1 | 0.049 |
| 40 | 94.0 (89.3-101.3) | 8.8 | 2368 | 1 | 0.042 |
| 100 | 109.4 (96.7-124.7) | 10.0 | 2680 | 3 | 0.112 |

Latencias (ms) p50 / p95 / p99 por endpoint (3 corridas x 90 s, tras 30 s de calentamiento no medido):

| Endpoint | 10 VUs | 40 VUs | 100 VUs |
| --- | --- | --- | --- |
| GET /proyectos/{id}/exportar | 49.43 / 341.73 / 1047.32 (n=56) | 2865.71 / 9649.6 / 16286.91 (n=90) | 8363.98 / 28016.15 / 36843.6 (n=124) |
| GET /salud | 51.37 / 129.03 / 1147.7 (n=41) | 2323.54 / 9355.18 / 11501.37 (n=79) | 9292.99 / 29666.17 / 35127.06 (n=79) |
| POST /sesiones/recuperar | 89.5 / 1406.88 / 1406.88 (n=7) | 2625.96 / 11698.5 / 11698.5 (n=17) | 8532.35 / 21154.59 / 21154.59 (n=18) |
| POST /tareas/siguiente | 113.36 / 404.72 / 996.28 (n=296) | 2077.19 / 9861.59 / 14400.0 (n=501) | 5561.97 / 25681.8 / 36993.61 (n=662) |
| POST /tareas/{id}/enviar | 109.53 / 288.75 / 1008.01 (n=255) | 2079.65 / 10479.24 / 15187.73 (n=423) | 7213.25 / 27699.88 / 43859.7 (n=493) |
| POST /tareas/{id}/liberar | 152.32 / 388.11 / 525.44 (n=41) | 1493.62 / 6406.9 / 9986.71 (n=78) | 3969.85 / 14745.79 / 42537.6 (n=169) |
| POST /tareas/{id}/revision | 94.15 / 196.89 / 587.99 (n=438) | 2761.05 / 9658.78 / 14856.64 (n=419) | 7564.5 / 28730.02 / 38560.35 (n=432) |
| PUT /tareas/{id}/borrador | 87.3 / 290.67 / 843.79 (n=918) | 2682.98 / 8950.2 / 15468.95 (n=761) | 7750.36 / 27806.24 / 41410.81 (n=703) |

docker stats (CPU % media; memoria máx MiB) de los contenedores principales:

| Contenedor | 10 VUs | 40 VUs | 100 VUs |
| --- | --- | --- | --- |
| gateway | 8.3 ; 51.6 | 13.2 ; 59.9 | 15.7 ; 63.5 |
| registro | 4.4 ; 55.0 | 8.5 ; 55.2 | 8.4 ; 56.4 |
| sesion | 4.4 ; 47.2 | 8.1 ; 48.3 | 7.8 ; 43.2 |
| valkey | 1.5 ; 14.0 | 2.8 ; 17.6 | 2.7 ; 19.7 |
| mongo1 | 18.4 ; 446.8 | 30.6 ; 451.2 | 27.3 ; 457.6 |
| mongo2 | 20.3 ; 412.4 | 28.0 ; 422.0 | 25.4 ; 431.3 |
| mongo3 | 17.1 ; 412.8 | 28.4 ; 424.7 | 25.7 ; 422.7 |
| historial | 1.2 ; 414.0 | 1.8 ; 372.5 | 1.5 ; 372.5 |

- Interpretación P6: con el simulador dentro de la red docker la plataforma escala de 10 a 100 VUs sin errores; a 100 VUs el gateway (un solo proceso uvicorn) llega a 70 % de CPU media y las latencias de cola (p95 de siguiente ~1.2 s) crecen. Medido desde el host a través de `localhost:8000` el reenvío de puertos de Docker Desktop limita el rendimiento a ~10 req/s con latencias de segundos aunque los servicios internos respondan en <20 ms (comprobado con el mismo simulador dentro del contenedor del gateway); esa corrida se conserva como evidencia de la limitación del entorno.
- Tareas `disponible` en Mongo: 53771 al empezar la última corrida de P6 y 50056 al terminar (de 60 000 iniciales; las pruebas anteriores y las corridas previas de P6 consumieron el resto): no hizo falta reponerlas ni volver a cargar el dataset.
- Corridas P6 en la red: una primera (`p6_red_previo`) mostró 1 error 503 (keep-alive gateway->sesión); tras ajustar el gateway una corrida intermedia mostró 2 `ReadError` del propio simulador (keep-alive cliente->gateway, sin errores en los logs del gateway) y se repitió tras ajustar el simulador; esta última (carpeta `p6/`) tuvo 0 errores y es la que se reporta. La intermedia no se conserva.

## P8. Fallas y recuperación (p8_fallas_20261030T2319.json; evidence/p8/operaciones.json)

| Escenario | siguiente (códigos) | enviar (códigos) | RTO / ventana sin escrituras / fallos |
| --- | --- | --- | --- |
| S0 línea base (15 s) | {'200': 239} | {'200': 240} | - |
| S1 secundario detenido | {'200': 385} | {'200': 387} | rto_s=0.0; ventana_s=0.0; fallos=0 |
| S2a primario detenido (docker stop) (nuevo primario: mongo2) | {'200': 391} | {'200': 393} | rto_s=0.0; ventana_s=0.0; fallos=0 |
| S2b primario muerto (docker kill) (nuevo primario: mongo2) | {'200': 209, '503': 9} | {'200': 209, '503': 9} | rto_s=10.9; ventana_s=10.8; fallos=18 |
| S3 dos secundarios detenidos (sin mayoría) | {'200': 1} | {'503': 90} | tras reiniciar los nodos: 14.4 s hasta replica set completo y 15.8 s hasta el primer envío exitoso; el reintento del usuario: HTTP 200 (reconciliado=True) |
| S4 reinicio de Valkey a mitad de sesión | {'200': 160, '503': 1} | {'200': 159} | borrador sigue: True; enviar tras reinicio HTTP 200; rto_s=2.5; fallos=1 |
| S5 sesión interrumpida y recuperada | - | - | recuperar HTTP 200; edad del borrador 1178 ms; enviar tras recuperar HTTP 200 |

- Sin mayoría (S3), respuesta de cada ruta mientras Mongo no puede confirmar:
  - POST /tareas/siguiente (escritura): HTTP 503 (3063 ms)
  - POST /tareas/{id}/enviar (escritura): HTTP 503 (3255 ms)
  - PUT /tareas/{id}/borrador (sólo Valkey): HTTP 200 (10 ms)
  - GET /proyectos/{id}/exportar (lectura Mongo): HTTP 503 (8687 ms)
  - POST /sesiones/recuperar: HTTP 503 (2874 ms)
  - GET /proyectos/{id}/editando (Valkey): HTTP 200 (26 ms)
  - GET /analitica/q1 (sólo Parquet): HTTP 200 (23022 ms)
  - GET /salud: HTTP 503 (2146 ms)

- RPO: {"envios_con_http_200": 3999, "de_ellos_reconciliados (503 previo, ya aplicado en Mongo)": 7, "tareas_perdidas_en_mongo (RPO)": 0, "con_estado_incorrecto": 0, "tareas_con_dos_acks_200": 0, "eventos_ack_ausentes_en_lago": 0, "tareas_con_mas_de_una_fila_anotada_en_lago": 0, "envios_nunca_confirmados (todos los intentos 503/error o 409)": 0, "de_esos_con_409_tras_un_503 (commit ambiguo: estaban ya anotadas)": 0, "de_esos_estado_en_mongo": {}, "ejemplos_perdidas": []}.
- Hallazgo y corrección: en la primera corrida (evidence/p8_fallas_20261030T2312.*) 6 envíos que dieron 503 por falta de mayoría (WriteConcernTimeout) SÍ se habían aplicado en Mongo; al reintentar se obtenía 409 y su evento nunca llegó al datalake. Se añadió la reconciliación (`confirmar_envio` en registro, indicador `previo` en sesión y lógica en el gateway) que republica el mismo evento (mismo event_id) y responde 200 si el envío ya estaba aplicado y quedó un pendiente en Valkey.

## Errores corregidos durante esta fase

- `registro`: índice extra reemplazado por `tareas_estado_prioridad_expira` (asignación sin proyecto pasó de 59 992 claves/105 ms a 2 claves/~1 ms; P2).
- `gateway`: `keepalive_expiry=2 s` hacia registro/sesión y reintento del PUT idempotente del envío pendiente (503 espurio por conexión keep-alive cerrada, visto con 100 VUs en P6).
- Reconciliación de envíos ambiguos tras 503 (registro `confirmar_envio`, sesión `previo`, gateway) - ver P8.
- `analitica`: ruta `GET /evento/{id}` (3.ª de 6) para P4; filtro antes de deduplicar (`OPT_FILTRO_PRIMERO`); herramienta `dataset/compactar_lago.py`.

## Limitaciones declaradas

- Todo corre en una sola máquina: w:1 vs w:majority, 1 vs 2 workers de Spark y las fallas de nodos no permiten inferir el comportamiento de un clúster en varias máquinas.
- El conjunto principal contiene, además de los ~800 000 eventos generados, los eventos de las pruebas (decenas de miles); se compactó antes de las mediciones P5 pero P4/P6/P8 vuelven a crear archivos pequeños (se compacta de nuevo al final).
- Las medidas de P6 mezclan la capacidad de la plataforma con la del propio generador de carga y los contenedores de otro curso que comparten el equipo.
- El historial no compacta automáticamente: en producción haría falta un trabajo periódico (POST /compactar) porque cada micro-lote crea un archivo.

