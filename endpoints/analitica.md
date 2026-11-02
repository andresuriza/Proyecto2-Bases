# Analítica (interno `http://analitica:8004`)

FastAPI + PySpark 3.5.3 con `SparkSession` caliente (misma imagen base `apache/spark:3.5.3` que master y workers: Java 11, Python 3.8; sólo DataFrame API, sin UDF). Lee **sólo** Parquet bajo `<lago>/eventos` en modo lectura (`:ro`); no accede a Mongo ni al historial. Con Valkey sólo publica latidos `salud:analitica` y `salud:spark` (EX 15). Master en `SPARK_MASTER` (`local[*]` para pruebas sin clúster); el driver anuncia `spark.driver.host=analitica` con puertos fijos 7078/7079.

| Ruta | Descripción |
|---|---|
| `GET /q/{q1..q5}?conjunto=principal\|grande\|validacion&explain=0\|1&contar=0\|1` | `{consulta, conjunto, filas, n_filas, tiempo_s, n_ejecucion, eventos_leidos? (contar=1), plan? (explain=1: explain formatted tras ejecutar)}`; 404 conjunto sin datos; 503 fallo de Spark |
| `GET /evento/{event_id}?conjunto=principal` | Comprobación de visibilidad (prueba P4): `{event_id, filas_crudas, filas_dedup, estado, tiempo_s}`; lee los Parquet en ese instante (sin caché), sin deduplicar para exponer duplicados crudos. |
| `GET /salud` | 200 `{estado, detalle, spark:{estado, detalle}, master}` (estado del master por su API JSON) |

Conjuntos: `principal`=/datalake, `grande`=/datalake_grande, `validacion`=/datalake_validacion. Todas las consultas deduplican por `event_id` y usan zona UTC; duraciones en segundos; percentiles con `percentile_approx(accuracy=100000)` (exactos hasta 100 000 valores; definición nearest-rank).

| Q | Salida |
|---|---|
| q1 | `dia, modalidad, aprobadas` (ts > max(ts) − 90 días; además `mes >= mes(corte)` para podar particiones) |
| q2 | `equipo_id, mes, aprobadas, rechazadas, proporcion_aprobadas` (cuenta eventos de revisión, no tareas distintas: rechazada 2 veces y luego aprobada = 2 rechazadas + 1 aprobada) |
| q3 | `modalidad, tipo_tarea, n, dur_media_s, dur_p95_s` (duración = ts(anotada) − tomada_en; una duración por cada toma que terminó en `anotada`) |
| q4 | `cliente_id, proyecto_id, rechazadas, aprobadas, proporcion_rechazadas` (mismos eventos de revisión que q2) |
| q5 | `cliente_id, n, p50_s, p95_s, pct_incumple_sla` (ts(aprobada) − tomada_en > sla_horas·3600; `tomada_en` es el de la última toma, pues rechazar limpia la asignación y la nueva toma lo reinicia; medir desde la primera toma no está implementado) |

CLI para mediciones frío/caliente y 1 vs 2 workers: dentro del contenedor `analitica`, `python3 consultas.py --q all --ruta /datalake/eventos --repeticiones 3 --salida /tmp/out.json` (usa `SPARK_MASTER`; también funciona con `spark-submit`). Variables: `SPARK_SHUFFLE_PARTITIONS` (16), `SPARK_EXECUTOR_MEMORY` (2g), `SPARK_DRIVER_MEMORY` (2g), `SPARK_CORES_MAX`, `SPARK_WARMUP` (0/1).

## Rendimiento (resultado de P5)
`OPT_FILTRO_PRIMERO=1` (por defecto) aplica el filtro de cada consulta antes de `dropDuplicates(event_id)`; `0` restablece el orden original (sólo para comparar). La mejora principal es operativa: el historial publica un archivo por micro-lote (muchos archivos pequeños) y el montaje 9p de Docker Desktop hace lenta cada apertura, por lo que conviene compactar (`POST /compactar?mes=` del historial; para `eventos_grande` `dataset/compactar_lago.py`). Ver `evidence/p5/` y `evidence/RESUMEN.md`.
