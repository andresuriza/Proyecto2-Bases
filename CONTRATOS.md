# Contratos internos (fuente única de verdad para todos los componentes)

Enunciado: `proyecto2-enunciado.pdf`. Plan: ver secciones abajo. Stack: Python 3.12 + FastAPI + uvicorn, imágenes `python:3.12-slim`, salvo analítica: usa la imagen `apache/spark:3.5.3` (Python 3.8, Java 11; ver sección Historial, analitica y dataset).

## Red y puertos (docker-compose, red interna `p2net`)
| Servicio | Host interno | Puerto interno | Publicado al host |
|---|---|---|---|
| gateway | gateway | 8000 | **8000:8000 (único)** |
| registro | registro | 8001 | no |
| sesion | sesion | 8002 | no |
| historial | historial | 8003 | no |
| analitica | analitica | 8004 | Spark UI del driver en `127.0.0.1:4040` (sólo localhost); la API (8004) no se publica |
| mongo1..3 | mongo1, mongo2, mongo3 | 27017 | no (ningún puerto de Mongo se publica al host) |
| valkey | valkey | 6379 | no |
| spark-master | spark-master | 7077 / UI 8080 | UI en `127.0.0.1:18080` (mapeo `127.0.0.1:18080:8080`, sólo localhost) |
| spark-worker-1 (siempre), spark-worker-2 (perfil `spark2`) | spark-worker-1, spark-worker-2 | | no |

Variables de entorno comunes:
- `MONGO_URI=mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0`
- `MONGO_W=majority` (P2 compara con `1`)
- `VALKEY_URL=redis://valkey:6379/0`
- `LOCK_TTL_MS=120000` (las pruebas pueden bajarlo)
- `DATALAKE_DIR=/datalake` (bind mount de `./datalake`), `DATALAKE_GRANDE_DIR=/datalake_grande` (opcional)
- `DATALAKE_VALIDACION_DIR=/datalake_validacion`
- `SPARK_MASTER=spark://spark-master:7077`
- `OPT_FILTRO_PRIMERO=1` (P5; `0` = deduplicar antes de filtrar)

Reglas de aislamiento: registro sólo importa pymongo (nunca redis); sesion sólo redis/valkey (nunca pymongo) y no decide estado; analitica sólo lee el lago (`DATALAKE_DIR`, `DATALAKE_GRANDE_DIR`, `DATALAKE_VALIDACION_DIR`) en `:ro`; simulador/pruebas sólo HTTP a `http://localhost:8000` (o `http://gateway:8000` dentro de la red, perfil `tools`).

Cabeceras propagadas por el gateway: `X-Request-ID` (se genera si falta), `X-Anotador-ID`.

## Valkey
- `lock:tarea:{id}` → valor = token; `SET NX PX LOCK_TTL_MS`; renovación con script Lua compare-and-pexpire; liberación compare-and-del.
- `sesion:{anotador}` HASH: tarea_id, proyecto_id, etapa, token, actualizado (epoch ms)
- `borrador:{tarea}` HASH: anotador_id, token, contenido (JSON str), ts (epoch ms), version_local
- `editando:{proyecto}` ZSET: miembro = anotador_id, score = epoch ms del último latido
- `salud:{capa}` STRING JSON {estado: ok|degradado|caido, detalle, ts} con EX 15; capas: gateway, registro, sesion, historial, analitica, mongo, valkey, spark. Cada servicio publica su latido cada 5 s.
- Stream de eventos: `eventos` (XADD por el gateway tras 200 de Mongo); grupo de consumidores `historial`.

## Registro (MongoDB, db `plataforma`)
Colecciones: clientes, proyectos, batches, tareas, anotadores. `_id` = UUID string.
Tarea:
```
{_id, proyecto_id, cliente_id, batch_id, modalidad: texto|imagen|video|lidar,
 tipo_tarea: span|bbox|poligono|bbox_video|cuboide_3d, prioridad:int(1..5),
 archivo:{uri, sha256, bytes, mime, meta:{...}},
 estado: disponible|abierta|anotada|en_revision|aprobada,
 asignacion:{anotador_id, equipo_id, token, tomada_en, expira} | null,
 anotacion:{...}|null, ultima_revision:{revisor_id, resultado, motivo_codigo, ts}|null,
 version:int, ultimo_evento_id, creada_en, actualizada_en}
```
Proyecto: {_id, cliente_id, nombre, modalidad, taxonomia:[{etiqueta, instruccion}], reglas, sla_horas, estado: activo|cerrado, creado_en}
Anotador: {_id, nombre, equipo_id, rol: anotador|revisor}
Índices obligatorios: tareas(proyecto_id,estado,prioridad), tareas(asignacion.anotador_id,estado), tareas(batch_id), proyectos(cliente_id,estado). Extra justificado: tareas(estado, prioridad desc, asignacion.expira) (asignación sin filtro de proyecto y re-oferta de tareas vencidas; ver evidence/p2_*).

Rutas internas registro (≤6): `POST /proyectos`, `POST /proyectos/{id}/lotes`, `POST /tareas/asignar`, `POST /tareas/{id}/transicion` (accion: enviar|liberar|iniciar_revision|aprobar|rechazar), `GET /proyectos/{id}/exportar?cursor=&limite=`, `GET /salud`.
Cada transición válida devuelve `{tarea, evento}` donde evento es la fila Parquet (ver abajo). Conflicto → 409; Mongo no disponible (ServerSelectionTimeout/NotPrimary/WTimeout) → 503.

Rutas internas sesion (≤6): `POST /sesiones/abrir` (adquiere lock, crea sesion), `PUT /borradores/{tarea}`, `POST /sesiones/recuperar`, `POST /locks/{tarea}/liberar`, `GET /proyectos/{id}/editando`, `GET /salud`.
Rutas internas historial: `GET /salud`, `GET /estado` (lag, últimos lotes), `POST /compactar?mes=`, `POST /reprocesar` (extra, prueba P4).
Rutas internas analitica: `GET /q/{q1..q5}?conjunto=principal|grande|validacion&explain=0|1&contar=0|1`, `GET /evento/{event_id}`, `GET /salud`.

## Evento (fila Parquet) — esquema fijo
| campo | tipo |
|---|---|
| event_id | string (uuid) |
| ts | timestamp[ms, UTC] |
| mes | string AAAA-MM (también columna de partición `mes=`) |
| tarea_id, proyecto_id, cliente_id, batch_id | string |
| equipo_id, anotador_id, revisor_id | string (nullable) |
| modalidad | string (texto/imagen/video/lidar) |
| tipo_tarea | string |
| estado | string: abierta/anotada/en_revision/aprobada/rechazada/disponible (la fila de `liberar` lleva el estado que queda en Mongo) |
| motivo | string nullable (código de rechazo) |
| version | int32 |
| tomada_en | timestamp[ms, UTC] nullable (momento de la toma vigente) |
| sla_horas | int32 |

Layout: `datalake/eventos/mes=AAAA-MM/part-<batchid>.parquet` (batchid = `<primer_id>_<ultimo_id>` del micro-lote del stream); temporales en `datalake/_tmp/` y publicación por `os.replace`. Conjunto grande: `datalake_grande/eventos/...` mismo esquema.

Definiciones de consultas (sobre eventos deduplicados por event_id):
- Q1: count(estado='aprobada') por día(ts) y modalidad, últimos 90 días respecto de max(ts) del conjunto.
- Q2: aprobadas / (aprobadas+rechazadas) por equipo_id y mes. Cuenta EVENTOS de revisión (filas `aprobada` y `rechazada`), no tareas distintas: una tarea rechazada dos veces y luego aprobada aporta 1 aprobada y 2 rechazadas. Justificación: cada revisión es una decisión de calidad independiente; la proporción mide la tasa de aceptación por revisión.
- Q3: duración = ts(anotada) − tomada_en por tarea/toma; media y p95 por modalidad y tipo_tarea. Mide cada toma que terminó en `anotada` (una tarea re-tomada tras un rechazo o liberación aporta una duración por toma anotada).
- Q4: rechazadas / (aprobadas+rechazadas) por cliente_id y proyecto_id. Misma semántica por evento que Q2 (complementaria de la proporción de aprobadas).
- Q5: ts(aprobada) − tomada_en: p50, p95 por cliente_id y % con duración > sla_horas. `tomada_en` es el de la fila `aprobada`, es decir, la ÚLTIMA toma (la asignación conserva `tomada_en` hasta aprobar; `rechazar` la limpia y la nueva toma lo reinicia), así que el SLA se mide desde la última toma hasta la aprobación. Medirlo desde la primera toma de la tarea NO está implementado.

## Build (convención de Dockerfiles) — OBLIGATORIA para historial y analitica
- Contexto de build de todos los servicios Python: `./servicios` (compose: `build: {context: ./servicios, dockerfile: <svc>/Dockerfile}`).
- Dockerfile de cada servicio en `servicios/<svc>/Dockerfile`, con rutas relativas a `servicios/`:
  ```
  FROM python:3.12-slim            # (analitica: imagen con Java 17 + PySpark; ver nota Spark)
  ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
  WORKDIR /app
  COPY <svc>/requirements.txt /tmp/requirements.txt
  RUN pip install -r /tmp/requirements.txt
  COPY common /app/common
  COPY <svc> /app/<svc>
  CMD ["uvicorn", "<svc>.main:app", "--host", "0.0.0.0", "--port", "<puerto>"]
  ```
- Los servicios importan `from common import config` (`MONGO_URI`, `VALKEY_URL`, `LOCK_TTL_MS`, `DATALAKE_DIR`, ...), `from common.logs import setup_logging, RequestIDMiddleware` y, si usan Valkey, `from common.heartbeat import bucle_latidos, publicar`.
- Latidos: `asyncio.create_task(bucle_latidos(redis_asyncio_client, "historial", sonda))` en el lifespan; `sonda()` devuelve `(estado, detalle)`. Analítica además publica `salud:spark`.
- `GET /salud` de historial/analitica: HTTP 200 con JSON `{"estado": "ok|degradado|caido", "detalle": "..."}`.
- Healthcheck de compose (opcional) con `python -c "import urllib.request ..."` (no hay curl en slim).
- Spark: compose usa `apache/spark:3.5.3` (master/workers). El driver (analitica) debe usar la misma versión menor de Python que los workers si PySpark ejecuta UDFs de Python; evitar UDFs.
- `herramientas` (perfil `tools`): `docker compose --profile tools run --rm herramientas python <script>`; repo montado en `/src`, `GATEWAY_URL=http://gateway:8000`, `MONGO_URI`, `VALKEY_URL`, `/datalake` (rw) y `/datalake_grande` disponibles.

## Decisiones/aclaraciones añadidas por gateway/registro/sesión
- **Entrada del stream `eventos`**: `XADD eventos * evento <json>`; un único campo `evento` con el JSON de la fila (claves = columnas del esquema; `ts` y `tomada_en` como ISO-8601 UTC con milisegundos, p. ej. `2026-10-31T01:23:34.841Z`; `mes` ya calculado). El gateway los publica tras el 200 de Mongo.
- **Registro (rutas)**: además de las acciones del contrato, `transicion` acepta `renovar` (extiende `asignacion.expira`, sin evento ni cambio de versión; el gateway la invoca como mucho cada LOCK_TTL/4 al guardar borradores). `POST /tareas/asignar` primero devuelve la tarea abierta vigente del anotador (`reanudada:true`, evento null); con `solo_reanudar:true` no toma tareas nuevas.
- **Sesión**: `PUT /borradores/{tarea}` con `pendiente:true` guarda el envío pendiente (`pendiente:{tarea}`, STRING JSON, TTL 1 h) y `POST /locks/{tarea}/liberar` con `solo_pendiente:true` lo borra (así se respeta el máximo de 6 rutas).
- **Salud**: registro NO publica en Valkey; el gateway escribe `salud:registro` y `salud:mongo` (y `salud:gateway`, `salud:valkey`) cada 5 s. `GET /salud` del gateway: 200 operativo/degradado, 503 caído (cuerpo siempre presente). Caído = falla registro/mongo(sin primario)/sesion/valkey.
- **Enviar**: el gateway primero comprueba el lock con el token en Valkey (409 si no vigente) y guarda el pendiente; después Mongo decide. 2.º envío → 409 (lock ya liberado, o estado ≠ abierta).
- `equipo_id` del evento viene de la colección `anotadores` (null si el anotador no existe); `cliente_id` y `sla_horas` están desnormalizados en cada tarea.

## Historial, analitica y dataset (agente de historial/analitica)
- Compose monta el lago con `${DATALAKE_SRC:-./datalake}` (y `DATALAKE_GRANDE_SRC`, `DATALAKE_VALIDACION_SRC`). Docker Desktop NO puede montar carpetas de Google Drive (H:): el contenedor ve un directorio vacio. Aqui se usa un `.env` local (`C:/p2work/...`); en un disco normal se borra. Workers, analitica y herramientas montan tambien `./datalake_validacion:/datalake_validacion:ro` (`DATALAKE_VALIDACION_DIR`). UI del master Spark en `127.0.0.1:18080` (8080 esta bloqueado en esta maquina).
- Historial escribe `eventos/mes=AAAA-MM/part-<primer_id>_<ultimo_id>.parquet`; `mes` NO esta dentro del archivo (solo particion). `ts`/`tomada_en` = timestamp[ms,UTC]. Ruta extra `POST /reprocesar` (prueba P4). Ver `endpoints/historial.md`.
- Analitica: imagen basada en `apache/spark:3.5.3` (Python 3.8, Java 11); NO usa `common/` (requiere 3.10+). Habla con Valkey solo para latidos `salud:analitica` y `salud:spark`. `conjunto=principal|grande|validacion`. Definiciones: Q1 `ts > max(ts) - 90 dias` (max sobre todo el conjunto, dia = fecha UTC); percentiles nearest-rank (`percentile_approx` accuracy 100000); Q3 solo filas `anotada` con `tomada_en` (una duración por toma anotada); Q5 solo `aprobada` con `tomada_en` (última toma, no la primera), incumple si dur > `sla_horas`*3600; Q2/Q4 cuentan eventos de revisión (no tareas distintas: rechazada 2 veces y luego aprobada = 2 rechazadas + 1 aprobada) e incluyen `equipo_id` null como grupo. Ver `endpoints/analitica.md`.
- Dataset: `dataset/{generar_registro,cargar_registro,generar_historial,generar_validacion,comparar_validacion}.py`; procedimiento en `datos/README.md`. `comparar_validacion.py` sale con 0 si Q1-Q5 coinciden con `datalake_validacion/esperado.json`.

## Fase 2: simulador, pruebas y ajustes (integración + experimentos)
- **Simulador** (`simulador/simulador.py`, sólo `httpx`): VUs `anotador` (siguiente -> borrador cada 2 s durante 2-10 s -> enviar; 5 % liberar; 3 % interrupción + `/sesiones/recuperar`), `revisor` (toma ids de tareas enviadas en la misma corrida; iniciar -> aprobar 80 % / rechazar con motivo) y `consumidor` (exportar paginado, /salud, analítica opcional). CLI: `--vus --duracion --calentamiento --proyecto|--proyecto-por-vu --salida x.jsonl --resumen x.json`. `anotadores.json`/`proyectos.json` son ids volcados del dataset cargado (el simulador no toca Mongo). Corre en el host (`localhost:8000`) o dentro de la red docker (`docker cp simulador herramientas:/sim`; `--url http://gateway:8000`).
- **Pruebas** (`pruebas/p1..p8`, `run_all.ps1`, `graficas.py`, `resumen.py`, `indice.py`): se ejecutan en el host con un venv (`C:/p2work/venv`: httpx, numpy, pyarrow, matplotlib); la inyección de fallas usa `docker compose stop/start/restart/kill`; las rutas internas no publicadas se llaman con `docker compose exec` (`pruebas/comun.py::interno`). Evidencia en `evidence/` (ver `evidence/README.md` y `evidence/RESUMEN.md`).
- **Carga (P6)**: la medición principal se hace con el simulador DENTRO de la red docker (contenedor `herramientas`): desde el host, el reenvío de puertos de Docker Desktop limita a ~10 req/s (latencias de segundos con servicios internos <20 ms); esa corrida se conserva (`evidence/p6_host/`).
- **Índice extra de tareas**: `tareas(estado, prioridad desc, asignacion.expira)` (nombre `tareas_estado_prioridad_expira`) sustituye a `tareas(estado, asignacion.expira)`: sirve a las dos ramas del `$or` de asignación con orden por prioridad (SORT_MERGE) cuando no hay filtro de proyecto (de 59 992 claves/105 ms a 2 claves) y a la re-oferta de tareas vencidas.
- **Reconciliación de envíos ambiguos** (hallazgo de P8): un `enviar` con 503 por falta de mayoría (WriteConcernTimeout) puede haberse aplicado en Mongo. `sesion` devuelve `previo` en `PUT /borradores/{id}` con `pendiente:true`; si el reintento recibe 409 de registro y había pendiente previo, el gateway llama a `transicion` con `accion: confirmar_envio` (registro reconstruye el MISMO evento: `event_id = ultimo_evento_id`, `ts = actualizada_en`, sólo si la tarea sigue `anotada` con ese token), lo publica y responde 200 con `reconciliado:true`. Un segundo envío posterior (sin pendiente) sigue dando 409 sin fila.
- **Gateway**: clientes httpx con `keepalive_expiry=2 s` (menor que el keep-alive de uvicorn, 5 s) y reintento del PUT idempotente del envío pendiente.
- **Analítica**: nueva ruta `GET /evento/{event_id}?conjunto=` (3.ª de 6); `OPT_FILTRO_PRIMERO=1` (filtrar antes de `dropDuplicates(event_id)`, equivalente porque los duplicados son copias idénticas); compose pasa `OPT_FILTRO_PRIMERO`. Rendimiento: el montaje 9p de Docker Desktop hace lenta cada apertura de archivo; conviene compactar (`POST /compactar?mes=` del historial; `dataset/compactar_lago.py` para el conjunto grande).
- **Datos de prueba en el lago principal**: las pruebas añaden eventos al conjunto principal (decenas de miles sobre los ~800 000 generados); P5 se midió con ~812 000 y se compacta al terminar.
