# Datos de prueba: procedimiento de generación y carga

Todo es determinista (semilla 42 por defecto). Los generadores necesitan `python>=3.10`, `numpy`, `pyarrow` (y `pymongo` el cargador); la imagen `herramientas` (perfil `tools`) los trae.

## Volúmenes (resultado con `--seed 42`)
| Entidad | Cantidad |
|---|---|
| clientes | 60 |
| proyectos | 180 (120 activos en Mongo + 60 cerrados sólo en el historial) |
| batches | 600 (5 por proyecto activo, 100 tareas c/u) |
| tareas del registro | 60 000: texto 18 000, imagen 18 000, video 12 000, lidar 12 000 (todas `disponible`) |
| anotadores | 300 en 30 equipos; 30 revisores (10 %) |
| historial principal | 800 000 eventos, 100 840 tareas, 180 proyectos, 12 meses (2025-11 a 2026-10), ~10 archivos por mes |
| validación | 56 eventos únicos + 4 duplicados exactos (`datalake_validacion/esperado.json`) |
| eventos_grande | `--eventos 10000000` (opcional; unos 7 min de generación) |

## Comandos desde cero
```bash
docker compose up -d --build                       # núcleo (registro crea los índices al arrancar)
docker compose --profile tools up -d herramientas
H="docker compose --profile tools exec herramientas"
# 1) registro (Mongo)
$H python dataset/generar_registro.py --seed 42 --out dataset/out/
$H python dataset/cargar_registro.py --dir dataset/out --limpiar   # w=majority, idempotente
# 2) historial principal (datalake) y validación
$H python dataset/generar_historial.py --seed 42 --eventos 800000 --out datalake/eventos
$H python dataset/generar_validacion.py --out datalake_validacion
# 3) conjunto grande (opcional)
$H python dataset/generar_historial.py --seed 7 --eventos 10000000 --out datalake_grande/eventos
# 4) comprobar Q1-Q5 contra lo esperado (sale con 0 si coinciden)
$H python dataset/comparar_validacion.py --url http://analitica:8004 --esperado datalake_validacion/esperado.json
```
Las pruebas de carga consumen tareas `disponible`; para reponerlas repita el paso 1 (`--limpiar`) y anote fecha, motivo y efecto sobre la comparabilidad.

## Métodos de generación
**Registro** (`generar_registro.py`): UUID v4 derivados de `random.Random(seed)`. Modalidades por proyecto 30/30/20/20 (imagen alterna bbox/polígono por tarea; texto=span, video=bbox_video, lidar=cuboide_3d). `archivo`: URI `s3://datos-anotacion/...`, `sha256` de `seed|uri`, `bytes` y `mime` por modalidad (`text/plain`, `image/jpeg`, `video/mp4`, `application/vnd.pcd`) y `meta` con mínimos/máximos y dimensiones. Prioridad 1..5 (10/20/40/20/10 %). `cliente_id` y `sla_horas` desnormalizados. Un proyecto LiDAR (`ejemplo_lidar: true`, SLA 48 h) con taxonomía `lidar#peaton/vehiculo/ciclista/signal` e instrucción "skater = ciclista".

**Historial** (`generar_historial.py`): ciclo abierta, anotada, en_revision, aprobada o rechazada (el rechazo vuelve a abierta con nueva toma, máx. 5 ciclos) y liberaciones (38 % de las tomas). Creación de tareas uniforme por día (fin de semana x0.35), hora N(14 h, 3.5 h), dentro de la vida de cada proyecto (los cerrados terminan antes). Duraciones lognormales: anotación con mediana 4/8/25/55 min (texto/imagen/video/lidar); espera a revisión con mediana 2.5/3/4/8 h (sigma 1.2), x3 en ~10 % de proyectos lentos y x2.5 en el proyecto LiDAR de ejemplo; revisión mediana 8 min; probabilidad de rechazo por equipo ~Beta(3.5, 6.5) escalada por modalidad. Los eventos posteriores al 2026-10-30 se descartan (tareas en curso). Escritura por chunks de 10 000 tareas, un archivo `part-gen-s<seed>-<chunk>.parquet` por mes (partición `mes=`) con el mismo esquema que el consumidor. Los ids de clientes, proyectos, equipos y anotadores son los del registro; las tareas del historial son tareas ya procesadas (distintas de las 60 000 `disponible`).

**Validación** (`generar_validacion.py`): 14 tareas escritas a mano en el código (aprobadas, rechazos con re-toma, liberación, tarea en curso, un incumplimiento de SLA de 48 h y otro de 24 h, eventos dentro y fuera de la ventana de 90 días de Q1, tipo `poligono`) y un segundo archivo con eventos duplicados. `esperado.json` se calcula con Python puro (sin Spark) según CONTRATOS.

## Docker sobre Google Drive
Si el repositorio está en una carpeta de Google Drive (p. ej. `H:`), Docker Desktop monta directorios vacíos. Solución usada: `.env` (no versionado) con `DATALAKE_SRC`, `DATALAKE_GRANDE_SRC` y `DATALAKE_VALIDACION_SRC` apuntando a una carpeta de un disco local (`C:/p2work/...`); `docker-compose.yml` usa `${DATALAKE_SRC:-./datalake}`. En un disco normal no hace falta `.env`. Con esa configuración, los generadores y `dataset/` deben copiarse/ejecutarse sobre esa carpeta local.

## Notas de la fase de experimentos
- `eventos_grande` (10 000 000 eventos) se generó con `generar_historial.py --seed 7 --eventos 10000000` en 232 s (1602 archivos pequeños, 550 MiB tras compactar). Para consultarlo con Spark se compactó con `python dataset/compactar_lago.py --lago <lago_grande>` (1602 -> 12 archivos en 67 s).
- El lago principal (800 000 eventos generados) recibe además los eventos de las pruebas; las mediciones de P5 se hicieron con ~812 000 eventos tras compactar con `POST /compactar` del historial (272 -> 12 archivos).
- Las pruebas de carga no agotaron las 60 000 tareas `disponible` (ver `evidence/p6*/` y el JSON de P6), por lo que no hubo que volver a cargar el registro.
