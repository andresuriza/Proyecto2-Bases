# Proyecto 2 — Plataforma distribuida de anotación de datos

TI-4601 Bases de Datos Avanzadas (TEC). Gateway REST como única entrada; registro (MongoDB, 3 nodos, `w:majority`), sesión (Valkey con AOF `everysec`), historial (micro-lotes Parquet) y analítica (Apache Spark sólo lectura). Todo corre en contenedores.

- Informe (PDF con apéndices): `informe/informe.pdf` (fuente `informe/informe.tex`; las tablas se generan con `python pruebas/informe_tablas.py` a partir de `evidence/`).
- Contratos internos: `CONTRATOS.md`. Endpoints del gateway y de los servicios internos: `endpoints/`. Datos de prueba: `datos/README.md`. Evidencia: `evidence/` (`evidence/README.md` es el índice y `evidence/RESUMEN.md` reúne las cifras clave con su archivo).

## Estructura del repositorio

| Ruta | Contenido |
|---|---|
| `docker-compose.yml` | servicios y perfiles (`spark2`, `tools`) |
| `endpoints/` | endpoints del gateway y de registro, sesión, historial y analítica |
| `servicios/` | `gateway`, `registro`, `sesion`, `historial`, `analitica`, `common`, `herramientas` (imagen de utilidades) |
| `dataset/` | generadores y cargadores (registro en Mongo, historial en Parquet, validación) y compactador del lago |
| `simulador/` | simulador de usuarios (sólo HTTP contra el gateway) |
| `pruebas/` | pruebas P1–P8, `run_all.ps1`, gráficas, resumen e índice de evidencia, generador de tablas del informe |
| `evidence/` | resultados originales de las pruebas |
| `datos/` | documentación de la generación y carga de datos |
| `informe/` | informe técnico (LaTeX y PDF) |

## 1. Requisitos previos

- Docker Desktop (o Docker Engine) con Compose v2. Medido con Docker 29.9.2 / Compose v5.6.1, VM con 12 vCPU y 15.5 GiB (`evidence/entorno.txt`); no se probó con menos recursos.
- Para las pruebas y las gráficas: Python 3.10+ en el host (se usó 3.14.4) con `httpx numpy pyarrow matplotlib`. `pruebas/run_all.ps1` crea el venv si no existe. Las pruebas están escritas para PowerShell/Windows (`run_all.ps1`); los scripts `pruebas/p*.py` son Python y llaman a `docker`/`docker compose`.
- Puertos del host: sólo se publica **8000** (gateway). Además, la UI del driver Spark en `127.0.0.1:4040` y la del master en `127.0.0.1:18080` (sólo localhost).

### Nota importante: repositorio en Google Drive y archivo `.env`
Si el repositorio está en una carpeta de Google Drive (p. ej. `H:\`), Docker Desktop **no puede montarla**: el contenedor ve un directorio vacío. El lago Parquet es un bind mount, así que hay que usar una carpeta de un disco local mediante un archivo `.env` junto a `docker-compose.yml` (no se versiona):

```
DATALAKE_SRC=C:/p2work/datalake
DATALAKE_GRANDE_SRC=C:/p2work/datalake_grande
DATALAKE_VALIDACION_SRC=C:/p2work/datalake_validacion
```

Cree las carpetas antes de arrancar. En un disco normal **no hace falta** `.env`: se usan `./datalake`, `./datalake_grande` y `./datalake_validacion`. Las pruebas leen el lago local con las variables de entorno `LAGO_LOCAL` y `LAGO_GRANDE_LOCAL` (por defecto `C:/p2work/datalake` y `C:/p2work/datalake_grande`; ajústelas si usa otra ruta). Los volúmenes de Mongo y Valkey son volúmenes con nombre de Docker y no se ven afectados. El contenedor `herramientas` monta `.:/src`, que en Drive aparece vacío: allí copie los archivos con `docker cp` (ver abajo).

## 2. Arranque y parada

```bash
docker compose up -d --build        # núcleo: mongo x3 + mongo-init, valkey, registro, sesion, gateway, historial, analitica, spark-master, spark-worker-1
docker compose --profile spark2 up -d spark-worker-2     # 2.º worker de Spark (comparación 1 vs 2 workers)
docker compose --profile tools up -d herramientas        # utilidades: simulador, cargadores del dataset
curl localhost:8000/salud                                 # operativo / degradado / caído, con el estado de cada capa
docker compose stop                 # detener sin borrar
docker compose down                 # baja los contenedores; conserva los volúmenes (mongo, valkey)
docker compose down -v              # reinicio total (borra Mongo y Valkey; el lago Parquet está en el disco del host)
```

Núcleo mínimo (sin historial ni analítica): `docker compose up -d --build mongo1 mongo2 mongo3 mongo-init valkey registro sesion gateway`.

Variables (se pasan al invocar compose): `MONGO_W` (`majority` por defecto; `1` sólo para comparar), `LOCK_TTL_MS` (120000; bájelo para probar vencimiento, p. ej. `LOCK_TTL_MS=5000 docker compose up -d registro sesion gateway`), `OPT_FILTRO_PRIMERO` (1 por defecto; 0 = deduplicar antes de filtrar, sólo para comparar en P5).

## 3. Datos de prueba desde cero

Conteos (semilla 42; detalle y método en `datos/README.md`): 60 clientes, 180 proyectos (120 en el registro, 60 cerrados sólo en el historial), 600 batches, 60 000 tareas (texto 18 000, imagen 18 000, video 12 000, lidar 12 000), 300 anotadores en 30 equipos (30 revisores), historial de ~800 000 eventos en 12 meses (2025-11 a 2026-10), validación de 56 eventos únicos + 4 duplicados y, opcional, `eventos_grande` de 10 000 000 filas.

Con el repositorio en un disco normal (monta `.:/src`):

```bash
docker compose --profile tools up -d herramientas
H="docker compose --profile tools exec herramientas"
$H python dataset/generar_registro.py --seed 42 --out dataset/out/
$H python dataset/cargar_registro.py --dir dataset/out --limpiar        # w=majority; idempotente
$H python dataset/generar_historial.py --seed 42 --eventos 800000 --out datalake/eventos
$H python dataset/generar_validacion.py --out datalake_validacion
$H python dataset/generar_historial.py --seed 7 --eventos 10000000 --out datalake_grande/eventos   # opcional (~4-7 min)
$H python dataset/compactar_lago.py --lago /datalake_grande                                         # el conjunto grande se compacta antes de consultarlo
$H python dataset/comparar_validacion.py --url http://analitica:8004 --esperado datalake_validacion/esperado.json   # sale con 0 si Q1-Q5 coinciden
```

Con el repositorio en Google Drive (`.:/src` vacío): copie `dataset/` al contenedor y escriba en las rutas montadas del lago (`/datalake`, `/datalake_grande`, `/datalake_validacion`):

```bash
docker cp dataset herramientas:/work_dataset
H="docker compose --profile tools exec herramientas"
$H python /work_dataset/generar_registro.py --seed 42 --out /tmp/out/
$H python /work_dataset/cargar_registro.py --dir /tmp/out --limpiar
$H python /work_dataset/generar_historial.py --seed 42 --eventos 800000 --out /datalake/eventos
$H python /work_dataset/generar_validacion.py --out /datalake_validacion
```

(Estas variantes usan los mismos scripts y banderas; el procedimiento exacto con el que se generó la evidencia está en `datos/README.md`.) **Reiniciar Mongo:** `cargar_registro.py --limpiar` vacía las colecciones y vuelve a cargar las 60 000 tareas `disponible`. Las pruebas de carga consumen tareas `disponible`; si se agotan, repita la carga y anote fecha, motivo y efecto sobre la comparabilidad (`datos/README.md`).

## 4. Ejecutar consultas y la API

```bash
curl "localhost:8000/analitica/q1?conjunto=principal"          # Q1..Q5; conjunto=principal|grande; explain=1 devuelve el plan
curl "localhost:8000/analitica/q3?conjunto=grande&explain=1"
```

Ejemplo completo de anotación LiDAR (crear proyecto, tomar tarea, enviar cuboide, segundo envío 409) en `endpoints/gateway.md`. Compactar el lago (recomendado tras muchos micro-lotes): `docker compose exec historial python -c "import urllib.request as u;print(u.urlopen(u.Request('http://localhost:8003/compactar?mes=2026-10',method='POST')).read())"` (o `POST /compactar?mes=AAAA-MM` del historial desde `herramientas`); para el conjunto grande, `dataset/compactar_lago.py`.

## 5. Pruebas (P1–P8) y evidencia

Con el stack levantado y **sin tráfico ajeno**: las pruebas detienen, matan y reinician contenedores (cada una restaura el estado normal al terminar).

```powershell
.\pruebas\run_all.ps1                       # entorno, P1, P2, P3, P5, P4, P6, P8, gráficas, resumen e índice
.\pruebas\run_all.ps1 -SaltarGrande         # omite el conjunto de 10 M en P5
.\pruebas\run_all.ps1 -Venv C:\p2work\venv  # venv alternativo (por defecto C:\p2work\venv)
```

Individualmente (desde la raíz; usan el venv del host):

| Prueba | Comando | Qué hace |
|---|---|---|
| entorno | `python pruebas/entorno.py` | escribe `evidence/entorno.txt` |
| P1 API y salud | `python pruebas/p1_api.py` | rutas y códigos, `/salud` normal/degradado/caído, puertos, aislamiento (~4 min) |
| P2 concurrencia | `python pruebas/p2_concurrencia.py [--rondas 3 --ops 600 --hilos 8 --rondas-bench 3]` | 50 solicitudes con 10 tareas, 409 en el segundo envío, `w:majority` vs `w:1`, `explain` |
| P3 sesión | `python pruebas/p3_sesion.py [--trials 5 --ttl-ms 10000]` | borrador recuperable, reinicio de Valkey, lock vencido |
| P5 analítica | `python pruebas/p5_analitica.py [--saltar-grande ...]` | validación, frío/caliente, 1 vs 2 workers, planes, conjunto grande |
| P4 historial | `python pruebas/p4_historial.py [--trials 10]` | visibilidad HTTP 200 → Spark, reproceso, dedup, compactación |
| P6 carga | `python pruebas/p6_carga.py --modo red` (o `--modo host`) | 10/40/100 VUs, 30 s de calentamiento + 3 corridas de 90 s; `--niveles`, `--duracion`, `--calentamiento`, `--corridas` |
| P8 fallas | `python pruebas/p8_fallas.py [--hilos 6]` | secundario, primario (stop y kill), sin mayoría, Valkey; RTO y RPO |
| gráficas / resumen / índice | `python pruebas/graficas.py`, `resumen.py`, `indice.py` | `evidence/graficas/`, `evidence/RESUMEN.md`, `evidence/README.md` |

P2 y P8 modifican Mongo y reinician nodos; P5 reinicia `analitica` y arranca/detiene `spark-worker-2`. Una ejecución completa toma del orden de una hora.

**Simulador solo (HTTP contra el gateway):**
```powershell
python simulador/simulador.py --vus 10 --duracion 60 --proyecto-por-vu --salida sim.jsonl --resumen sim.json
# dentro de la red docker (recomendado para carga):
docker cp simulador herramientas:/sim
docker exec herramientas python /sim/simulador.py --url http://gateway:8000 --vus 40 --duracion 90 --calentamiento 30 --proyecto-por-vu
```
`simulador/proyectos.json` y `simulador/anotadores.json` son los ids del dataset cargado; si regenera el dataset con otra semilla, actualícelos.

**Dónde queda la evidencia:** cada prueba escribe un par `evidence/<prueba>_<AAAAMMDDThhmm>.json/.md` (fecha y hora, objetivo, configuración, comando, resultado, interpretación y archivos de respaldo); P5/P6/P8 dejan además `evidence/p5/`, `p6/`, `p6_host/`, `p6_red_previo/`, `p8/` y `graficas/`. Índice: `evidence/README.md`.

**Informe:** `python pruebas/informe_tablas.py` regenera `informe/tablas/*.tex` desde `evidence/`; compile con `latexmk -pdf informe.tex` dentro de `informe/` (o `docker run --rm -v <copia local>:/doc -w /doc texlive/texlive:latest-medium latexmk -pdf informe.tex`; el paquete `xurl` puede requerir `tlmgr install xurl`; copie la carpeta a un disco local si el repositorio está en Drive).

## 6. Solución de problemas

- **Contenedores que ven carpetas vacías / Spark no encuentra datos:** el repositorio está en Google Drive. Use `.env` con `DATALAKE_*` en un disco local (sección 1) y ejecute `docker compose up -d` de nuevo.
- **Puerto 8080 bloqueado en el host:** la UI del master de Spark se publica en `127.0.0.1:18080` (no en 8080). La UI del driver está en `127.0.0.1:4040`.
- **Spark lento (decenas de segundos):** hay demasiados archivos pequeños en el lago (cada micro-lote crea uno) y el montaje 9p de Docker Desktop hace lenta cada apertura. Compacte con `POST /compactar?mes=AAAA-MM` (historial) o `dataset/compactar_lago.py`. Ver Sección 5 del informe (272 archivos → 12).
- **El simulador desde Windows se satura (~10 req/s, latencias de segundos):** limitación del reenvío de puertos de Docker Desktop hacia `localhost:8000`, no de la plataforma (`evidence/p6_host/`). Para carga, ejecute el simulador dentro de la red (`docker cp simulador herramientas:/sim` y `--url http://gateway:8000`) o use `python pruebas/p6_carga.py --modo red`.
- **`/salud` en degradado o caído:** `curl localhost:8000/salud` indica la capa. Caído = registro, Mongo (sin primario), sesión o Valkey; degradado = historial, analítica, Spark o un nodo de Mongo.
- **503 en escrituras:** Mongo sin primario o sin mayoría (`mongo_no_disponible`); espere la elección (~10 s) o reinicie los nodos detenidos (`docker compose start mongo2 mongo3`). Un envío con 503 puede haberse aplicado; reintentarlo lo reconcilia (informe, Sección 7).
- **Se agotaron las tareas `disponible`:** repita `cargar_registro.py --limpiar` (sección 3) y anote la fecha, el motivo y el efecto sobre la comparabilidad.
- **Reiniciar solo Mongo/Valkey:** `docker compose down -v` borra ambos volúmenes; luego `up -d` y recargue el dataset. El lago Parquet no se borra (bórrelo a mano en la carpeta de `DATALAKE_SRC` o regenere con `generar_historial.py`).
- **Tras una prueba interrumpida:** `docker compose up -d` y `docker compose --profile spark2 stop spark-worker-2`; verifique `curl localhost:8000/salud` y `rs.status()` (`docker exec mongo1 mongosh --quiet --eval "rs.status().members.map(m=>[m.name,m.stateStr])"`).
