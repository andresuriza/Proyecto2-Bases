# Historial (interno `http://historial:8003`)

Consumidor del stream `eventos` de Valkey (grupo `historial`, consumidor `historial-1`) que escribe el datalake Parquet. No publica puerto al host. De Valkey lee sólo el stream y publica el latido `salud:historial` (EX 15, cada 5 s). Escribe en `/datalake` (rw).

## Micro-lotes
`XREADGROUP` hasta **N=500** eventos o **T=2 s** (`LOTE_N`, `LOTE_T_S`). Cada lote: validar (esquema de CONTRATOS) → copia cruda en `/datalake/_replay/<batch_id>.jsonl` → un archivo por mes en `/datalake/_tmp/` → `fsync` → `os.replace` a `eventos/mes=AAAA-MM/part-<batch_id>.parquet` → `XACK`. `batch_id = <primer id del stream>_<último id>` (determinista: reprocesar el mismo lote sobrescribe el mismo archivo). La columna `mes` es sólo la partición (no se guarda dentro del archivo). `ts`/`tomada_en`: `timestamp[ms, UTC]`.

Recuperación: al arrancar se leen los pendientes propios (`XREADGROUP ... 0`) y se reclaman (`XAUTOCLAIM`, 30 s de inactividad) los de otros consumidores. Los eventos inválidos se descartan (contador `invalidos`) para no bloquear el consumo. Garantía at-least-once; Spark deduplica por `event_id`. Si tras una caída el micro-lote se rearma con otros límites, el nombre de archivo difiere y las filas quedan duplicadas: es inocuo por la deduplicación.

| Ruta | Descripción |
|---|---|
| `GET /salud` | 200 `{estado, detalle}`; `degradado` si hubo error en los últimos 15 s |
| `GET /estado` | `xlen`, `lag`, `pendientes`, lotes y eventos escritos, `ultimos_lotes` (batch_id, eventos, archivos, ms), archivos por mes |
| `POST /compactar?mes=AAAA-MM` | fusiona los `part-*.parquet` del mes en `part-compact-<hash>.parquet`, deduplicado por event_id |
| `POST /reprocesar?batch_id=&duplicar=` | gancho de prueba P4: re-publica un lote desde `_replay/` (o `XRANGE`). `duplicar=false`: mismo nombre (sobrescritura idempotente); `duplicar=true`: archivo extra `-dup<ms>` (filas duplicadas en el lago para probar la deduplicación de Spark). Sin `batch_id` usa el último lote |

## Compactación y lectores concurrentes
Orden: (1) listar orígenes; (2) escribir el compactado de forma atómica (tmp + rename); (3) borrar los orígenes. Entre 2 y 3 hay filas duplicadas pero nunca faltantes, y Spark deduplica. Un lector que listó archivos antes del borrado podría ver `FileNotFound`; analítica reintenta una vez. Los archivos que lleguen durante la compactación no se tocan (sólo se borran los listados en 1).
