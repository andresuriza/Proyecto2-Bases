# Gateway (`http://localhost:8000`)

Único componente publicado al host. Propaga `X-Request-ID` (se genera si falta y se devuelve en la respuesta) y `X-Anotador-ID` a los servicios internos. Errores internos de Mongo/Valkey/servicios se traducen a **503**. Los eventos se publican en el stream `eventos` de Valkey **después** de que Mongo confirma.

Reintentos: sólo llamadas idempotentes (GET, PUT borrador, salud; 1-2 reintentos con backoff). `enviar`, `liberar`, `revision`, `siguiente` **nunca** se reintentan.

| Método y ruta | Cuerpo | Respuestas |
|---|---|---|
| `POST /proyectos` | `{nombre, modalidad(texto\|imagen\|video\|lidar), taxonomia:[{etiqueta, instruccion}], sla_horas, reglas?, cliente_id?, cliente_nombre?}` | 201 proyecto; 422 validación; 503 |
| `POST /proyectos/{id}/lotes` | `{nombre?, prioridad?(1-5), tareas:[{archivo:{uri, sha256(64 hex), bytes, mime, meta}, tipo_tarea?, prioridad?}]}` (≤5000) | 201 `{batch_id, n_tareas, tarea_ids}`; 404; 409 proyecto cerrado; 422; 503 |
| `POST /tareas/siguiente` | `{anotador_id, proyecto_id?}` | 200 `{tarea, token, borrador, reanudada, lock_ttl_ms, expira}`; **204** sin tareas; 409 lock ocupado (se compensa liberando); 503 |
| `PUT /tareas/{id}/borrador` | `{anotador_id, token, contenido:{...}, version_local?}` | 200 `{ok, ts, lock_ttl_ms}` (renueva lock); 409 lock vencido/token inválido; 503 |
| `POST /tareas/{id}/enviar` | `{anotador_id, token, anotacion:{...}}` | **200** `{tarea_id, estado:"anotada", version, evento_id}` (`reconciliado:true` si un envío anterior con 503 ya estaba aplicado en Mongo y se republicó su evento); **409** token/lock inválido o tarea ya no abierta; **503** Mongo no responde (sin reintento; borrador y envío pendiente quedan en Valkey); 404 |
| `POST /tareas/{id}/liberar` | `{anotador_id, token}` | 200 `{estado:"disponible"}`; 409; 404; 503 |
| `POST /tareas/{id}/revision` | `{revisor_id, accion: iniciar\|aprobar\|rechazar, motivo_codigo?}` | 200 `{estado, version, evento}`; **422** si rechazar sin `motivo_codigo`; 409 transición inválida; 404; 503 |
| `POST /sesiones/recuperar` | `{anotador_id}` | 200 `{tarea, token, borrador:{contenido, ts, edad_ms}, pendiente, lock}`; 404 sin sesión recuperable; 503 |
| `GET /analitica/{q1..q5}?conjunto=principal\|grande&explain=0\|1` | — | 200 resultado de Spark (sólo Parquet); 503 analítica caída |
| `GET /proyectos/{id}/exportar?cursor=&limite=` | — | 200 `{items, siguiente_cursor, n}` (tareas `aprobada`, paginación por `_id`) |
| `GET /salud` | — | 200 `operativo\|degradado`, **503** `caido`; el cuerpo siempre lleva `capas` |

Extra (lectura): `GET /proyectos/{id}/editando` lista los anotadores con sesión activa (no está en la tabla del §7; es una consulta del servicio de sesión).

## /salud

Capas: gateway, registro, mongo, sesion, valkey, historial, analitica, spark. Cada capa combina la sonda directa (timeout ~1 s, 1 reintento) con el latido `salud:{capa}` de Valkey (cada 5 s, TTL 15 s; el gateway publica por registro/mongo/valkey, porque registro no usa Valkey). Reglas: **caído** si falla alguna capa de núcleo (registro, mongo sin primario, sesión, Valkey) → HTTP 503; **degradado** si falla otra (historial, analítica, spark, un nodo de Mongo o hay eventos en el buffer local del gateway) → HTTP 200; si no, **operativo**. Nunca devuelve 500.

## Ejemplo LiDAR

```bash
curl -s -X POST localhost:8000/proyectos -H 'content-type: application/json' -d '{
 "nombre":"LiDAR vehiculos autonomos","modalidad":"lidar","sla_horas":48,"cliente_nombre":"AutoCo",
 "taxonomia":[
  {"etiqueta":"lidar#peaton","instruccion":"Persona a pie."},
  {"etiqueta":"lidar#vehiculo","instruccion":"Auto, bus o camion."},
  {"etiqueta":"lidar#ciclista","instruccion":"Un skater se clasifica como ciclista, no como peaton."},
  {"etiqueta":"lidar#signal","instruccion":"Senales de transito."}]}'

curl -s -X POST localhost:8000/tareas/siguiente -H 'content-type: application/json' \
  -d '{"anotador_id":"a-001","proyecto_id":"<id>"}'
# -> {"tarea":{"_id":"...","archivo":{"uri":"s3://lidar/0001.pcd",...}},"token":"9f2c...","borrador":null,...}

curl -s -X POST localhost:8000/tareas/<tarea>/enviar -H 'content-type: application/json' -d '{
 "anotador_id":"a-001","token":"9f2c...",
 "anotacion":{"cuboides":[{
   "etiqueta":"lidar#ciclista","centro":[12.1,-3.4,0.9],"dimensiones":[1.8,0.6,1.7],
   "yaw":1.57,"tipo_geometria":"cuboide_3d","puntos_dentro":214}]}}'
# -> 200 {"tarea_id":"...","estado":"anotada","version":2,"evento_id":"..."}
# segundo envio -> 409
```
