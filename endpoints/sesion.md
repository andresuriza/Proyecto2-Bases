# Sesión (interno `http://sesion:8002`, Valkey)

No usa MongoDB ni decide el estado de las tareas. Llaves: `lock:tarea:{id}` (token, `SET NX PX`), `sesion:{anotador}` (hash), `borrador:{tarea}` (hash), `editando:{proyecto}` (ZSET), `pendiente:{tarea}` (envío pendiente, TTL 1 h). Renovación y liberación de lock con Lua compare-and-pexpire / compare-and-del. Valkey caído → 503.

| Ruta | Cuerpo | Respuestas |
|---|---|---|
| `POST /sesiones/abrir` | `{anotador_id, tarea_id, proyecto_id, token, etapa?}` | 200 `{lock_ttl_ms, borrador}`; 409 lock de otro token (mismo token = idempotente) |
| `PUT /borradores/{tarea}` | `{anotador_id, token, contenido, version_local?, pendiente?}` | 200 (renueva el lock); 409 token/lock inválido. Con `pendiente:true` guarda el envío pendiente en `pendiente:{tarea}` sin tocar el borrador y responde `previo:true|false` (había un envío anterior sin confirmar) |
| `POST /sesiones/recuperar` | `{anotador_id}` | 200 `{sesion, lock:{valido, ttl_ms}, borrador:{contenido, ts, edad_ms}, pendiente}` (`sesion:null` si no hay) |
| `POST /locks/{tarea}/liberar` | `{token, anotador_id?, proyecto_id?, solo_pendiente?}` | 200; 409 si el lock pertenece a otro token. Borra lock, borrador, pendiente, sesión y `editando`; con `solo_pendiente` sólo el pendiente |
| `GET /proyectos/{id}/editando` | — | 200 `{n, editando:[{anotador_id, ultimo_latido}]}` (descarta latidos > LOCK_TTL) |
| `GET /salud` | — | 200 `{estado, detalle}` |
