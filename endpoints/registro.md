# Registro (interno `http://registro:8001`, MongoDB `plataforma`)

No usa Valkey. Escritura con `w=MONGO_W` (por defecto `majority`, `wTimeoutMS=3000`), lectura del primario, `serverSelectionTimeoutMS=2500`. Cualquier error de Mongo (sin primario, timeout de selección, WTimeout, red) → **503** `{"detail":"mongo_no_disponible"}`. Cada transición válida hace `$inc version` y devuelve `{tarea, evento}`; `evento` es la fila del datalake (esquema en CONTRATOS.md).

Índices creados al inicio: `tareas(proyecto_id,estado,prioridad desc)`, `tareas(asignacion.anotador_id,estado)`, `tareas(batch_id)`, `proyectos(cliente_id,estado)` y el extra `tareas(estado, prioridad desc, asignacion.expira)` (`tareas_estado_prioridad_expira`: asignación sin filtro de proyecto y re-oferta de tareas abiertas con plazo vencido; justificación y explain antes/después en `evidence/p2_concurrencia_20261030T2333.json`).

| Ruta | Cuerpo | Respuestas |
|---|---|---|
| `POST /proyectos` | `{nombre, modalidad, taxonomia[], sla_horas, reglas?, cliente_id?, cliente_nombre?}` | 201 proyecto |
| `POST /proyectos/{id}/lotes` | `{nombre?, prioridad?, tareas:[{archivo, tipo_tarea?, prioridad?}]}` | 201 `{batch_id, tarea_ids}`; 404; 409 |
| `POST /tareas/asignar` | `{anotador_id, proyecto_id?, solo_reanudar?}` | 200 `{tarea, evento, reanudada}`; 204 sin tareas |
| `POST /tareas/{id}/transicion` | `{accion, ...}` ver abajo | 200 `{tarea, evento}`; 409; 404; 422; 503 |
| `GET /proyectos/{id}/exportar?cursor=&limite=` | — | 200 `{items, siguiente_cursor}` |
| `GET /salud` | — | 200 `{estado, mongo:{estado, detalle, total, vivos}}` (siempre 200) |

## Asignación atómica
1. Si el anotador ya tiene una tarea `abierta` con `asignacion.expira` futuro, se devuelve esa (`reanudada:true`, sin evento).
2. Si no, `find_one_and_update` con filtro `estado:disponible` **o** (`estado:abierta` y `asignacion.expira < ahora`), opcionalmente `proyecto_id`, `sort prioridad desc`; fija `estado:abierta`, `asignacion{anotador_id, equipo_id, token, tomada_en, expira}`, `$inc version`, `ultimo_evento_id`. Evento `abierta`.

## Transiciones (`accion`)
| accion | requiere | desde → hasta | evento |
|---|---|---|---|
| `enviar` | anotador_id, token, anotacion | abierta → anotada (token y anotador deben coincidir) | anotada |
| `liberar` | anotador_id, token | abierta → disponible (asignación limpiada) | disponible |
| `iniciar_revision` | revisor_id | anotada → en_revision | en_revision |
| `aprobar` | revisor_id | en_revision → aprobada | aprobada |
| `rechazar` | revisor_id, motivo_codigo | en_revision → **disponible** | **rechazada** (con `motivo`) |
| `renovar` | anotador_id, token | extiende `asignacion.expira` (sin cambio de versión ni evento) | — |
| `confirmar_envio` | anotador_id, token | reconciliación (P8): si la tarea sigue `anotada` con ese token devuelve `{tarea, evento, reconciliado:true}` reconstruyendo el MISMO evento (`event_id = ultimo_evento_id`); no modifica nada | 409 si ya no está `anotada`/token distinto |

Estado o token que no coinciden → 409 `{error:"conflicto", estado_actual}`; tarea inexistente → 404.
