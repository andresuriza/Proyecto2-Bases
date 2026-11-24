# P2. Registro, réplicas y concurrencia de asignación

- **Fecha y hora:** 2026-10-30T21:21:52 (duración 306.4 s)
- **Comando ejecutado:** `python pruebas/p2_concurrencia.py`
- **Máquina:** Windows-11-10.0.26200-SP0; CPU lógicos=12; Docker 29.9.2 (12 vCPU, 15.5 GiB); detalle en `evidence/entorno.txt`
- **Archivos que respaldan:** `evidence/p2_concurrencia_20261030T2121.json`, `evidence/p2_concurrencia_20261030T2121.md`

## Objetivo

Estado del replica set; 50 solicitudes simultáneas con 10 tareas libres (10 asignaciones y 40 respuestas 204, ninguna tarea con dos dueños); segundo envío -> 409 sin fila extra en el datalake; w:majority vs w:1; explain de los índices.

## Configuración

```json
{
 "rondas_concurrentes": 3,
 "solicitudes_por_ronda": 50,
 "tareas_libres": 10,
 "ops_bench": 600,
 "hilos_bench": 8,
 "rondas_bench_por_modo": 2,
 "mecanismo": "ProcessPoolExecutor (50 procesos) con barrera de reloj; httpx contra el gateway"
}
```

## Resultado observado

```json
{
 "rondas_concurrentes": [
  {
   "ronda": 1,
   "codigos": {
    "200": 10,
    "204": 40
   },
   "cumple": true,
   "dispersion_inicio_ms": 5791.1
  },
  {
   "ronda": 2,
   "codigos": {
    "204": 40,
    "200": 10
   },
   "cumple": true,
   "dispersion_inicio_ms": 3084.3
  },
  {
   "ronda": 3,
   "codigos": {
    "200": 10,
    "204": 40
   },
   "cumple": true,
   "dispersion_inicio_ms": 5956.2
  }
 ],
 "todas_las_rondas_cumplen": true,
 "segundo_enviar": 409,
 "fila_extra_en_lago": false,
 "w_majority_vs_w1": {
  "majority": {
   "enviar": {
    "p50_ms": 171.2,
    "p95_ms": 294.89,
    "p99_ms": 378.38,
    "n_total": 1200
   },
   "siguiente": {
    "p50_ms": 171.4,
    "p95_ms": 285.42,
    "p99_ms": 344.07,
    "n_total": 1200
   },
   "ciclos_por_s_media": 20.5
  },
  "w1": {
   "enviar": {
    "p50_ms": 163.13,
    "p95_ms": 308.2,
    "p99_ms": 414.77,
    "n_total": 1200
   },
   "siguiente": {
    "p50_ms": 164.19,
    "p95_ms": 294.56,
    "p99_ms": 397.1,
    "n_total": 1200
   },
   "ciclos_por_s_media": 21.4
  }
 },
 "MONGO_W_restaurado": "majority",
 "replica_set": {
  "mongo1:27017": "PRIMARY",
  "mongo2:27017": "SECONDARY",
  "mongo3:27017": "SECONDARY"
 }
}
```

### rs.status() resumen  _(21:21:55)_

```json
{
 "set": "rs0",
 "miembros": [
  {
   "nombre": "mongo1:27017",
   "estado": "PRIMARY",
   "salud": 1,
   "optime": "2026-10-31T03:21:45.000Z",
   "uptime_s": 7202
  },
  {
   "nombre": "mongo2:27017",
   "estado": "SECONDARY",
   "salud": 1,
   "optime": "2026-10-31T03:21:45.000Z",
   "pingMs": {
    "high": 0,
    "low": 0,
    "unsigned": false
   },
   "uptime_s": 118
  },
  {
   "nombre": "mongo3:27017",
   "estado": "SECONDARY",
   "salud": 1,
   "optime": "2026-10-31T03:21:45.000Z",
   "pingMs": {
    "high": 0,
    "low": 0,
    "unsigned": false
   },
   "uptime_s": 118
  }
 ],
 "prioridades": [
  {
   "host": "mongo1:27017",
   "prioridad": 2,
   "votos": 1
  },
  {
   "host": "mongo2:27017",
   "prioridad": 1,
   "votos": 1
  },
  {
   "host": "mongo3:27017",
   "prioridad": 1,
   "votos": 1
  }
 ],
 "writeConcernMajorityJournalDefault": true,
 "defaultRW": {
  "w": "majority",
  "wtimeout": 0
 }
}
```

### Ronda concurrente 1  _(21:22:35)_

```json
{
 "ronda": 1,
 "proyecto_id": "f992313c-222b-4dd3-8fa3-bc06c84caad3",
 "codigos": {
  "200": 10,
  "204": 40
 },
 "tareas_distintas_en_respuestas_200": 10,
 "dispersion_inicio_ms": 5791.1,
 "latencia_ms": {
  "n": 50,
  "p50_ms": 110.07,
  "p95_ms": 402.24,
  "p99_ms": 446.76,
  "media_ms": 145.07,
  "max_ms": 446.76
 },
 "mongo": {
  "por_estado": [
   {
    "_id": "abierta",
    "n": 10
   }
  ],
  "anotadores_con_mas_de_una_tarea": 0,
  "tareas_abiertas": 10,
  "anotadores_distintos_en_abiertas": 10
 },
 "locks_valkey_con_token_del_dueno": 10,
 "cumple": true
}
```

### Ronda concurrente 2  _(21:23:09)_

```json
{
 "ronda": 2,
 "proyecto_id": "dfa53cc0-6e02-4786-a030-bfd31d233e27",
 "codigos": {
  "204": 40,
  "200": 10
 },
 "tareas_distintas_en_respuestas_200": 10,
 "dispersion_inicio_ms": 3084.3,
 "latencia_ms": {
  "n": 50,
  "p50_ms": 218.82,
  "p95_ms": 506.97,
  "p99_ms": 670.85,
  "media_ms": 258.61,
  "max_ms": 670.85
 },
 "mongo": {
  "por_estado": [
   {
    "_id": "abierta",
    "n": 10
   }
  ],
  "anotadores_con_mas_de_una_tarea": 0,
  "tareas_abiertas": 10,
  "anotadores_distintos_en_abiertas": 10
 },
 "locks_valkey_con_token_del_dueno": 10,
 "cumple": true
}
```

### Ronda concurrente 3  _(21:23:54)_

```json
{
 "ronda": 3,
 "proyecto_id": "295311de-827f-4000-81ba-43b34c928a23",
 "codigos": {
  "200": 10,
  "204": 40
 },
 "tareas_distintas_en_respuestas_200": 10,
 "dispersion_inicio_ms": 5956.2,
 "latencia_ms": {
  "n": 50,
  "p50_ms": 111.83,
  "p95_ms": 299.48,
  "p99_ms": 431.12,
  "media_ms": 137.39,
  "max_ms": 431.12
 },
 "mongo": {
  "por_estado": [
   {
    "_id": "abierta",
    "n": 10
   }
  ],
  "anotadores_con_mas_de_una_tarea": 0,
  "tareas_abiertas": 10,
  "anotadores_distintos_en_abiertas": 10
 },
 "locks_valkey_con_token_del_dueno": 10,
 "cumple": true
}
```

### Segundo envío de una tarea ya enviada  _(21:24:12)_

```json
{
 "tarea_id": "a0a257a5-0bb9-445a-9923-7817c4d2fe96",
 "primer_enviar": 200,
 "segundo_enviar": 409,
 "enviar_otro_anotador_mismo_token": 409,
 "filas_lago_tras_6s": [
  [
   "abierta",
   "bfefa069"
  ],
  [
   "anotada",
   "699f4c96"
  ]
 ],
 "filas_lago_tras_12s": [
  [
   "abierta",
   "bfefa069"
  ],
  [
   "anotada",
   "699f4c96"
  ]
 ],
 "filas_estado_anotada": 1,
 "mongo": "{\n  _id: 'a0a257a5-0bb9-445a-9923-7817c4d2fe96',\n  estado: 'anotada',\n  version: 2,\n  ultimo_evento_id: '699f4c96-2bff-45ea-b59f-ead1d1db8809'\n}",
 "cumple": true
}
```

### Microbenchmark con MONGO_W=majority  _(21:24:47)_

```json
{
 "proyecto_id": "a35dfd43-6f1c-48cf-9349-45cda21f1710",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 25.9,
 "ciclos_por_s": 23.1,
 "siguiente": {
  "n": 600,
  "p50_ms": 142.67,
  "p95_ms": 209.95,
  "p99_ms": 246.87,
  "media_ms": 148.38,
  "max_ms": 357.57
 },
 "enviar": {
  "n": 600,
  "p50_ms": 143.33,
  "p95_ms": 224.98,
  "p99_ms": 280.38,
  "media_ms": 150.26,
  "max_ms": 348.97
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "majority"
}
```

### Microbenchmark con MONGO_W=1  _(21:25:25)_

```json
{
 "proyecto_id": "c17584e3-3a5b-4b83-8c83-a40f8084d786",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 28.9,
 "ciclos_por_s": 20.7,
 "siguiente": {
  "n": 600,
  "p50_ms": 161.55,
  "p95_ms": 310.34,
  "p99_ms": 432.15,
  "media_ms": 177.15,
  "max_ms": 763.97
 },
 "enviar": {
  "n": 600,
  "p50_ms": 162.82,
  "p95_ms": 317.58,
  "p99_ms": 475.71,
  "media_ms": 178.27,
  "max_ms": 945.54
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "1"
}
```

### Microbenchmark con MONGO_W=majority  _(21:26:10)_

```json
{
 "proyecto_id": "61896374-c67f-4e50-bb45-11fd8d2440e3",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 33.8,
 "ciclos_por_s": 17.8,
 "siguiente": {
  "n": 600,
  "p50_ms": 200.13,
  "p95_ms": 360.9,
  "p99_ms": 441.27,
  "media_ms": 216.41,
  "max_ms": 775.58
 },
 "enviar": {
  "n": 600,
  "p50_ms": 199.07,
  "p95_ms": 364.8,
  "p99_ms": 476.37,
  "media_ms": 216.17,
  "max_ms": 773.93
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "majority"
}
```

### Microbenchmark con MONGO_W=1  _(21:26:48)_

```json
{
 "proyecto_id": "188d475f-4323-457f-a48d-3c8c6ac058a8",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 27.1,
 "ciclos_por_s": 22.2,
 "siguiente": {
  "n": 600,
  "p50_ms": 166.83,
  "p95_ms": 278.79,
  "p99_ms": 362.05,
  "media_ms": 174.73,
  "max_ms": 482.9
 },
 "enviar": {
  "n": 600,
  "p50_ms": 163.45,
  "p95_ms": 298.82,
  "p99_ms": 353.83,
  "media_ms": 172.93,
  "max_ms": 456.7
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "1"
}
```

### Comparación w:majority vs w:1 (media de las rondas)  _(21:26:56)_

Los valores son medias de los percentiles de cada ronda. En un solo host con 3 nodos en contenedores la latencia de replicación es mínima, así que la diferencia NO permite inferir el rendimiento de un clúster distribuido.

```json
{
 "majority": {
  "enviar": {
   "p50_ms": 171.2,
   "p95_ms": 294.89,
   "p99_ms": 378.38,
   "n_total": 1200
  },
  "siguiente": {
   "p50_ms": 171.4,
   "p95_ms": 285.42,
   "p99_ms": 344.07,
   "n_total": 1200
  },
  "ciclos_por_s_media": 20.5
 },
 "w1": {
  "enviar": {
   "p50_ms": 163.13,
   "p95_ms": 308.2,
   "p99_ms": 414.77,
   "n_total": 1200
  },
  "siguiente": {
   "p50_ms": 164.19,
   "p95_ms": 294.56,
   "p99_ms": 397.1,
   "n_total": 1200
  },
  "ciclos_por_s_media": 21.4
 }
}
```

### explain(executionStats) por consulta  _(21:26:58)_

```json
{
 "proyecto_probado": "a5135ea0-fa53-434d-a6d5-901d8b621d41",
 "total_tareas": 62437,
 "explain": {
  "1_asignar_con_proyecto (tareas_proyecto_estado_prioridad + tareas_estado_expira)": {
   "plan": [
    "LIMIT",
    "FETCH",
    "SORT_MERGE",
    "IXSCAN(tareas_proyecto_estado_prioridad)",
    "FETCH",
    "IXSCAN(tareas_proyecto_estado_prioridad)"
   ],
   "nReturned": 1,
   "keysExaminadas": 1,
   "docsExaminados": 1,
   "ms": 3
  },
  "1b_asignar_con_proyecto_forzando_COLLSCAN (comparacion)": {
   "plan": [
    "SORT",
    "COLLSCAN"
   ],
   "nReturned": 1,
   "keysExaminadas": 0,
   "docsExaminados": 62437,
   "ms": 65
  },
  "2_asignar_sin_proyecto (global)": {
   "plan": [
    "SUBPLAN",
    "SORT",
    "FETCH",
    "OR",
    "IXSCAN(tareas_estado_expira)",
    "IXSCAN(tareas_estado_expira)"
   ],
   "nReturned": 1,
   "keysExaminadas": 59992,
   "docsExaminados": 59992,
   "ms": 105
  },
  "3_reanudar_tarea_abierta_del_anotador (tareas_anotador_estado)": {
   "plan": [
    "LIMIT",
    "FETCH",
    "IXSCAN(tareas_estado_expira)"
   ],
   "nReturned": 0,
   "keysExaminadas": 0,
   "docsExaminados": 0,
   "ms": 0
  },
  "4_tareas_de_un_batch (tareas_batch)": {
   "plan": [
    "FETCH",
    "IXSCAN(tareas_batch)"
   ],
   "nReturned": 100,
   "keysExaminadas": 100,
   "docsExaminados": 100,
   "ms": 0
  },
  "5_proyectos_activos_del_cliente (proyectos_cliente_estado)": {
   "plan": [
    "FETCH",
    "IXSCAN(proyectos_cliente_estado)"
   ],
   "nReturned": 1,
   "keysExaminadas": 1,
   "docsExaminados": 1,
   "ms": 0
  },
  "6_EXTRA_tareas_abiertas_vencidas (tareas_estado_expira)": {
   "plan": [
    "FETCH",
    "IXSCAN(tareas_estado_expira)"
   ],
   "nReturned": 29,
   "keysExaminadas": 29,
   "docsExaminados": 29,
   "ms": 0
  },
  "6b_EXTRA_sin_indice_forzando_COLLSCAN (comparacion)": {
   "plan": [
    "COLLSCAN"
   ],
   "nReturned": 29,
   "keysExaminadas": 0,
   "docsExaminados": 62437,
   "ms": 37
  },
  "7_exportar_aprobadas_paginado": {
   "plan": [
    "SORT",
    "FETCH",
    "IXSCAN(tareas_proyecto_estado_prioridad)"
   ],
   "nReturned": 0,
   "keysExaminadas": 0,
   "docsExaminados": 0,
   "ms": 1
  }
 },
 "indices": {
  "tareas": [
   {
    "nombre": "_id_",
    "clave": {
     "_id": 1
    }
   },
   {
    "nombre": "tareas_proyecto_estado_prioridad",
    "clave": {
     "proyecto_id": 1,
     "estado": 1,
     "prioridad": -1
    }
   },
   {
    "nombre": "tareas_anotador_estado",
    "clave": {
     "asignacion.anotador_id": 1,
     "estado": 1
    }
   },
   {
    "nombre": "tareas_batch",
    "clave": {
     "batch_id": 1
    }
   },
   {
    "nombre": "tareas_estado_expira",
    "clave": {
     "estado": 1,
     "asignacion.expira": 1
    }
   }
  ],
  "proyectos": [
   {
    "nombre": "_id_",
    "clave": {
     "_id": 1
    }
   },
   {
    "nombre": "proyectos_cliente_estado",
    "clave": {
     "cliente_id": 1,
     "estado": 1
    }
   }
  ]
 }
}
```

## Interpretación

Con 10 tareas libres y 50 solicitudes simultáneas se obtuvieron exactamente 10 asignaciones (200) y 40 respuestas 204 en cada una de las 3 rondas; la asignación es el findOneAndUpdate atómico de Mongo, por lo que ninguna tarea tuvo dos dueños (comprobado en las respuestas, con agregación en Mongo y con los tokens de los locks en Valkey). El segundo envío devolvió 409 y el lago no recibió una fila extra. La comparación w:majority/w:1 se midió en una sola máquina (3 nodos en contenedores): w:1 evita esperar la confirmación de la mayoría, pero la diferencia absoluta es pequeña porque no hay latencia de red real entre los nodos; no es extrapolable a un clúster distribuido. Los explain muestran qué índice usa cada consulta.
