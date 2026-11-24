# P2. Registro, réplicas y concurrencia de asignación

- **Fecha y hora:** 2026-10-30T23:33:02 (duración 396.4 s)
- **Comando ejecutado:** `python pruebas/p2_concurrencia.py`
- **Máquina:** Windows-11-10.0.26200-SP0; CPU lógicos=12; Docker 29.9.2 (12 vCPU, 15.5 GiB); detalle en `evidence/entorno.txt`
- **Archivos que respaldan:** `evidence/p2_concurrencia_20261030T2333.json`, `evidence/p2_concurrencia_20261030T2333.md`

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
 "rondas_bench_por_modo": 3,
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
    "204": 40,
    "200": 10
   },
   "cumple": true,
   "dispersion_inicio_ms": 27.0
  },
  {
   "ronda": 2,
   "codigos": {
    "204": 40,
    "200": 10
   },
   "cumple": true,
   "dispersion_inicio_ms": 25.3
  },
  {
   "ronda": 3,
   "codigos": {
    "200": 10,
    "204": 40
   },
   "cumple": true,
   "dispersion_inicio_ms": 11.1
  }
 ],
 "todas_las_rondas_cumplen": true,
 "segundo_enviar": 409,
 "fila_extra_en_lago": false,
 "w_majority_vs_w1": {
  "majority": {
   "enviar": {
    "p50_ms": 174.24,
    "p95_ms": 300.54,
    "p99_ms": 365.97,
    "n_total": 1800
   },
   "siguiente": {
    "p50_ms": 173.6,
    "p95_ms": 292.47,
    "p99_ms": 352.81,
    "n_total": 1800
   },
   "ciclos_por_s_media": 20.8
  },
  "w1": {
   "enviar": {
    "p50_ms": 164.82,
    "p95_ms": 265.06,
    "p99_ms": 327.01,
    "n_total": 1800
   },
   "siguiente": {
    "p50_ms": 165.61,
    "p95_ms": 272.91,
    "p99_ms": 327.85,
    "n_total": 1800
   },
   "ciclos_por_s_media": 22.5
  }
 },
 "w_directo_en_mongo": [
  {
   "w": "majority",
   "n": 1500,
   "total_ms": 24446,
   "media_ms": 16.297333333333334,
   "p50_ms": 15,
   "p95_ms": 25,
   "p99_ms": 34
  },
  {
   "w": "1",
   "n": 1500,
   "total_ms": 7390,
   "media_ms": 4.926666666666667,
   "p50_ms": 4,
   "p95_ms": 8,
   "p99_ms": 11
  },
  {
   "w": "majority",
   "n": 1500,
   "total_ms": 22022,
   "media_ms": 14.681333333333333,
   "p50_ms": 14,
   "p95_ms": 22,
   "p99_ms": 26
  },
  {
   "w": "1",
   "n": 1500,
   "total_ms": 6772,
   "media_ms": 4.514666666666667,
   "p50_ms": 4,
   "p95_ms": 7,
   "p99_ms": 10
  },
  {
   "w": "majority",
   "n": 1500,
   "total_ms": 22266,
   "media_ms": 14.844,
   "p50_ms": 14,
   "p95_ms": 21,
   "p99_ms": 27
  },
  {
   "w": "1",
   "n": 1500,
   "total_ms": 6827,
   "media_ms": 4.551333333333333,
   "p50_ms": 4,
   "p95_ms": 7,
   "p99_ms": 10
  }
 ],
 "MONGO_W_restaurado": "majority",
 "replica_set": {
  "mongo1:27017": "PRIMARY",
  "mongo2:27017": "SECONDARY",
  "mongo3:27017": "SECONDARY"
 }
}
```

### rs.status() resumen  _(23:33:05)_

```json
{
 "set": "rs0",
 "miembros": [
  {
   "nombre": "mongo1:27017",
   "estado": "PRIMARY",
   "salud": 1,
   "optime": "2026-10-31T05:32:53.000Z",
   "uptime_s": 592
  },
  {
   "nombre": "mongo2:27017",
   "estado": "SECONDARY",
   "salud": 1,
   "optime": "2026-10-31T05:32:53.000Z",
   "pingMs": {
    "high": 0,
    "low": 0,
    "unsigned": false
   },
   "uptime_s": 175
  },
  {
   "nombre": "mongo3:27017",
   "estado": "SECONDARY",
   "salud": 1,
   "optime": "2026-10-31T05:32:53.000Z",
   "pingMs": {
    "high": 0,
    "low": 0,
    "unsigned": false
   },
   "uptime_s": 175
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

### Pool de procesos  _(23:33:28)_

```json
{
 "procesos_distintos": 50
}
```

### Ronda concurrente 1  _(23:33:34)_

```json
{
 "ronda": 1,
 "proyecto_id": "524e55cf-58c7-4a11-9886-3505882e132e",
 "codigos": {
  "204": 40,
  "200": 10
 },
 "tareas_distintas_en_respuestas_200": 10,
 "dispersion_inicio_ms": 27.0,
 "latencia_ms": {
  "n": 50,
  "p50_ms": 711.04,
  "p95_ms": 894.97,
  "p99_ms": 897.4,
  "media_ms": 741.58,
  "max_ms": 897.4
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

### Ronda concurrente 2  _(23:33:39)_

```json
{
 "ronda": 2,
 "proyecto_id": "b7f9cead-f591-4bba-90ac-fde51e567666",
 "codigos": {
  "204": 40,
  "200": 10
 },
 "tareas_distintas_en_respuestas_200": 10,
 "dispersion_inicio_ms": 25.3,
 "latencia_ms": {
  "n": 50,
  "p50_ms": 820.25,
  "p95_ms": 877.55,
  "p99_ms": 889.32,
  "media_ms": 808.94,
  "max_ms": 889.32
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

### Ronda concurrente 3  _(23:33:44)_

```json
{
 "ronda": 3,
 "proyecto_id": "2f3aa16d-04e7-438f-ad71-4c57f067baf8",
 "codigos": {
  "200": 10,
  "204": 40
 },
 "tareas_distintas_en_respuestas_200": 10,
 "dispersion_inicio_ms": 11.1,
 "latencia_ms": {
  "n": 50,
  "p50_ms": 709.0,
  "p95_ms": 778.42,
  "p99_ms": 782.47,
  "media_ms": 718.51,
  "max_ms": 782.47
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

### Segundo envío de una tarea ya enviada  _(23:33:59)_

```json
{
 "tarea_id": "6693e93f-80c1-4a18-b353-ea491f1f6b9c",
 "primer_enviar": 200,
 "segundo_enviar": 409,
 "enviar_otro_anotador_mismo_token": 409,
 "filas_lago_tras_6s": [
  [
   "abierta",
   "3f7969c4"
  ],
  [
   "anotada",
   "13901070"
  ]
 ],
 "filas_lago_tras_12s": [
  [
   "abierta",
   "3f7969c4"
  ],
  [
   "anotada",
   "13901070"
  ]
 ],
 "filas_estado_anotada": 1,
 "mongo": "{\n  _id: '6693e93f-80c1-4a18-b353-ea491f1f6b9c',\n  estado: 'anotada',\n  version: 2,\n  ultimo_evento_id: '13901070-8c01-4ecc-9db4-e681ae3f5026'\n}",
 "cumple": true
}
```

### Microbenchmark con MONGO_W=majority  _(23:34:40)_

```json
{
 "proyecto_id": "f7d77b86-972d-47df-a943-5cb6d6a5d0d4",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 28.9,
 "ciclos_por_s": 20.8,
 "siguiente": {
  "n": 600,
  "p50_ms": 169.7,
  "p95_ms": 300.03,
  "p99_ms": 363.95,
  "media_ms": 182.49,
  "max_ms": 632.88
 },
 "enviar": {
  "n": 600,
  "p50_ms": 173.52,
  "p95_ms": 319.23,
  "p99_ms": 375.58,
  "media_ms": 186.06,
  "max_ms": 539.39
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "majority"
}
```

### Microbenchmark con MONGO_W=1  _(23:35:13)_

```json
{
 "proyecto_id": "f415d9cd-8fcf-4926-b8c4-82efedda7d20",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 23.0,
 "ciclos_por_s": 26.1,
 "siguiente": {
  "n": 600,
  "p50_ms": 141.39,
  "p95_ms": 232.3,
  "p99_ms": 274.2,
  "media_ms": 146.73,
  "max_ms": 311.97
 },
 "enviar": {
  "n": 600,
  "p50_ms": 138.72,
  "p95_ms": 223.57,
  "p99_ms": 258.54,
  "media_ms": 142.3,
  "max_ms": 347.01
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "1"
}
```

### Microbenchmark con MONGO_W=majority  _(23:35:51)_

```json
{
 "proyecto_id": "afb7aedd-ec5b-4198-b87d-daf91e1e43e0",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 26.2,
 "ciclos_por_s": 22.9,
 "siguiente": {
  "n": 600,
  "p50_ms": 156.95,
  "p95_ms": 257.95,
  "p99_ms": 300.18,
  "media_ms": 166.51,
  "max_ms": 400.11
 },
 "enviar": {
  "n": 600,
  "p50_ms": 156.02,
  "p95_ms": 245.84,
  "p99_ms": 295.05,
  "media_ms": 165.4,
  "max_ms": 354.3
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "majority"
}
```

### Microbenchmark con MONGO_W=1  _(23:36:27)_

```json
{
 "proyecto_id": "c86f15de-4db4-4b16-8e70-410fbaf491ee",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 25.4,
 "ciclos_por_s": 23.6,
 "siguiente": {
  "n": 600,
  "p50_ms": 152.78,
  "p95_ms": 248.52,
  "p99_ms": 301.78,
  "media_ms": 161.84,
  "max_ms": 499.13
 },
 "enviar": {
  "n": 600,
  "p50_ms": 151.19,
  "p95_ms": 246.84,
  "p99_ms": 302.8,
  "media_ms": 159.75,
  "max_ms": 389.29
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "1"
}
```

### Microbenchmark con MONGO_W=majority  _(23:37:10)_

```json
{
 "proyecto_id": "07d37999-0d5b-4f65-a399-c8b9cbbce072",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 32.3,
 "ciclos_por_s": 18.6,
 "siguiente": {
  "n": 600,
  "p50_ms": 194.15,
  "p95_ms": 319.43,
  "p99_ms": 394.29,
  "media_ms": 204.64,
  "max_ms": 472.54
 },
 "enviar": {
  "n": 600,
  "p50_ms": 193.18,
  "p95_ms": 336.54,
  "p99_ms": 427.28,
  "media_ms": 207.41,
  "max_ms": 549.44
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "majority"
}
```

### Microbenchmark con MONGO_W=1  _(23:37:55)_

```json
{
 "proyecto_id": "a47176b9-3b0d-48f7-a877-13eb91178601",
 "ops": 600,
 "hilos": 8,
 "duracion_s": 33.5,
 "ciclos_por_s": 17.9,
 "siguiente": {
  "n": 600,
  "p50_ms": 202.66,
  "p95_ms": 337.92,
  "p99_ms": 407.57,
  "media_ms": 214.84,
  "max_ms": 476.4
 },
 "enviar": {
  "n": 600,
  "p50_ms": 204.54,
  "p95_ms": 324.76,
  "p99_ms": 419.7,
  "media_ms": 214.16,
  "max_ms": 461.87
 },
 "errores": {
  "sig": 0,
  "env": 0
 },
 "MONGO_W_en_contenedor": "1"
}
```

### Comparación w:majority vs w:1 (media de las rondas)  _(23:38:04)_

Los valores son medias de los percentiles de cada ronda. En un solo host con 3 nodos en contenedores la latencia de replicación es mínima, así que la diferencia NO permite inferir el rendimiento de un clúster distribuido.

```json
{
 "majority": {
  "enviar": {
   "p50_ms": 174.24,
   "p95_ms": 300.54,
   "p99_ms": 365.97,
   "n_total": 1800
  },
  "siguiente": {
   "p50_ms": 173.6,
   "p95_ms": 292.47,
   "p99_ms": 352.81,
   "n_total": 1800
  },
  "ciclos_por_s_media": 20.8
 },
 "w1": {
  "enviar": {
   "p50_ms": 164.82,
   "p95_ms": 265.06,
   "p99_ms": 327.01,
   "n_total": 1800
  },
  "siguiente": {
   "p50_ms": 165.61,
   "p95_ms": 272.91,
   "p99_ms": 327.85,
   "n_total": 1800
  },
  "ciclos_por_s_media": 22.5
 }
}
```

### w:majority vs w:1 directamente en Mongo (1500 updateOne secuenciales, 3 pares, resolución 1 ms)  _(23:39:35)_

Sin HTTP ni gateway: aísla el coste del reconocimiento de la mayoría en un clúster local de 3 contenedores.

```json
[
 {
  "w": "majority",
  "n": 1500,
  "total_ms": 24446,
  "media_ms": 16.297333333333334,
  "p50_ms": 15,
  "p95_ms": 25,
  "p99_ms": 34
 },
 {
  "w": "1",
  "n": 1500,
  "total_ms": 7390,
  "media_ms": 4.926666666666667,
  "p50_ms": 4,
  "p95_ms": 8,
  "p99_ms": 11
 },
 {
  "w": "majority",
  "n": 1500,
  "total_ms": 22022,
  "media_ms": 14.681333333333333,
  "p50_ms": 14,
  "p95_ms": 22,
  "p99_ms": 26
 },
 {
  "w": "1",
  "n": 1500,
  "total_ms": 6772,
  "media_ms": 4.514666666666667,
  "p50_ms": 4,
  "p95_ms": 7,
  "p99_ms": 10
 },
 {
  "w": "majority",
  "n": 1500,
  "total_ms": 22266,
  "media_ms": 14.844,
  "p50_ms": 14,
  "p95_ms": 21,
  "p99_ms": 27
 },
 {
  "w": "1",
  "n": 1500,
  "total_ms": 6827,
  "media_ms": 4.551333333333333,
  "p50_ms": 4,
  "p95_ms": 7,
  "p99_ms": 10
 }
]
```

### explain(executionStats) por consulta  _(23:39:39)_

```json
{
 "proyecto_probado": "f2f25eef-1f45-4bfd-b7dc-67e030974b2b",
 "total_tareas": 81740,
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
   "ms": 1
  },
  "1b_asignar_con_proyecto_forzando_COLLSCAN (comparacion)": {
   "plan": [
    "SORT",
    "COLLSCAN"
   ],
   "nReturned": 1,
   "keysExaminadas": 0,
   "docsExaminados": 81740,
   "ms": 219
  },
  "2_asignar_sin_proyecto (global)": {
   "plan": [
    "SUBPLAN",
    "LIMIT",
    "FETCH",
    "SORT_MERGE",
    "IXSCAN(tareas_estado_prioridad_expira)",
    "IXSCAN(tareas_estado_prioridad_expira)"
   ],
   "nReturned": 1,
   "keysExaminadas": 2,
   "docsExaminados": 1,
   "ms": 1
  },
  "3_reanudar_tarea_abierta_del_anotador (tareas_anotador_estado)": {
   "plan": [
    "LIMIT",
    "FETCH",
    "IXSCAN(tareas_anotador_estado)"
   ],
   "nReturned": 0,
   "keysExaminadas": 1,
   "docsExaminados": 1,
   "ms": 1
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
    "IXSCAN(tareas_estado_prioridad_expira)"
   ],
   "nReturned": 100,
   "keysExaminadas": 101,
   "docsExaminados": 100,
   "ms": 1
  },
  "6b_EXTRA_sin_indice_forzando_COLLSCAN (comparacion)": {
   "plan": [
    "COLLSCAN"
   ],
   "nReturned": 100,
   "keysExaminadas": 0,
   "docsExaminados": 81740,
   "ms": 50
  },
  "7_exportar_aprobadas_paginado": {
   "plan": [
    "SORT",
    "FETCH",
    "IXSCAN(tareas_proyecto_estado_prioridad)"
   ],
   "nReturned": 22,
   "keysExaminadas": 22,
   "docsExaminados": 22,
   "ms": 2
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
    "nombre": "tareas_estado_prioridad_expira",
    "clave": {
     "estado": 1,
     "prioridad": -1,
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
