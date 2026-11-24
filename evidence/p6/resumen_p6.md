# P6 resumen de carga

Fecha: 2026-10-31T00:02:57. Percentiles en ms sobre 3 corridas x 90 s por nivel.

| Endpoint | VUs | n | p50 | p95 | p99 | errores |
|---|---|---|---|---|---|---|
| GET /proyectos/{id}/exportar | 10 | 136 | 20.42 | 33.89 | 39.98 | 0 |
| GET /proyectos/{id}/exportar | 40 | 516 | 20.67 | 36.41 | 45.96 | 0 |
| GET /proyectos/{id}/exportar | 100 | 1241 | 38.36 | 107.35 | 165.78 | 0 |
| GET /salud | 10 | 36 | 15.55 | 21.78 | 26.55 | 0 |
| GET /salud | 40 | 164 | 16.57 | 36.25 | 46.52 | 0 |
| GET /salud | 100 | 376 | 38.69 | 140.71 | 235.92 | 0 |
| POST /sesiones/recuperar | 10 | 8 | 15.32 | 23.86 | 23.86 | 0 |
| POST /sesiones/recuperar | 40 | 30 | 14.62 | 29.19 | 31.88 | 0 |
| POST /sesiones/recuperar | 100 | 71 | 39.72 | 421.97 | 591.34 | 0 |
| POST /tareas/siguiente | 10 | 329 | 31.23 | 131.4 | 184.07 | 0 |
| POST /tareas/siguiente | 40 | 1258 | 31.4 | 444.68 | 558.25 | 0 |
| POST /tareas/siguiente | 100 | 3016 | 69.89 | 1241.17 | 2097.63 | 0 |
| POST /tareas/{id}/enviar | 10 | 293 | 32.69 | 47.11 | 58.77 | 0 |
| POST /tareas/{id}/enviar | 40 | 1125 | 30.78 | 52.7 | 77.4 | 0 |
| POST /tareas/{id}/enviar | 100 | 2679 | 66.87 | 300.48 | 693.26 | 0 |
| POST /tareas/{id}/liberar | 10 | 36 | 23.56 | 43.63 | 46.05 | 0 |
| POST /tareas/{id}/liberar | 40 | 133 | 54.37 | 228.96 | 252.42 | 0 |
| POST /tareas/{id}/liberar | 100 | 337 | 292.97 | 870.84 | 995.93 | 0 |
| POST /tareas/{id}/revision | 10 | 496 | 20.65 | 29.37 | 33.2 | 0 |
| POST /tareas/{id}/revision | 40 | 2022 | 20.04 | 33.91 | 50.84 | 0 |
| POST /tareas/{id}/revision | 100 | 4858 | 37.54 | 135.37 | 327.85 | 0 |
| PUT /tareas/{id}/borrador | 10 | 1045 | 10.3 | 16.24 | 25.1 | 0 |
| PUT /tareas/{id}/borrador | 40 | 4152 | 9.61 | 18.87 | 28.47 | 0 |
| PUT /tareas/{id}/borrador | 100 | 9886 | 21.52 | 119.83 | 290.26 | 0 |

| VUs | envios/min (media; min-max) | req/s | peticiones | errores (5xx/red) | tasa error % |
|---|---|---|---|---|---|
| 10 | 65.1 (62.7-66.7) | 8.8 | 2379 | 0 | 0.0 |
| 40 | 250.0 (248.0-251.3) | 34.8 | 9400 | 0 | 0.0 |
| 100 | 595.3 (583.3-605.3) | 83.2 | 22464 | 0 | 0.0 |

## docker stats (CPU % media / máx; memoria máx MiB)

| Contenedor | 10 VUs | 40 VUs | 100 VUs |
|---|---|---|---|
| analitica | 0.8 / 1.6 ; 376.4 | 1.0 / 4.0 ; 377.2 | 2.0 / 4.1 ; 380.2 |
| gateway | 8.6 / 25.2 ; 59.4 | 22.9 / 60.4 ; 61.9 | 70.4 / 132.2 ; 60.2 |
| herramientas | 3.6 / 35.1 ; 24.9 | 11.3 / 32.2 ; 29.7 | 37.9 / 71.2 ; 37.3 |
| historial | 1.2 / 3.8 ; 47.4 | 2.0 / 5.2 ; 49.1 | 4.6 / 8.3 ; 49.7 |
| mongo1 | 22.5 / 107.1 ; 505.1 | 26.1 / 94.7 ; 505.4 | 54.5 / 164.1 ; 530.8 |
| mongo2 | 17.6 / 125.0 ; 383.2 | 23.8 / 122.0 ; 404.9 | 41.2 / 140.3 ; 434.1 |
| mongo3 | 19.1 / 116.2 ; 373.8 | 22.5 / 111.6 ; 429.5 | 44.0 / 133.2 ; 444.8 |
| registro | 5.1 / 23.1 ; 54.0 | 10.2 / 30.5 ; 47.6 | 28.4 / 60.6 ; 57.2 |
| sesion | 4.7 / 23.1 ; 45.3 | 11.0 / 29.2 ; 47.4 | 28.1 / 59.9 ; 41.6 |
| spark-master | 0.3 / 1.2 ; 443.3 | 0.3 / 0.8 ; 445.2 | 0.5 / 1.4 ; 445.6 |
| spark-worker-1 | 0.3 / 0.6 ; 518.0 | 0.3 / 0.7 ; 518.6 | 0.9 / 2.9 ; 519.1 |
| valkey | 2.7 / 28.7 ; 56.3 | 3.5 / 28.2 ; 60.1 | 9.6 / 66.9 ; 67.6 |
