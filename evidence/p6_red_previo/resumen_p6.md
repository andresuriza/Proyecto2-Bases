# P6 resumen de carga

Fecha: 2026-10-30T22:49:04. Percentiles en ms sobre 3 corridas x 90 s por nivel.

| Endpoint | VUs | n | p50 | p95 | p99 | errores |
|---|---|---|---|---|---|---|
| GET /proyectos/{id}/exportar | 10 | 67 | 16.65 | 31.41 | 51.76 | 0 |
| GET /proyectos/{id}/exportar | 40 | 237 | 16.41 | 35.61 | 44.61 | 0 |
| GET /proyectos/{id}/exportar | 100 | 678 | 97.08 | 495.21 | 1425.27 | 0 |
| GET /salud | 10 | 26 | 16.3 | 27.02 | 27.2 | 0 |
| GET /salud | 40 | 165 | 18.45 | 37.58 | 47.66 | 0 |
| GET /salud | 100 | 346 | 126.57 | 542.98 | 901.94 | 0 |
| POST /sesiones/recuperar | 10 | 11 | 12.13 | 25.52 | 25.52 | 0 |
| POST /sesiones/recuperar | 40 | 34 | 17.57 | 41.75 | 107.91 | 0 |
| POST /sesiones/recuperar | 100 | 74 | 166.06 | 1102.62 | 2449.59 | 0 |
| POST /tareas/siguiente | 10 | 318 | 27.24 | 86.08 | 179.95 | 0 |
| POST /tareas/siguiente | 40 | 1264 | 33.13 | 326.79 | 493.3 | 0 |
| POST /tareas/siguiente | 100 | 2683 | 282.29 | 2188.39 | 2945.9 | 0 |
| POST /tareas/{id}/enviar | 10 | 288 | 27.07 | 48.45 | 97.73 | 0 |
| POST /tareas/{id}/enviar | 40 | 1124 | 31.88 | 54.55 | 82.88 | 0 |
| POST /tareas/{id}/enviar | 100 | 2363 | 251.5 | 1284.92 | 2382.16 | 1 |
| POST /tareas/{id}/liberar | 10 | 30 | 29.6 | 37.88 | 38.39 | 0 |
| POST /tareas/{id}/liberar | 40 | 140 | 56.54 | 328.81 | 391.88 | 0 |
| POST /tareas/{id}/liberar | 100 | 318 | 608.08 | 1588.98 | 1812.32 | 0 |
| POST /tareas/{id}/revision | 10 | 508 | 18.94 | 30.37 | 46.37 | 0 |
| POST /tareas/{id}/revision | 40 | 2022 | 20.52 | 38.05 | 71.33 | 0 |
| POST /tareas/{id}/revision | 100 | 4334 | 117.01 | 514.99 | 1421.56 | 0 |
| PUT /tareas/{id}/borrador | 10 | 1032 | 9.44 | 17.17 | 26.12 | 0 |
| PUT /tareas/{id}/borrador | 40 | 4138 | 10.38 | 21.75 | 38.71 | 0 |
| PUT /tareas/{id}/borrador | 100 | 8252 | 94.31 | 659.92 | 1433.24 | 0 |

| VUs | envios/min (media; min-max) | req/s | peticiones | errores (5xx/red) | tasa error % |
|---|---|---|---|---|---|
| 10 | 64.0 (61.3-66.7) | 8.4 | 2280 | 0 | 0.0 |
| 40 | 249.8 (248.0-251.3) | 33.8 | 9124 | 0 | 0.0 |
| 100 | 524.9 (478.0-554.7) | 70.6 | 19048 | 1 | 0.005 |

## docker stats (CPU % media / máx; memoria máx MiB)

| Contenedor | 10 VUs | 40 VUs | 100 VUs |
|---|---|---|---|
| analitica | 0.8 / 3.7 ; 753.8 | 1.1 / 3.4 ; 754.0 | 1.7 / 3.5 ; 754.1 |
| gateway | 7.6 / 32.2 ; 66.2 | 22.9 / 70.0 ; 67.6 | 86.8 / 172.1 ; 68.4 |
| herramientas | 3.1 / 14.3 ; 24.6 | 11.7 / 35.1 ; 29.0 | 50.2 / 126.6 ; 37.0 |
| historial | 1.4 / 4.3 ; 372.3 | 2.2 / 7.2 ; 372.6 | 4.9 / 8.9 ; 372.7 |
| mongo1 | 16.9 / 108.8 ; 465.1 | 26.8 / 98.4 ; 467.5 | 58.2 / 159.8 ; 506.3 |
| mongo2 | 16.5 / 106.8 ; 424.1 | 24.8 / 132.2 ; 447.6 | 50.3 / 173.3 ; 477.5 |
| mongo3 | 14.1 / 76.7 ; 425.4 | 24.4 / 99.7 ; 431.3 | 49.1 / 151.0 ; 466.1 |
| registro | 5.6 / 19.5 ; 55.6 | 9.3 / 32.3 ; 55.5 | 29.8 / 60.4 ; 55.8 |
| sesion | 6.2 / 30.3 ; 49.9 | 9.8 / 26.1 ; 39.5 | 32.4 / 69.1 ; 48.0 |
| spark-master | 0.2 / 0.8 ; 431.1 | 0.4 / 1.8 ; 431.7 | 1.7 / 54.7 ; 436.9 |
| spark-worker-1 | 0.4 / 1.0 ; 1347.6 | 0.3 / 0.8 ; 1342.5 | 0.5 / 1.6 ; 1342.5 |
| valkey | 2.1 / 7.4 ; 21.5 | 2.7 / 9.2 ; 23.4 | 8.5 / 24.1 ; 31.3 |
