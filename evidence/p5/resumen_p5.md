# P5 resumen (tiempos en segundos, medidos por el servicio analitica)

| Config | q1 fría / caliente (mediana) | q2 fría / caliente (mediana) | q3 fría / caliente (mediana) | q4 fría / caliente (mediana) | q5 fría / caliente (mediana) |
|---|---|---|---|---|---|
| C0 original, sin compactar, 1 worker | 64.216 / 44.26 | 26.518 / 23.606 | 32.52 / 25.413 | 25.271 / 23.847 | 28.013 / 24.334 |
| C1 original, compactado, 1 worker | 34.841 / 6.587 | 15.429 / 4.544 | 10.414 / 5.332 | 5.801 / 4.595 | 7.758 / 5.075 |
| C2 final (filtro antes de dedup), 1 worker | 22.804 / 3.886 | 6.645 / 3.432 | 7.016 / 3.529 | 4.799 / 3.575 | 5.408 / 3.425 |
| C3 final, 2 workers | 28.29 / 3.59 | 7.46 / 3.163 | 8.083 / 3.978 | 5.939 / 2.968 | 6.294 / 4.574 |
| G2 grande 2 workers | 35.106 / 7.014 | 15.338 / 9.179 | 19.127 / 14.078 | 15.425 / 11.84 | 18.469 / 15.372 |
| G1 grande 1 worker | 34.712 / 6.446 | 17.517 / 12.322 | 28.939 / 15.546 | 17.456 / 10.423 | 15.47 / 12.712 |
