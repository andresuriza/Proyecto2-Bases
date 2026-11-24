"""Editor simulado (proceso aparte que P3 mata a mitad de edición): borrador cada 2 s.
Imprime una línea JSON por cada borrador confirmado (HTTP 200) para que P3 conozca el último ack."""
import json
import sys
import time

import httpx

url, anotador, tarea, token = sys.argv[1:5]
c = httpx.Client(base_url=url, timeout=10)
v = 0
while True:
    v += 1
    t = time.time()
    r = c.put(f"/tareas/{tarea}/borrador", json={"anotador_id": anotador, "token": token, "version_local": v,
                                                  "contenido": {"v": v, "texto": "x" * 200, "t_cliente": t}})
    print(json.dumps({"v": v, "status": r.status_code, "t_cliente": t,
                      "ts_servidor": r.json().get("ts") if r.status_code == 200 else None}), flush=True)
    time.sleep(2)
