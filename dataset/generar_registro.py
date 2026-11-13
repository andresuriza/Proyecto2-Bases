"""Generador determinista (seed) del dataset del registro operacional (MongoDB).

Volúmenes (enunciado §9): 60 clientes, 180 proyectos (120 activos para Mongo + 60
cerrados que sólo existen en el historial), 600 batches, 60 000 tareas (30 % texto,
30 % imagen, 20 % video, 20 % LiDAR), 300 anotadores en 30 equipos (10 % revisores).

Salida: JSONL en --out (clientes, proyectos, batches, tareas, anotadores) + resumen.json.
`generar(seed)` también lo importa generar_historial.py para que los ids coincidan.

Uso: python dataset/generar_registro.py --seed 42 --out dataset/out/
"""
import argparse
import hashlib
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

N_CLIENTES, N_ACTIVOS, N_CERRADOS, N_BATCHES = 60, 120, 60, 600
N_TAREAS, N_EQUIPOS, POR_EQUIPO = 60000, 30, 10
REVISORES_POR_EQUIPO = 1  # 30 revisores de 300 = 10 %

INICIO = datetime(2025, 11, 1, tzinfo=timezone.utc)   # ventana histórica: 12 meses
FIN = datetime(2026, 10, 30, 23, 59, 59, tzinfo=timezone.utc)

MODALIDADES = ["texto", "imagen", "video", "lidar"]
TIPOS = {"texto": ["span"], "imagen": ["bbox", "poligono"], "video": ["bbox_video"],
         "lidar": ["cuboide_3d"]}
MIME = {"texto": "text/plain", "imagen": "image/jpeg", "video": "video/mp4",
        "lidar": "application/vnd.pcd"}
EXT = {"texto": "txt", "imagen": "jpg", "video": "mp4", "lidar": "pcd"}
TAXONOMIAS = {
    "texto": [("texto#persona", "Nombres propios de personas."),
              ("texto#organizacion", "Empresas, instituciones y entidades."),
              ("texto#lugar", "Ciudades, países y direcciones.")],
    "imagen": [("imagen#objeto", "Cualquier objeto delimitable con su caja o polígono."),
               ("imagen#persona", "Persona completa o parcialmente visible."),
               ("imagen#vehiculo", "Automóviles, motos y bicicletas con su conductor aparte.")],
    "video": [("video#objeto_movil", "Seguir el objeto con una caja en cada fotograma clave."),
              ("video#persona", "Peatón o trabajador en escena."),
              ("video#vehiculo", "Vehículo en movimiento.")],
    "lidar": [("lidar#peaton", "Persona a pie; incluye personas que empujan un cochecito."),
              ("lidar#vehiculo", "Automóvil, camión, bus o moto con motor."),
              ("lidar#ciclista", "Persona sobre bicicleta, monopatín o patineta; un skater debe "
                                 "clasificarse como ciclista y no como peatón."),
              ("lidar#signal", "Señal vertical de tránsito o semáforo.")],
}
NOMBRES = ["Andino", "Boreal", "Cumbre", "Delta", "Éter", "Faro", "Granito", "Horizonte", "Índigo",
           "Júpiter", "Kora", "Lumen", "Mistral", "Nodo", "Orbita", "Pampa", "Quasar", "Rumbo",
           "Sierra", "Tundra"]
SUFIJOS = ["Robotics", "Mobility", "Health", "Retail", "Labs", "Systems", "Vision", "Cloud", "AI", "Data"]


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class Gen:
    def __init__(self, seed):
        self.rng = random.Random(seed)
        self.seed = seed

    def uid(self):
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))


def generar(seed=42):
    """Devuelve dict con listas: clientes, proyectos, batches, tareas, anotadores (+ equipos)."""
    g = Gen(seed)
    rng = g.rng
    clientes = []
    for i in range(N_CLIENTES):
        clientes.append({"_id": g.uid(), "nombre": f"{NOMBRES[i % 20]} {SUFIJOS[(i // 20 * 3 + i) % 10]} {i + 1:02d}",
                         "pais": rng.choice(["CR", "MX", "US", "CO", "ES", "DE"]),
                         "creado_en": iso(INICIO - timedelta(days=rng.randint(30, 400)))})

    # --- equipos y anotadores: 30 equipos x 10 personas (1 revisor + 9 anotadores) ---
    equipos = [g.uid() for _ in range(N_EQUIPOS)]
    anotadores = []
    for ei, eq in enumerate(equipos):
        for k in range(POR_EQUIPO):
            rol = "revisor" if k < REVISORES_POR_EQUIPO else "anotador"
            anotadores.append({"_id": g.uid(), "nombre": f"Persona {ei * POR_EQUIPO + k + 1:03d}",
                               "equipo_id": eq, "rol": rol})

    # --- proyectos: 120 activos + 60 cerrados, modalidades 30/30/20/20 ---
    def reparto(n):
        n_t, n_i = round(n * .3), round(n * .3)
        n_v = round(n * .2)
        return ["texto"] * n_t + ["imagen"] * n_i + ["video"] * n_v + ["lidar"] * (n - n_t - n_i - n_v)

    proyectos = []
    mods = reparto(N_ACTIVOS)
    rng.shuffle(mods)
    # el primer proyecto LiDAR activo es el caso de ejemplo del enunciado (SLA 48 h)
    ejemplo_idx = mods.index("lidar")
    mods_c = reparto(N_CERRADOS)
    rng.shuffle(mods_c)
    todos = [(m, "activo") for m in mods] + [(m, "cerrado") for m in mods_c]
    for i, (mod, estado) in enumerate(todos):
        cli = clientes[i % N_CLIENTES] if i < N_CLIENTES else rng.choice(clientes)
        ini = INICIO + timedelta(days=rng.randint(0, 150))
        sla = rng.choice([24, 48, 48, 72, 96])
        nombre = f"{mod.capitalize()} {NOMBRES[rng.randrange(20)]} {i + 1:03d}"
        tipo_p = rng.choice(TIPOS[mod])
        ejemplo = (estado == "activo" and i == ejemplo_idx)
        if ejemplo:
            nombre = "Anotación LiDAR para vehículos autónomos (ejemplo del enunciado)"
            sla = 48
        p = {"_id": g.uid(), "cliente_id": cli["_id"], "nombre": nombre, "modalidad": mod,
             "tipo_tarea": tipo_p,
             "taxonomia": [{"etiqueta": e, "instruccion": ins} for e, ins in TAXONOMIAS[mod]],
             "reglas": {"min_revisiones": 1, "max_reintentos": 5, "tipo_geometria": tipo_p},
             "sla_horas": sla, "estado": estado, "creado_en": iso(ini),
             "ejemplo_lidar": ejemplo}
        if estado == "cerrado":
            p["cerrado_en"] = iso(ini + timedelta(days=rng.randint(90, 240)))
        proyectos.append(p)

    # --- batches (600) y tareas (60 000) sobre los 120 proyectos activos ---
    activos = [p for p in proyectos if p["estado"] == "activo"]
    batches, tareas = [], []
    por_batch = N_TAREAS // N_BATCHES  # 100
    por_proyecto = N_BATCHES // N_ACTIVOS  # 5
    ahora = FIN
    for p in activos:
        for b in range(por_proyecto):
            bid = g.uid()
            batches.append({"_id": bid, "proyecto_id": p["_id"], "cliente_id": p["cliente_id"],
                            "nombre": f"lote-{b + 1}", "n_tareas": por_batch,
                            "creado_en": iso(ahora - timedelta(days=rng.randint(0, 5)))})
            mod = p["modalidad"]
            for k in range(por_batch):
                tid = g.uid()
                uri = f"s3://datos-anotacion/{p['cliente_id'][:8]}/{p['_id'][:8]}/{bid[:8]}/{tid}.{EXT[mod]}"
                sha = hashlib.sha256(f"{seed}|{uri}".encode()).hexdigest()
                tipo = rng.choice(TIPOS[mod]) if mod == "imagen" else p["tipo_tarea"]
                if mod == "texto":
                    n = rng.randint(200, 20000)
                    meta, size = {"caracteres": n, "idioma": "es", "min": 0, "max": n}, n * 2
                elif mod == "imagen":
                    w, h = rng.choice([(640, 480), (1280, 720), (1920, 1080), (4032, 3024)])
                    meta, size = {"ancho": w, "alto": h, "min": 0, "max": 255}, int(w * h * rng.uniform(.15, .4))
                elif mod == "video":
                    d = rng.randint(5, 120)
                    meta, size = {"duracion_s": d, "fps": rng.choice([24, 30, 60]), "ancho": 1920,
                                  "alto": 1080, "min": 0, "max": 255}, d * rng.randint(400_000, 1_500_000)
                else:
                    pts = rng.randint(80_000, 400_000)
                    meta, size = {"puntos": pts, "frecuencia_hz": 10,
                                  "min": round(rng.uniform(-3, -1), 2), "max": round(rng.uniform(15, 40), 2),
                                  "sistema": "xyz-intensidad"}, pts * 16
                tareas.append({
                    "_id": tid, "proyecto_id": p["_id"], "cliente_id": p["cliente_id"], "batch_id": bid,
                    "modalidad": mod, "tipo_tarea": tipo, "prioridad": rng.choices([1, 2, 3, 4, 5], [10, 20, 40, 20, 10])[0],
                    "archivo": {"uri": uri, "sha256": sha, "bytes": size, "mime": MIME[mod], "meta": meta},
                    "estado": "disponible", "asignacion": None, "anotacion": None, "ultima_revision": None,
                    "version": 0, "ultimo_evento_id": None, "sla_horas": p["sla_horas"],
                    "creada_en": iso(ahora), "actualizada_en": iso(ahora)})
    return {"clientes": clientes, "proyectos": proyectos, "batches": batches, "tareas": tareas,
            "anotadores": anotadores, "equipos": equipos}


def escribir(datos, out):
    os.makedirs(out, exist_ok=True)
    resumen = {}
    for nombre in ("clientes", "proyectos", "batches", "tareas", "anotadores"):
        ruta = os.path.join(out, f"{nombre}.jsonl")
        with open(ruta, "w", encoding="utf8") as fh:
            for d in datos[nombre]:
                fh.write(json.dumps(d, ensure_ascii=False, separators=(",", ":")) + "\n")
        resumen[nombre] = len(datos[nombre])
    t = datos["tareas"]
    resumen["tareas_por_modalidad"] = {m: sum(1 for x in t if x["modalidad"] == m) for m in MODALIDADES}
    resumen["proyectos_activos"] = sum(1 for p in datos["proyectos"] if p["estado"] == "activo")
    resumen["proyectos_cerrados"] = sum(1 for p in datos["proyectos"] if p["estado"] == "cerrado")
    resumen["equipos"] = len(datos["equipos"])
    resumen["revisores"] = sum(1 for a in datos["anotadores"] if a["rol"] == "revisor")
    resumen["proyecto_lidar_ejemplo"] = next(p["_id"] for p in datos["proyectos"] if p.get("ejemplo_lidar"))
    with open(os.path.join(out, "resumen.json"), "w", encoding="utf8") as fh:
        json.dump(resumen, fh, indent=1, ensure_ascii=False)
    return resumen


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="dataset/out/")
    a = ap.parse_args()
    r = escribir(generar(a.seed), a.out)
    print(json.dumps(r, indent=1, ensure_ascii=False))
