"""P5b. Resultados completos de Q1-Q5 sobre el conjunto principal (cuerpos archivados + estadísticos para la interpretación).

Hace una ejecución de calentamiento de cada consulta (no se archiva el cuerpo, sí su tiempo) y una ejecución medida;
guarda el cuerpo completo de la respuesta en evidence/p5/resultados_principal_q1.json ... q5.json y el estado del lago
(archivos, filas crudas, event_id distintos, eventos_leidos con contar=1). No compacta ni modifica nada.

Uso: python pruebas/p5_resultados.py
"""
import glob
import json
import os
import statistics as stt
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import comun as C  # noqa: E402
from p5_analitica import cuenta_lago  # noqa: E402

QS = ["q1", "q2", "q3", "q4", "q5"]
DIR = os.path.join(C.EVIDENCE, "p5")
os.makedirs(DIR, exist_ok=True)


def r4(x):
    return None if x is None else round(x, 4)


def por_dia_tmp(f):
    r = {}
    for x in f:
        r[x["dia"]] = r.get(x["dia"], 0) + x["aprobadas"]
    return r


def resumen_q1(f):
    tot = sum(x["aprobadas"] for x in f)
    dias = sorted({x["dia"] for x in f})
    mod = {}
    for x in f:
        mod[x["modalidad"]] = mod.get(x["modalidad"], 0) + x["aprobadas"]
    por_dia = {}
    for x in f:
        por_dia[x["dia"]] = por_dia.get(x["dia"], 0) + x["aprobadas"]
    mayor = max(por_dia.items(), key=lambda kv: kv[1])
    menor = min(por_dia.items(), key=lambda kv: kv[1])
    ult = max(por_dia_tmp(f))
    return {"total_aprobadas": tot, "total_sin_ultimo_dia": tot - por_dia_tmp(f)[ult], "dias_distintos": len(dias), "primer_dia": dias[0], "ultimo_dia": dias[-1],
            "por_modalidad": dict(sorted(mod.items())), "aprobadas_por_dia_total": {
                "min": menor[1], "dia_min": menor[0], "max": mayor[1], "dia_max": mayor[0],
                "mediana": stt.median(por_dia.values()), "media": round(stt.mean(por_dia.values()), 2)},
            "por_dia_y_modalidad": {"min": min(x["aprobadas"] for x in f), "max": max(x["aprobadas"] for x in f),
                                    "mediana": stt.median(x["aprobadas"] for x in f)},
            "primeros_7_dias_total": [[d, por_dia[d]] for d in dias[:7]], "ultimos_7_dias_total": [[d, por_dia[d]] for d in dias[-7:]]}


def resumen_q2(f):
    pr = [x["proporcion_aprobadas"] for x in f]
    eq = sorted({x["equipo_id"] for x in f if x["equipo_id"] is not None})
    sin_eq = [x for x in f if x["equipo_id"] is None]
    meses = sorted({x["mes"] for x in f})
    ap = sum(x["aprobadas"] for x in f)
    re = sum(x["rechazadas"] for x in f)
    ordenado = sorted(f, key=lambda x: x["proporcion_aprobadas"])
    por_equipo = {}
    for x in f:
        a = por_equipo.setdefault(x["equipo_id"] or "(sin equipo)", [0, 0])
        a[0] += x["aprobadas"]
        a[1] += x["rechazadas"]
    prop_eq = {e: r4(a / (a + r)) for e, (a, r) in sorted(por_equipo.items())}
    return {"equipos": len(eq), "filas_sin_equipo": sin_eq, "filas": len(f), "meses": len(meses), "primer_mes": meses[0], "ultimo_mes": meses[-1],
            "total_aprobadas": ap, "total_rechazadas": re, "proporcion_global": r4(ap / (ap + re)),
            "proporcion": {"min": r4(min(pr)), "mediana": r4(stt.median(pr)), "max": r4(max(pr)), "rango": r4(max(pr) - min(pr))},
            "grupo_min": {k: ordenado[0][k] for k in ("equipo_id", "mes", "aprobadas", "rechazadas", "proporcion_aprobadas")},
            "grupo_max": {k: ordenado[-1][k] for k in ("equipo_id", "mes", "aprobadas", "rechazadas", "proporcion_aprobadas")},
            "proporcion_por_equipo_todos_los_meses": prop_eq,
            "revisiones_por_grupo": {"min": min(x["aprobadas"] + x["rechazadas"] for x in f),
                                     "max": max(x["aprobadas"] + x["rechazadas"] for x in f)}}


def resumen_q3(f):
    return {"filas": [{k: (r4(v) if isinstance(v, float) else v) for k, v in x.items()} for x in f],
            "total_n": sum(x["n"] for x in f),
            "media_mas_alta": max(f, key=lambda x: x["dur_media_s"]), "media_mas_baja": min(f, key=lambda x: x["dur_media_s"]),
            "p95_mas_alto": max(f, key=lambda x: x["dur_p95_s"])["dur_p95_s"], "p95_mas_bajo": min(f, key=lambda x: x["dur_p95_s"])["dur_p95_s"],
            "cociente_p95_sobre_media": {"min": r4(min(x["dur_p95_s"] / x["dur_media_s"] for x in f)),
                                         "max": r4(max(x["dur_p95_s"] / x["dur_media_s"] for x in f))}}


def resumen_q4(f):
    pr = [x["proporcion_rechazadas"] for x in f]
    clientes = sorted({x["cliente_id"] for x in f if x["cliente_id"] is not None})
    rj = sum(x["rechazadas"] for x in f)
    ap = sum(x["aprobadas"] for x in f)
    o = sorted(f, key=lambda x: x["proporcion_rechazadas"])
    por_cli = {}
    for x in f:
        a = por_cli.setdefault(x["cliente_id"] or "(sin cliente)", [0, 0])
        a[0] += x["rechazadas"]
        a[1] += x["aprobadas"]
    cl = {c: {"rechazadas": a, "aprobadas": b, "proporcion_rechazadas": r4(a / (a + b))} for c, (a, b) in sorted(por_cli.items())}
    return {"clientes": len(clientes), "pares_cliente_proyecto": len(f), "total_rechazadas": rj, "total_aprobadas": ap,
            "proporcion_global_rechazadas": r4(rj / (rj + ap)),
            "proporcion": {"min": r4(min(pr)), "mediana": r4(stt.median(pr)), "max": r4(max(pr))},
            "mayor_rechazo": [{k: x[k] for k in ("cliente_id", "proyecto_id", "rechazadas", "aprobadas", "proporcion_rechazadas")} for x in o[::-1][:3]],
            "menor_rechazo": [{k: x[k] for k in ("cliente_id", "proyecto_id", "rechazadas", "aprobadas", "proporcion_rechazadas")} for x in o[:3]],
            "clientes_con_100_o_mas_revisiones": sum(1 for v in cl.values() if v["rechazadas"] + v["aprobadas"] >= 100),
            "cliente_mayor_rechazo_n>=100": [dict(cliente_id=c, **v) for c, v in sorted(
                ((c, v) for c, v in cl.items() if v["rechazadas"] + v["aprobadas"] >= 100), key=lambda kv: -kv[1]["proporcion_rechazadas"])[:3]],
            "cliente_menor_rechazo_n>=100": [dict(cliente_id=c, **v) for c, v in sorted(
                ((c, v) for c, v in cl.items() if v["rechazadas"] + v["aprobadas"] >= 100), key=lambda kv: kv[1]["proporcion_rechazadas"])[:3]],
            "proporcion_por_cliente": {"min": r4(min(v["proporcion_rechazadas"] for v in cl.values())),
                                       "mediana": r4(stt.median(v["proporcion_rechazadas"] for v in cl.values())),
                                       "max": r4(max(v["proporcion_rechazadas"] for v in cl.values()))}}


def resumen_q5(f):
    return {"filas": [{k: (r4(v) if isinstance(v, float) else v) for k, v in x.items()} for x in f],
            "total_n": sum(x["n"] for x in f), "clientes_distintos": len({x["cliente_id"] for x in f}),
            "p50_s": {"min": min(x["p50_s"] for x in f), "max": max(x["p50_s"] for x in f)},
            "p95_s": {"min": min(x["p95_s"] for x in f), "max": max(x["p95_s"] for x in f)},
            "pct_incumple_sla": {"min": r4(min(x["pct_incumple_sla"] for x in f)), "max": r4(max(x["pct_incumple_sla"] for x in f)),
                                 "mediana": r4(stt.median(x["pct_incumple_sla"] for x in f))},
            "mayor_incumplimiento": [x for x in sorted(f, key=lambda x: -x["pct_incumple_sla"])[:3]],
            "menor_incumplimiento_n>=100": [x for x in sorted((x for x in f if x["n"] >= 100), key=lambda x: x["pct_incumple_sla"])[:3]],
            "p50_mediana_clientes_s": r4(stt.median(x["p50_s"] for x in f)), "p95_mediana_clientes_s": r4(stt.median(x["p95_s"] for x in f)),
            "clientes_con_pct_incumple_mayor_a_10": sum(1 for x in f if x["pct_incumple_sla"] > 10),
            "global_ponderado_pct_incumple": r4(sum(x["n"] * x["pct_incumple_sla"] for x in f) / sum(x["n"] for x in f))}


RES = {"q1": resumen_q1, "q2": resumen_q2, "q3": resumen_q3, "q4": resumen_q4, "q5": resumen_q5}


def main():
    ev = C.Evidencia(
        "p5_resultados", "P5b. Resultados completos de Q1-Q5 sobre el conjunto principal",
        "Archivar los cuerpos completos de las respuestas de Q1-Q5 (conjunto principal) y el tiempo de cada consulta, para explicar "
        "las mediciones (enunciado §8) con valores que se puedan verificar en los archivos.",
        {"ruta": "GET /analitica/{q}?conjunto=principal vía gateway (localhost:8000)",
         "medicion": "tiempo_s = medido por el servicio analitica; una ejecución de calentamiento y una medida por consulta (sin reiniciar analitica, "
                     "sesión Spark ya creada: no es ejecución fría)",
         "spark": "master standalone, 1 worker (stack tal cual estaba levantado)",
         "nota": "el lago principal incluye los eventos de las pruebas anteriores; no se compactó ni modificó antes de medir"})
    c = C.cliente(900)
    lago = cuenta_lago(C.LAGO)
    meses = {os.path.basename(d): len(glob.glob(d + "/*.parquet")) for d in sorted(glob.glob(C.LAGO + "/eventos/mes=*"))}
    ev.paso("Estado del lago principal al medir (lectura directa con pyarrow)", {**lago, "archivos_por_mes": meses})
    _, salud = C.esperar_todo_ok(60)
    ev.paso("Salud", C.resumen_salud(salud))
    cal = {}
    for q in QS:
        r = c.get(f"/analitica/{q}", params={"conjunto": "principal"})
        cal[q] = {"http": r.status_code, "tiempo_s": r.json().get("tiempo_s") if r.status_code == 200 else None,
                  "n_ejecucion": r.json().get("n_ejecucion") if r.status_code == 200 else None}
    ev.paso("Calentamiento (cuerpos no archivados)", cal)
    res, tiempos, resum = {}, {}, {}
    for q in QS:
        t0 = time.time()
        r = c.get(f"/analitica/{q}", params={"conjunto": "principal"})
        pared = round((time.time() - t0) * 1000)
        if r.status_code != 200:
            ev.paso(f"{q} FALLO", {"http": r.status_code, "cuerpo": r.text[:300]})
            continue
        d = r.json()
        ruta = f"p5/resultados_principal_{q}.json"
        with open(os.path.join(C.EVIDENCE, ruta), "w", encoding="utf8") as fh:
            json.dump(d, fh, indent=1, ensure_ascii=False)
        ev.archivo_extra("evidence/" + ruta)
        tiempos[q] = {"tiempo_s": d["tiempo_s"], "pared_cliente_ms": pared, "n_filas": d["n_filas"], "n_ejecucion": d["n_ejecucion"]}
        resum[q] = RES[q](d["filas"])
        ev.paso(f"{q}: {d['n_filas']} filas en {d['tiempo_s']} s", {"archivo": "evidence/" + ruta, **tiempos[q]})
    lago_fin = cuenta_lago(C.LAGO)
    import pyarrow.compute as pc
    t = C.lago_dataset().to_table(columns=["ts", "estado", "proyecto_id"])
    dia = pc.strftime(t["ts"], "%Y-%m-%d")
    mx = pc.max(t["ts"]).as_py().isoformat()
    ult = sorted(set(dia.to_pylist()))[-4:]
    por_dia = {}
    for dd in ult:
        s_ = t.filter(pc.equal(dia, dd))
        cnt_ = {}
        for e in s_["estado"].to_pylist():
            cnt_[e] = cnt_.get(e, 0) + 1
        por_dia[dd] = {"eventos": s_.num_rows, "proyectos_distintos": len(set(s_["proyecto_id"].to_pylist())), "por_estado": dict(sorted(cnt_.items()))}
    ev.paso("Eventos crudos por día UTC en los últimos 4 días del lago (lectura directa, para explicar el último día de Q1)",
            {"max_ts": mx, "por_dia": por_dia})
    ev.paso("Lago al terminar", lago_fin)
    ev.resultado(lago_al_medir=lago, lago_al_terminar=lago_fin, max_ts_lago=mx, ultimos_dias_lago=por_dia,
                 nota_contar="el gateway no reenvía contar=1; las filas únicas se toman del lago (event_id_distintos)",
                 tiempos=tiempos, resumen=resum)
    ev.cerrar(
        "Los cuerpos completos de Q1-Q5 sobre el conjunto principal quedan archivados en evidence/p5/resultados_principal_q*.json; "
        "las cifras citadas en el informe salen de `resultado.resumen` (calculado con las filas archivadas, sin Spark) y los tiempos de "
        "`resultado.tiempos` (medidos por el servicio). Una sola ejecución medida por consulta tras calentamiento: es un valor puntual, no una "
        "mediana (para la dispersión ver P5). Los datos del principal incluyen los eventos de pruebas, por lo que los totales no coinciden con los ~800 000 del generador.")


if __name__ == "__main__":
    main()
