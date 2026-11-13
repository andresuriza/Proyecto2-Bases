"""Compara Q1-Q5 de analítica (conjunto=validacion) con datalake_validacion/esperado.json.

Uso: python dataset/comparar_validacion.py --url http://analitica:8004 --esperado datalake_validacion/esperado.json
(dentro de la red docker) o vía gateway: --url http://localhost:8000 --ruta /analitica/{q}
Sale con código 0 si las cinco coinciden (tolerancia 1e-6 en flotantes).
"""
import argparse
import json
import sys
import urllib.request


def get(url):
    with urllib.request.urlopen(url, timeout=300) as r:
        return json.loads(r.read())


def igual(a, b):
    if isinstance(a, float) or isinstance(b, float):
        return a is not None and b is not None and abs(float(a) - float(b)) < 1e-6
    return a == b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://analitica:8004")
    ap.add_argument("--ruta", default="/q/{q}")
    ap.add_argument("--esperado", default="datalake_validacion/esperado.json")
    ap.add_argument("--salida", default="")
    a = ap.parse_args()
    esp = json.load(open(a.esperado, encoding="utf8"))
    informe, ok_total = {}, True
    for q in ("q1", "q2", "q3", "q4", "q5"):
        r = get(f"{a.url}{a.ruta.format(q=q)}?conjunto=validacion")
        got, want = r["filas"], esp[q]
        ok = len(got) == len(want) and all(
            set(g) == set(w) and all(igual(g[k], w[k]) for k in w) for g, w in zip(got, want))
        informe[q] = {"coincide": ok, "filas_obtenidas": len(got), "filas_esperadas": len(want),
                      "tiempo_s": r.get("tiempo_s")}
        if not ok:
            informe[q]["obtenido"], informe[q]["esperado"] = got, want
        ok_total &= ok
        print(f"{q}: {'OK' if ok else 'DIFIERE'} ({len(got)} filas)")
    informe["todas_coinciden"] = ok_total
    if a.salida:
        json.dump(informe, open(a.salida, "w", encoding="utf8"), indent=1, ensure_ascii=False)
    sys.exit(0 if ok_total else 1)


if __name__ == "__main__":
    main()
