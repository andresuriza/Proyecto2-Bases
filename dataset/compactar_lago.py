"""Compacta un lago Parquet generado (muchos archivos pequeños por mes -> 1 archivo por mes).

Utilidad de preparación del conjunto grande (eventos_grande), que el servicio historial no puede
tocar (se monta :ro). Para el lago principal se usa POST /compactar del historial.
No deduplica (los generadores producen event_id únicos); conserva el esquema y la partición mes=.
Publicación atómica: escribe en <lago>/_tmp y reemplaza con os.replace; luego borra los orígenes.

Uso: python dataset/compactar_lago.py --lago C:/p2work/datalake_grande
"""
import argparse
import glob
import os
import time

import pyarrow.dataset as ds
import pyarrow.parquet as pq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lago", required=True)
    ap.add_argument("--row-group", type=int, default=500_000)
    a = ap.parse_args()
    raiz = os.path.join(a.lago, "eventos")
    tmp = os.path.join(a.lago, "_tmp")
    os.makedirs(tmp, exist_ok=True)
    antes = despues = filas = 0
    t0 = time.time()
    for d in sorted(glob.glob(os.path.join(raiz, "mes=*"))):
        orig = sorted(glob.glob(os.path.join(d, "*.parquet")))
        antes += len(orig)
        if len(orig) <= 1:
            despues += len(orig)
            continue
        tabla = ds.dataset(orig, format="parquet").to_table()
        destino = os.path.join(d, f"part-compact-{int(time.time())}.parquet")
        t = os.path.join(tmp, os.path.basename(destino))
        pq.write_table(tabla, t, row_group_size=a.row_group, compression="snappy")
        os.replace(t, destino)
        for f in orig:
            os.remove(f)
        despues += 1
        filas += tabla.num_rows
        print(f"{os.path.basename(d)}: {len(orig)} archivos -> 1 ({tabla.num_rows} filas)", flush=True)
    print(f"archivos {antes} -> {despues}; filas compactadas {filas}; {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
