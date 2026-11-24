"""Consultas analíticas Q1-Q5 sobre el datalake Parquet (sólo lectura).

Módulo autónomo (sin FastAPI/Valkey): se importa desde el servicio y también se
ejecuta con `spark-submit consultas.py --q q1 ...` para las pruebas P5 (frío vs
caliente, 1 vs 2 trabajadores). Sólo DataFrame API (sin UDF de Python), por lo que
la diferencia de versión de Python entre driver y workers es irrelevante.

Convenciones (ver CONTRATOS.md):
  * Se deduplica por event_id antes de cada consulta.
  * Duraciones en segundos. Percentiles = percentile_approx con accuracy 100000
    (exacto para N <= 100000: definición "nearest-rank", elemento de índice
    ceil(p*N)-1 de la lista ordenada).
  * Zona horaria de sesión UTC: día = fecha UTC de ts; mes = partición `mes`.
"""
import argparse
import json
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

ACC = 100000
CONSULTAS = ("q1", "q2", "q3", "q4", "q5")

ESQUEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("ts", T.TimestampType()),
    T.StructField("tarea_id", T.StringType()),
    T.StructField("proyecto_id", T.StringType()),
    T.StructField("cliente_id", T.StringType()),
    T.StructField("batch_id", T.StringType()),
    T.StructField("equipo_id", T.StringType()),
    T.StructField("anotador_id", T.StringType()),
    T.StructField("revisor_id", T.StringType()),
    T.StructField("modalidad", T.StringType()),
    T.StructField("tipo_tarea", T.StringType()),
    T.StructField("estado", T.StringType()),
    T.StructField("motivo", T.StringType()),
    T.StructField("version", T.IntegerType()),
    T.StructField("tomada_en", T.TimestampType()),
    T.StructField("sla_horas", T.IntegerType()),
    T.StructField("mes", T.StringType()),  # columna de partición (directorio mes=AAAA-MM)
])


def crear_spark(master=None, app="analitica", extra=None):
    """SparkSession con la configuración del proyecto (todo configurable por entorno)."""
    master = master or os.environ.get("SPARK_MASTER", "local[*]")
    b = (SparkSession.builder.master(master).appName(app)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
         .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false"))
    if not master.startswith("local"):
        host = os.environ.get("SPARK_DRIVER_HOST", "analitica")
        b = (b.config("spark.driver.host", host)
             .config("spark.driver.bindAddress", "0.0.0.0")
             .config("spark.driver.port", os.environ.get("SPARK_DRIVER_PORT", "7078"))
             .config("spark.blockManager.port", os.environ.get("SPARK_BLOCKMANAGER_PORT", "7079")))
        if os.environ.get("SPARK_CORES_MAX"):
            b = b.config("spark.cores.max", os.environ["SPARK_CORES_MAX"])
    for k, v in (extra or {}).items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


# Optimización medida en P5: aplicar el filtro de cada consulta ANTES de deduplicar por event_id
# (los duplicados son copias idénticas del mismo evento, así que filtrar antes o después da el mismo
# resultado) para que el shuffle de la deduplicación mueva sólo las filas relevantes.
# OPT_FILTRO_PRIMERO=0 restablece el orden original (deduplicar todo y luego filtrar).
FILTRO_PRIMERO = os.environ.get("OPT_FILTRO_PRIMERO", "1") == "1"


def leer(spark, ruta, dedup=True, filtro=None):
    """Lee `ruta` (= <lago>/eventos) con esquema explícito (evita inferir esquema).
    `filtro` (opcional) es la condición de la consulta; se combina con la deduplicación por event_id."""
    df = spark.read.schema(ESQUEMA).option("basePath", ruta).parquet(ruta)
    if filtro is not None and (FILTRO_PRIMERO or not dedup):
        df = df.filter(filtro)
        filtro = None
    if dedup:
        df = df.dropDuplicates(["event_id"])
    if filtro is not None:
        df = df.filter(filtro)
    return df


def _rev_cond():
    return F.col("estado").isin("aprobada", "rechazada")


def q1(spark, ruta):
    """Aprobadas por día y modalidad en los últimos 90 días respecto de max(ts) del conjunto.
    Se calcula max(ts) primero (lectura de una sola columna) y se aplica el filtro como
    literal junto con `mes >= mes(corte)`, de modo que Spark pode particiones antiguas."""
    max_ts = leer(spark, ruta, dedup=False).agg(F.max("ts")).collect()[0][0]
    if max_ts is None:
        return spark.createDataFrame([], "dia string, modalidad string, aprobadas long")
    corte = F.lit(max_ts) - F.expr("INTERVAL 90 DAYS")
    mes_corte = F.date_format(corte, "yyyy-MM")
    df = leer(spark, ruta, filtro=(F.col("estado") == "aprobada") & (F.col("mes") >= mes_corte)
                              & (F.col("ts") > corte))
    return (df
            .groupBy(F.date_format("ts", "yyyy-MM-dd").alias("dia"), "modalidad")
            .agg(F.count("*").alias("aprobadas"))
            .orderBy("dia", "modalidad"))


def q2(spark, ruta):
    """Aprobadas / (aprobadas + rechazadas) por equipo y mes."""
    df = leer(spark, ruta, filtro=_rev_cond())
    return (df.groupBy("equipo_id", "mes")
            .agg(F.sum((F.col("estado") == "aprobada").cast("int")).alias("aprobadas"),
                 F.sum((F.col("estado") == "rechazada").cast("int")).alias("rechazadas"))
            .withColumn("proporcion_aprobadas",
                        F.col("aprobadas") / (F.col("aprobadas") + F.col("rechazadas")))
            .orderBy("equipo_id", "mes"))


def _dur(ts_col="ts"):
    return F.col(ts_col).cast("double") - F.col("tomada_en").cast("double")


def q3(spark, ruta):
    """Duración (anotada.ts - tomada_en) en segundos: media y p95 por modalidad y tipo_tarea."""
    df = leer(spark, ruta, filtro=(F.col("estado") == "anotada") & F.col("tomada_en").isNotNull())
    return (df.withColumn("dur", _dur())
            .groupBy("modalidad", "tipo_tarea")
            .agg(F.count("*").alias("n"), F.avg("dur").alias("dur_media_s"),
                 F.percentile_approx("dur", 0.95, ACC).alias("dur_p95_s"))
            .orderBy("modalidad", "tipo_tarea"))


def q4(spark, ruta):
    """Rechazadas / (aprobadas + rechazadas) por cliente y proyecto."""
    df = leer(spark, ruta, filtro=_rev_cond())
    return (df.groupBy("cliente_id", "proyecto_id")
            .agg(F.sum((F.col("estado") == "rechazada").cast("int")).alias("rechazadas"),
                 F.sum((F.col("estado") == "aprobada").cast("int")).alias("aprobadas"))
            .withColumn("proporcion_rechazadas",
                        F.col("rechazadas") / (F.col("aprobadas") + F.col("rechazadas")))
            .orderBy("cliente_id", "proyecto_id"))


def q5(spark, ruta):
    """aprobada.ts - tomada_en: p50 y p95 (s) por cliente y % de tareas con duración > sla_horas."""
    df = leer(spark, ruta, filtro=(F.col("estado") == "aprobada") & F.col("tomada_en").isNotNull())
    return (df.withColumn("dur", _dur())
            .withColumn("incumple", (F.col("dur") > F.col("sla_horas") * 3600).cast("int"))
            .groupBy("cliente_id")
            .agg(F.count("*").alias("n"),
                 F.percentile_approx("dur", 0.5, ACC).alias("p50_s"),
                 F.percentile_approx("dur", 0.95, ACC).alias("p95_s"),
                 (F.avg("incumple") * 100).alias("pct_incumple_sla"))
            .orderBy("cliente_id"))


def buscar_evento(spark, ruta, event_id):
    """Busca un event_id en el lago (prueba P4: segundos entre el HTTP 200 y la visibilidad en Spark).
    Siempre lee los archivos Parquet en ese momento (sin caché) y reporta filas crudas y deduplicadas."""
    t0 = time.perf_counter()
    filas = (leer(spark, ruta, dedup=False).filter(F.col("event_id") == event_id)
             .select("event_id", "estado", "tarea_id", "ts").collect())
    return {"event_id": event_id, "filas_crudas": len(filas), "filas_dedup": len({r["event_id"] for r in filas}),
            "estado": filas[0]["estado"] if filas else None, "tiempo_s": round(time.perf_counter() - t0, 3)}


FUNCIONES = {"q1": q1, "q2": q2, "q3": q3, "q4": q4, "q5": q5}


def plan_formateado(spark, df):
    """explain('formatted') como string (tras ejecutar, con AQE muestra el plan final)."""
    modo = spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")
    return df._jdf.queryExecution().explainString(modo)


def _limpio(v):
    if isinstance(v, float):
        return round(v, 6)
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if hasattr(v, "__float__") and not isinstance(v, (int, bool)):
        return round(float(v), 6)
    return v


def ejecutar(spark, q, ruta, explain=False, contar=False):
    """Ejecuta una consulta y devuelve filas + tiempo. Reintenta una vez si un archivo
    desapareció por una compactación concurrente (FileNotFound al leer)."""
    if q not in FUNCIONES:
        raise ValueError(f"consulta desconocida: {q}")
    intento = 0
    while True:
        try:
            t0 = time.perf_counter()
            df = FUNCIONES[q](spark, ruta)
            filas = df.collect()
            dt = time.perf_counter() - t0
            break
        except Exception as e:
            intento += 1
            m = str(e)
            if intento < 2 and ("FileNotFound" in m or "No such file" in m or "does not exist" in m):
                continue
            raise
    cols = df.columns
    res = {"filas": [{c: _limpio(r[c]) for c in cols} for r in filas],
           "n_filas": len(filas), "tiempo_s": round(dt, 3), "reintentos": intento}
    if contar:
        res["eventos_leidos"] = leer(spark, ruta).count()
    if explain:
        res["plan"] = plan_formateado(spark, df)
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description="Ejecuta Q1-Q5 (para spark-submit)")
    ap.add_argument("--q", default="all", help="q1..q5 o all")
    ap.add_argument("--ruta", default="/datalake/eventos")
    ap.add_argument("--repeticiones", type=int, default=1)
    ap.add_argument("--explain", action="store_true")
    ap.add_argument("--salida", default="", help="archivo JSON de salida")
    ap.add_argument("--sin-filas", action="store_true", help="omitir filas en la salida")
    a = ap.parse_args(argv)
    t0 = time.perf_counter()
    spark = crear_spark(app="consultas-cli")
    t_init = time.perf_counter() - t0
    qs = CONSULTAS if a.q == "all" else (a.q,)
    out = {"master": spark.sparkContext.master, "init_sesion_s": round(t_init, 3), "ejecuciones": []}
    for rep in range(1, a.repeticiones + 1):
        for q in qs:
            r = ejecutar(spark, q, a.ruta, explain=a.explain and rep == 1)
            e = {"q": q, "rep": rep, "tiempo_s": r["tiempo_s"], "n_filas": r["n_filas"]}
            if not a.sin_filas:
                e["filas"] = r["filas"]
            if "plan" in r:
                e["plan"] = r["plan"]
            out["ejecuciones"].append(e)
            print(f"{q} rep={rep} {r['tiempo_s']}s filas={r['n_filas']}", file=sys.stderr)
    out["total_s"] = round(time.perf_counter() - t0, 3)
    txt = json.dumps(out, indent=1, ensure_ascii=False)
    if a.salida:
        with open(a.salida, "w", encoding="utf8") as fh:
            fh.write(txt)
    else:
        print(txt)
    spark.stop()


if __name__ == "__main__":
    main()
