# Contexto de build: ./servicios (ver CONTRATOS.md, sección Build)
# Base = misma imagen que los workers (Spark 3.5.3, Java 11, Python 3.8) para que driver y
# ejecutores coincidan exactamente. PySpark viene dentro de la imagen (/opt/spark/python).
FROM apache/spark:3.5.3
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip curl \
    && rm -rf /var/lib/apt/lists/*
COPY analitica/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt
ENV PYTHONUNBUFFERED=1 \
    SPARK_HOME=/opt/spark \
    PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3
WORKDIR /app
COPY analitica /app
RUN mkdir -p /tmp/spark-ivy && chown -R spark:spark /app /tmp/spark-ivy
USER spark
EXPOSE 8004 4040 7078 7079
CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8004", "--no-access-log"]
