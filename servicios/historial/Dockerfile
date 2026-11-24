# Contexto de build: ./servicios (ver CONTRATOS.md, sección Build)
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY historial/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY common /app/common
COPY historial /app/historial
EXPOSE 8003
CMD ["uvicorn", "historial.main:app", "--host", "0.0.0.0", "--port", "8003", "--no-access-log"]
