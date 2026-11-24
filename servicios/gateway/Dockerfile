# Contexto de build: ./servicios (ver CONTRATOS.md, sección Build)
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY gateway/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY common /app/common
COPY gateway /app/gateway
EXPOSE 8000
CMD ["uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
