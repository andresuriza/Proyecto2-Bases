# Imagen de herramientas (perfil `tools`): simulador, pruebas y cargadores de dataset.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN pip install httpx pymongo redis pyarrow pandas numpy tabulate matplotlib
WORKDIR /src
CMD ["sleep", "infinity"]
