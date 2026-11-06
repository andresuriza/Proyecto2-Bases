"""Configuración leída del entorno (ver CONTRATOS.md)."""
import os


def env(nombre: str, defecto: str | None = None) -> str | None:
    return os.environ.get(nombre, defecto)


def env_int(nombre: str, defecto: int) -> int:
    try:
        return int(os.environ.get(nombre, defecto))
    except ValueError:
        return defecto


MONGO_URI = env("MONGO_URI", "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0")
MONGO_W = env("MONGO_W", "majority")
VALKEY_URL = env("VALKEY_URL", "redis://valkey:6379/0")
LOCK_TTL_MS = env_int("LOCK_TTL_MS", 120000)
DATALAKE_DIR = env("DATALAKE_DIR", "/datalake")
SPARK_MASTER = env("SPARK_MASTER", "spark://spark-master:7077")
