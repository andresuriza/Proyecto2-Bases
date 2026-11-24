"""Escribe evidence/entorno.txt: máquina, versiones de Docker e imágenes, contenedores y recursos."""
import os
import platform
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comun import dc, guardar_texto, sh  # noqa: E402


def main():
    partes = [f"# Entorno de las pruebas ({datetime.now().isoformat(timespec='seconds')})\n"]

    def bloque(titulo, cmd):
        rc, out, err = sh(cmd, timeout=120)
        partes.append(f"\n## {titulo}\n$ {cmd if isinstance(cmd, str) else ' '.join(cmd)}\n{(out or err).strip()}\n")

    partes.append(f"host: {platform.platform()} ; python host {sys.version.split()[0]} ; cpu_logicos={os.cpu_count()}")
    bloque("CPU (wmic/PowerShell)", ["powershell", "-NoProfile", "-Command",
           "Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors,MaxClockSpeed | Format-List"])
    bloque("RAM del host", ["powershell", "-NoProfile", "-Command",
           "$m=(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory; \"RAM total: {0:N1} GiB\" -f ($m/1GB)"])
    bloque("docker version", ["docker", "version"])
    bloque("docker compose version", ["docker", "compose", "version"])
    bloque("docker info", ["docker", "info"])
    bloque("imágenes (compose images)", ["docker", "compose", "--profile", "tools", "images"])
    bloque("contenedores (docker ps)", ["docker", "ps", "--format", "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"])
    bloque("versiones: python servicios", ["docker", "compose", "exec", "-T", "gateway", "python", "-c",
           "import sys,fastapi,httpx,redis;print(sys.version);print('fastapi',fastapi.__version__,'httpx',httpx.__version__,'redis-py',redis.__version__)"])
    bloque("versión pymongo (registro)", ["docker", "compose", "exec", "-T", "registro", "python", "-c",
           "import pymongo;print('pymongo',pymongo.version)"])
    bloque("versión MongoDB", ["docker", "exec", "mongo1", "mongod", "--version"])
    bloque("versión Valkey", ["docker", "exec", "valkey", "valkey-server", "--version"])
    bloque("versión Spark/Java (analitica)", ["docker", "compose", "exec", "-T", "analitica", "bash", "-c",
           "java -version 2>&1; python3 -c 'import pyspark;print(\"pyspark\",pyspark.__version__)'"])
    bloque("docker stats (reposo)", ["docker", "stats", "--no-stream"])
    print(guardar_texto("entorno.txt", "".join(partes)))


if __name__ == "__main__":
    main()
