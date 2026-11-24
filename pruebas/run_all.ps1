# Ejecuta todas las pruebas P1..P8 en orden (secuencial: comparten el stack) y genera las gráficas.
# Requisitos: Docker Desktop con el stack levantado (`docker compose up -d`), y Python 3.10+ en el host.
# Uso (PowerShell, desde la raíz del repositorio):  .\pruebas\run_all.ps1 [-Venv C:\p2work\venv] [-SaltarGrande]
param(
    [string]$Venv = "C:\p2work\venv",
    [switch]$SaltarGrande
)
$ErrorActionPreference = "Stop"
$env:PYTHONIOENCODING = "utf-8"
if (-not (Test-Path "$Venv\Scripts\python.exe")) {
    python -m venv $Venv
    & "$Venv\Scripts\python.exe" -m pip install -q httpx numpy pyarrow matplotlib
}
$py = "$Venv\Scripts\python.exe"
function Paso($nombre, $args_) {
    Write-Host "=== $nombre ===" -ForegroundColor Cyan
    & $py @args_
    if ($LASTEXITCODE -ne 0) { Write-Host "FALLO en $nombre (rc=$LASTEXITCODE)" -ForegroundColor Red }
}
docker compose up -d
Paso "entorno"  @("pruebas/entorno.py")
Paso "P1 API y salud"        @("pruebas/p1_api.py")
Paso "P2 concurrencia"       @("pruebas/p2_concurrencia.py")
Paso "P3 sesion"             @("pruebas/p3_sesion.py")
$p5 = @("pruebas/p5_analitica.py"); if ($SaltarGrande) { $p5 += "--saltar-grande" }
Paso "P5 analitica"          $p5
Paso "P5b resultados Q1-Q5 (principal)" @("pruebas/p5_resultados.py")
Paso "P4 historial"          @("pruebas/p4_historial.py")
Paso "P4 frescura hasta Q1"  @("pruebas/p4_frescura_q1.py")
Paso "P6 carga"              @("pruebas/p6_carga.py")
Paso "P8 fallas"             @("pruebas/p8_fallas.py")
Paso "graficas"              @("pruebas/graficas.py")
Paso "resumen"               @("pruebas/resumen.py")
Paso "indice"                @("pruebas/indice.py")
Write-Host "Evidencia en evidence/ (ver evidence/README.md y evidence/RESUMEN.md)" -ForegroundColor Green
