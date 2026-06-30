@echo off
setlocal enabledelayedexpansion
title SDOQAP Installation & Environment Setup Tool
cls
cd /d "%~dp0..\.."

echo =======================================================================
echo   SDOQAP Platform Installer and Environment Setup
echo =======================================================================
echo   This script prepares and sets up the SDOQAP environment on your machine.
echo   Requirements: Docker Desktop, WSL2, and Windows OS.
echo =======================================================================
echo.

:: 1. Check Docker status
echo [1/4] Checking Docker Desktop status...
docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker Daemon is NOT running!
    echo Please make sure Docker Desktop on Windows is started and active.
    echo.
    pause
    exit /b 1
)
echo OK: Docker Daemon is active.
echo.

:: 2. Check WSL status
echo [2/4] Checking WSL2 environment...
wsl --status >nul 2>&1
if %errorlevel% neq 0 (
    echo [WARNING] Could not retrieve WSL status directly.
    echo Please make sure WSL2 is installed and enabled on Windows.
) else (
    echo OK: WSL2 is active.
)
echo.

:: 3. Generate default .env file if missing
echo [3/4] Checking environment configuration...
if not exist ".env" (
    echo Creating new .env file with current timezone configurations and ports...
    echo # Elasticsearch Config> .env
    echo ELASTICSEARCH_HOST=elasticsearch>> .env
    echo ELASTICSEARCH_PORT=9200>> .env
    echo.>> .env
    echo # Spark Cluster Config>> .env
    echo SPARK_MASTER_HOST=spark-master>> .env
    echo SPARK_MASTER_PORT=7077>> .env
    echo.>> .env
    echo # Observability UI Ports>> .env
    echo KIBANA_PORT=5601>> .env
    echo GRAFANA_PORT=3002>> .env
    echo.>> .env
    echo # n8n Orchestrator Port>> .env
    echo N8N_PORT=5678>> .env
    echo.>> .env
    echo # Serving API Port>> .env
    echo API_PORT=8002>> .env
    echo Environment config file created successfully.
) else (
    echo OK: Configuration file already exists.
)
echo.

:: 4. Build Docker Images
echo [4/4] Building Docker images (FastAPI serving layer, etc.)...
docker compose build
if %errorlevel% neq 0 (
    echo [ERROR] Docker build failed! Please check the output above.
    pause
    exit /b 1
)
echo.

echo =======================================================================
echo   Installation and Setup Completed Successfully!
echo =======================================================================
echo   You are now ready to run start_system.bat from the project root.
echo   Then run test_data_source.bat to test a CSV dataset or API URL.
echo =======================================================================
echo.
pause
exit /b 0
