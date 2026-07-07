@echo off
setlocal enabledelayedexpansion
title SDOQAP System Startup
cls
cd /d "%~dp0"

echo =======================================================================
echo   SDOQAP System Startup
echo =======================================================================
echo   This script starts the platform only.
echo   It does NOT run dataset/API ingestion tests.
echo.
echo   After this finishes, run:
echo     test_data_source.bat
echo =======================================================================
echo.

if not exist ".env" (
    echo [ERROR] .env file not found.
    echo Please run scripts\maintenance\install_sdoqap.bat first.
    echo.
    pause
    exit /b 1
)

for /f "usebackq tokens=1,2 delims==" %%A in (".env") do (
    set key=%%A
    set val=%%B
    if not "!key!"=="" (
        set firstchar=!key:~0,1!
        if not "!firstchar!"=="#" (
            set !key!=!val!
        )
    )
)

if "!API_PORT!"=="" set API_PORT=8002
if "!GRAFANA_PORT!"=="" set GRAFANA_PORT=3002
if "!N8N_PORT!"=="" set N8N_PORT=5678
if "!KIBANA_PORT!"=="" set KIBANA_PORT=5601

echo [1/5] Checking Docker Desktop...
docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker daemon is not running.
    echo Please start Docker Desktop and try again.
    echo.
    pause
    exit /b 1
)
echo Docker is running.
echo.

echo [2/5] Starting Docker Compose services...
docker compose up -d
if %errorlevel% neq 0 (
    echo [ERROR] Failed to start Docker containers.
    echo.
    pause
    exit /b 1
)
echo Services started.
echo.

if not exist "n8n\credentials.json" (
    echo Creating default n8n/credentials.json for Postgres...
    (
        echo [
        echo   {
        echo     "id": "c711aa48-356f-4cd2-b67a-12908a85f401",
        echo     "name": "Postgres Local",
        echo     "type": "postgres",
        echo     "data": {
        echo       "host": "postgres",
        echo       "database": "sdoqap_oltp",
        echo       "user": "sdoqap",
        echo       "password": "sdoqap",
        echo       "port": 5432,
        echo       "ssl": "disable"
        echo     }
        echo   }
        echo ]
    ) > "n8n\credentials.json"
)

echo [3/5] Preparing optional n8n workflow import...
if exist "n8n\ingestion_workflow.json" (
    docker cp n8n/ingestion_workflow.json sdoqap-n8n:/home/node/ingestion_workflow.json >nul 2>&1
    docker exec -u node sdoqap-n8n n8n import:workflow --active --input /home/node/ingestion_workflow.json >nul 2>&1
    if !errorlevel! equ 0 (
        echo n8n workflow import completed.
    ) else (
        echo [WARNING] n8n workflow import was skipped or failed. You can still test data directly with test_data_source.bat.
    )
) else (
    echo [WARNING] n8n\ingestion_workflow.json not found. Skipping workflow import.
)

if exist "n8n\credentials.json" (
    docker cp n8n/credentials.json sdoqap-n8n:/home/node/credentials.json >nul 2>&1
    docker exec -u node sdoqap-n8n n8n import:credentials --input /home/node/credentials.json >nul 2>&1
    if !errorlevel! equ 0 (
        echo n8n credentials import completed.
    ) else (
        echo [WARNING] n8n credentials import failed. Configure credentials manually if you use n8n.
    )
) else (
    echo [INFO] n8n credentials file not found. This is expected for GitHub clones.
)
echo.

echo [4/5] Waiting for core services...

:wait_hdfs
echo   - Waiting for HDFS NameNode safe mode OFF...
docker exec sdoqap-namenode hdfs dfsadmin -safemode get 2>nul | findstr /I "OFF" >nul
if %errorlevel% neq 0 (
    timeout /t 5 >nul
    goto wait_hdfs
)
echo HDFS is ready.
echo Initializing HDFS data directories and permissions...
docker exec sdoqap-namenode hadoop fs -mkdir -p /data/raw /data/active /data/quarantine /data/profiles
docker exec sdoqap-namenode hadoop fs -chmod -R 777 /data
echo HDFS permissions initialized.
echo.

:wait_es
echo   - Waiting for Elasticsearch health...
docker compose ps elasticsearch | findstr /I "healthy" >nul
if %errorlevel% neq 0 (
    timeout /t 5 >nul
    goto wait_es
)
echo Elasticsearch is ready.
echo Setting up kibana_system password...
curl -s -u elastic:sdoqap_secure -X POST "http://localhost:9200/_security/user/kibana_system/_password" -H "Content-Type: application/json" -d "{\"password\":\"sdoqap_secure\"}" >nul 2>&1

:wait_api
echo   - Waiting for FastAPI portal on port !API_PORT!...
curl -s -o NUL http://localhost:!API_PORT!/health
if %errorlevel% neq 0 (
    timeout /t 5 >nul
    goto wait_api
)
echo FastAPI is ready.

:wait_n8n
echo   - Waiting for n8n on port !N8N_PORT!...
curl -s -o NUL http://localhost:!N8N_PORT!/healthz
if %errorlevel% neq 0 (
    timeout /t 5 >nul
    goto wait_n8n
)
echo n8n is ready.
echo.

echo [5/5] Opening platform portals...
start http://localhost/
start http://localhost:!GRAFANA_PORT!/

echo.
echo =======================================================================
echo   SDOQAP platform is running.
echo =======================================================================
echo   Central Portal: http://localhost/
echo   Grafana:        http://localhost:!GRAFANA_PORT!/  ^(admin / admin^)
echo   n8n:            http://localhost:!N8N_PORT!/
echo   HDFS NameNode:  http://localhost:9870/
echo   Spark Master:   http://localhost:8081/
echo   Elasticsearch:  http://localhost:9200/
echo   Kibana:         http://localhost:!KIBANA_PORT!/
echo.
echo   To run a data test, use:
echo     test_data_source.bat
echo =======================================================================
echo.
pause
exit /b 0
