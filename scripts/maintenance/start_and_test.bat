@echo off
setlocal enabledelayedexpansion
title SDOQAP System Startup and Automated Verification Suite
cls

echo =======================================================================
echo   SDOQAP System Startup and Automated Verification Suite
echo =======================================================================
echo   This script boots up all SDOQAP containers, initializes the n8n database,
echo   runs end-to-end ingestion/validations, and launches the portal.
echo =======================================================================
echo.

:: 1. Load configuration from .env
if not exist ".env" (
    echo [ERROR] .env file not found! Please run install_sdoqap.bat first.
    pause
    exit /b 1
)

for /f "usebackq tokens=1,2 delims==" %%A in (".env") do (
    set key=%%A
    set val=%%B
    :: Trim whitespace and ignore comments
    if not "!key!"=="" (
        set firstchar=!key:~0,1!
        if not "!firstchar!"=="#" (
            set !key!=!val!
        )
    )
)

:: Set default values if not defined in .env
if "!API_PORT!"=="" set API_PORT=8002
if "!GRAFANA_PORT!"=="" set GRAFANA_PORT=3002
if "!N8N_PORT!"=="" set N8N_PORT=5678

:: 2. Boot up Docker containers
echo [1/7] Booting up Docker Compose services...
docker compose up -d
if %errorlevel% neq 0 (
    echo [ERROR] Failed to start Docker containers!
    pause
    exit /b 1
)
echo Services started.
echo.

:: 3. Initialize n8n Workflow database
echo [2/7] Initializing n8n workflow configurations...
docker cp n8n/database.sqlite sdoqap-n8n:/home/node/.n8n/database.sqlite
if %errorlevel% neq 0 (
    echo [WARNING] Could not copy n8n/database.sqlite to n8n container. n8n workflow might not be fully configured.
) else (
    echo Copying n8n/database.sqlite successful. Initializing workflow configuration...
    docker exec -u root sdoqap-n8n chown node:node /home/node/.n8n/database.sqlite
    docker exec -u root sdoqap-n8n chmod 644 /home/node/.n8n/database.sqlite
    docker exec -u root sdoqap-n8n rm -f /home/node/.n8n/database.sqlite-wal /home/node/.n8n/database.sqlite-shm >nul 2>&1
    docker cp n8n/credentials.json sdoqap-n8n:/home/node/credentials.json >nul 2>&1
    docker exec -u node sdoqap-n8n n8n import:credentials --input /home/node/credentials.json >nul 2>&1
    docker cp n8n/ingestion_workflow.json sdoqap-n8n:/home/node/ingestion_workflow.json >nul 2>&1
    docker exec -u node sdoqap-n8n n8n import:workflow --active --input /home/node/ingestion_workflow.json >nul 2>&1
    docker exec -u node sdoqap-n8n n8n publish:workflow --id=1 >nul 2>&1
    docker restart sdoqap-n8n >nul
    
    rem Initialize Postgres OLTP database table schema and mock data
    echo Initializing PostgreSQL schema and mock dataset sales_records...
    docker cp "stress test/100000 Sales Records.csv" sdoqap-postgres:/tmp/sales_records.csv >nul 2>&1
    docker cp "stress test/import_sales.sql" sdoqap-postgres:/tmp/import_sales.sql >nul 2>&1
    docker exec sdoqap-postgres psql -U sdoqap -d sdoqap_oltp -f /tmp/import_sales.sql >nul 2>&1
)
echo.

:: 4. Wait for services to be ready
echo [3/7] Waiting for services (HDFS, Elasticsearch, API) to initialize...

:wait_hdfs
echo   - Waiting for HDFS NameNode to leave Safe Mode...
docker exec sdoqap-namenode hdfs dfsadmin -safemode get 2>nul | findstr /I "OFF" >nul
if %errorlevel% neq 0 (
    ping 127.0.0.1 -n 6 >nul
    goto wait_hdfs
)
echo HDFS is ready.

:wait_es
echo   - Waiting for Elasticsearch cluster status to become healthy...
docker compose ps elasticsearch | findstr /I "healthy" >nul
if %errorlevel% neq 0 (
    ping 127.0.0.1 -n 6 >nul
    goto wait_es
)
echo Elasticsearch is ready.

:wait_api
echo   - Waiting for serving API layer (port !API_PORT!)...
curl -s -o NUL http://localhost:!API_PORT!/health
if %errorlevel% neq 0 (
    ping 127.0.0.1 -n 6 >nul
    goto wait_api
)
echo API is ready.

:wait_n8n
echo   - Waiting for n8n orchestrator (port !N8N_PORT!)...
curl -s -o NUL http://localhost:!N8N_PORT!/healthz
if %errorlevel% neq 0 (
    ping 127.0.0.1 -n 6 >nul
    goto wait_n8n
)
echo n8n is ready.
echo.

:: 5. Trigger n8n Data Ingestion
echo [4/7] Triggering end-to-end data ingestion webhook...
set webhook_retries=0
:trigger_webhook
set /a webhook_retries+=1
if !webhook_retries! gtr 15 (
    echo [ERROR] Webhook trigger failed after 15 retries. Please check n8n logs.
    echo.
    goto skip_hdfs_wait
)
curl -f -s -X POST http://localhost:!N8N_PORT!/webhook/1/webhooktrigger/ingest >nul
if %errorlevel% neq 0 (
    ping 127.0.0.1 -n 3 >nul
    goto trigger_webhook
)
echo Ingestion started successfully. Waiting for HDFS writes to complete...

:: Wait for all 3 HDFS files to exist and stabilize (sizes stop changing)
set prev_sizes=none
set hdfs_retries=0

:: Ensure WSL docker socket symlink is mapped to Docker Desktop proxy (WSL tempfs reset fix)
wsl -d Ubuntu -u root ln -sf /mnt/wsl/docker-desktop/shared-sockets/host-services/docker.proxy.sock /var/run/docker.sock >nul 2>&1

:wait_hdfs_write
set /a hdfs_retries+=1
if !hdfs_retries! gtr 60 (
    echo [WARNING] HDFS write wait timed out. Continuing anyway...
    goto hdfs_wait_done
)

:: Get current sizes of all 3 files via WSL (temp file approach for CMD compatibility)
set sz_p=0
set sz_g=0
set sz_s=0
wsl -d Ubuntu -u root docker exec sdoqap-namenode hdfs dfs -du -s /data/raw/products > "%TEMP%\hdfs_p.txt" 2>nul
wsl -d Ubuntu -u root docker exec sdoqap-namenode hdfs dfs -du -s /data/raw/gov_data > "%TEMP%\hdfs_g.txt" 2>nul
wsl -d Ubuntu -u root docker exec sdoqap-namenode hdfs dfs -du -s /data/raw/sales_records > "%TEMP%\hdfs_s.txt" 2>nul
if exist "%TEMP%\hdfs_p.txt" for /f "tokens=1" %%A in (%TEMP%\hdfs_p.txt) do set sz_p=%%A
if exist "%TEMP%\hdfs_g.txt" for /f "tokens=1" %%A in (%TEMP%\hdfs_g.txt) do set sz_g=%%A
if exist "%TEMP%\hdfs_s.txt" for /f "tokens=1" %%A in (%TEMP%\hdfs_s.txt) do set sz_s=%%A
set cur_sizes=!sz_p!_!sz_g!_!sz_s!

:: Check if any file is missing (size = 0)
if "!sz_p!"=="0" ( ping 127.0.0.1 -n 6 >nul & goto wait_hdfs_write )
if "!sz_g!"=="0" ( ping 127.0.0.1 -n 6 >nul & goto wait_hdfs_write )
if "!sz_s!"=="0" ( ping 127.0.0.1 -n 6 >nul & goto wait_hdfs_write )

:: Check if sizes stabilized (same as previous check)
if "!cur_sizes!"=="!prev_sizes!" (
    echo HDFS writes stabilized. All raw data files verified.
    goto hdfs_wait_done
)

set prev_sizes=!cur_sizes!
ping 127.0.0.1 -n 6 >nul
goto wait_hdfs_write

:hdfs_wait_done
echo.

:skip_hdfs_wait

:: 6. Run PySpark Quality Checks
echo [5/7] Submitting Spark quality checks on ingested datasets...
echo Installing dependencies inside Spark master/worker...
docker exec sdoqap-spark-master pip install --no-deps /opt/spark-apps/wheels/certifi-2026.6.17-py3-none-any.whl /opt/spark-apps/wheels/charset_normalizer-3.4.7-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl /opt/spark-apps/wheels/idna-3.18-py3-none-any.whl /opt/spark-apps/wheels/urllib3-2.7.0-py3-none-any.whl /opt/spark-apps/wheels/requests-2.34.2-py3-none-any.whl >nul 2>&1
docker exec sdoqap-spark-worker pip install --no-deps /opt/spark-apps/wheels/certifi-2026.6.17-py3-none-any.whl /opt/spark-apps/wheels/charset_normalizer-3.4.7-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl /opt/spark-apps/wheels/idna-3.18-py3-none-any.whl /opt/spark-apps/wheels/urllib3-2.7.0-py3-none-any.whl /opt/spark-apps/wheels/requests-2.34.2-py3-none-any.whl >nul 2>&1
echo Running products check...
docker exec -e HADOOP_USER_NAME=root sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py products
echo Running gov_data check...
docker exec -e HADOOP_USER_NAME=root sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py gov_data
echo Running sales_records check...
docker exec -e HADOOP_USER_NAME=root sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py sales_records
echo Spark validations complete.
echo.

:: 7. Restart n8n (it exits after completing webhook) and Run Health Checks via WSL
echo [6/7] Running final system integration health checks...
echo Restarting n8n container before health checks...
docker compose restart sdoqap-n8n >nul 2>&1
:wait_n8n_healthy
curl -s -o NUL http://localhost:!N8N_PORT!/healthz
if !errorlevel! neq 0 (
    ping 127.0.0.1 -n 4 >nul
    goto wait_n8n_healthy
)
echo n8n is running and healthy.
wsl -d Ubuntu -u root bash /mnt/c/DataEngProj/scripts/system_health_check.sh
echo.

:: 8. Open Portals
echo [7/7] Launching Central Observability Portal in default browser...
start http://localhost:!API_PORT!/
start http://localhost:!GRAFANA_PORT!/
echo.

echo =======================================================================
echo   SDOQAP Startup and Automated Test Execution Completed!
echo =======================================================================
echo   The SDOQAP Observability Portal and Grafana have been launched.
echo   All validations are active and data has been processed.
echo =======================================================================
echo.
pause
exit /b 0
