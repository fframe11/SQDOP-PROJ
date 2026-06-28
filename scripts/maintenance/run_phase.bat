@echo off
setlocal enabledelayedexpansion
title SDOQAP Phased Execution Manager (16GB RAM Mode)
cls

:check_docker
echo Checking Docker Desktop status...
docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo =============================================================
    echo [ERROR] Docker Daemon is NOT running!
    echo =============================================================
    echo Please make sure Docker Desktop on Windows is started.
    echo Also ensure your WSL2 environment is initialized.
    echo.
    echo Press any key to retry status check...
    pause >nul
    cls
    goto check_docker
)

:menu
echo =======================================================================
echo   SDOQAP Phased Execution Manager (Resource-Optimized 16GB RAM Mode)
echo =======================================================================
echo   [Phase Execution]
echo     1. Phase 1: Ingestion (n8n + HDFS)
echo     2. Phase 2: Processing (HDFS + Spark + Elasticsearch)
echo     3. Phase 3: Serving ^& Observability (HDFS + ES + Kibana + Grafana + FastAPI)
echo.
echo   [Utility Actions]
echo     4. Setup HDFS directories and upload sample data (users.csv)
echo     5. Run PySpark Quality Check Engine manually
echo     6. View running containers and health status
echo     7. Stop all services (Clean down)
echo     8. Exit
echo =======================================================================
set /p choice="Select an option (1-8): "

if "%choice%"=="1" goto phase1
if "%choice%"=="2" goto phase2
if "%choice%"=="3" goto phase3
if "%choice%"=="4" goto setup_hdfs
if "%choice%"=="5" goto run_spark
if "%choice%"=="6" goto status
if "%choice%"=="7" goto stopall
if "%choice%"=="8" goto exit
echo Invalid selection. Please try again.
pause
cls
goto menu

:phase1
echo.
echo =======================================================================
echo [Phase 1] Starting Ingestion Services...
echo =======================================================================
echo Stopping non-ingestion services to save memory...
docker compose stop elasticsearch kibana grafana spark-master spark-worker api
echo Starting NameNode, DataNode, and n8n...
docker compose up -d namenode datanode n8n
echo.
echo Ingestion Phase is ready!
echo   - n8n Web UI: http://localhost:5678
echo   - HDFS Namenode UI: http://localhost:9870
echo.
pause
cls
goto menu

:phase2
echo.
echo =======================================================================
echo [Phase 2] Starting Distributed Processing Services...
echo =======================================================================
echo Stopping ingestion/serving services to save memory...
docker compose stop n8n kibana grafana api
echo Starting HDFS, Spark, and Elasticsearch...
docker compose up -d namenode datanode elasticsearch spark-master spark-worker
echo.
echo Waiting for Elasticsearch to become healthy (JVM startup)...
:wait_es
docker compose ps elasticsearch | findstr "healthy" >nul
if %errorlevel% neq 0 (
    echo   - Waiting for Elasticsearch status 'healthy'...
    timeout /t 5 >nul
    goto wait_es
)
echo Elasticsearch is healthy!
echo.
echo Processing Phase is ready!
echo   - Spark Master UI: http://localhost:8081
echo   - Elasticsearch Endpoint: http://localhost:9200
echo.
echo Tip: Use option 4 to populate HDFS and option 5 to run the PySpark Quality Engine.
echo.
pause
cls
goto menu

:phase3
echo.
echo =======================================================================
echo [Phase 3] Starting Serving and Observability Services...
echo =======================================================================
echo Stopping Spark processing engines to save memory...
docker compose stop n8n spark-master spark-worker
echo Starting HDFS, Elasticsearch, Kibana, Grafana, and FastAPI...
docker compose up -d namenode datanode elasticsearch kibana grafana api
echo.
echo Waiting for Elasticsearch to become healthy...
:wait_es_p3
docker compose ps elasticsearch | findstr "healthy" >nul
if %errorlevel% neq 0 (
    echo   - Waiting for Elasticsearch status 'healthy'...
    timeout /t 5 >nul
    goto wait_es_p3
)
echo Elasticsearch is healthy!
echo.
echo Starting Kibana and Grafana...
docker compose up -d kibana grafana
echo.
echo Serving and Observability Phase is ready!
echo   - FastAPI Serving Layer: http://localhost:8000
echo   - FastAPI Health Check: http://localhost:8000/health
echo   - Grafana UI: http://localhost:3000 (admin / admin)
echo   - Kibana UI: http://localhost:5601
echo.
pause
cls
goto menu

:setup_hdfs
echo.
echo =======================================================================
echo [Action] Initializing HDFS Directories ^& Mock Ingest
echo =======================================================================
:: Check if HDFS is running
docker compose ps namenode | findstr "Up" >nul
if %errorlevel% neq 0 (
    echo [ERROR] HDFS NameNode is NOT running! Please start Phase 1 or Phase 2 first.
    pause
    cls
    goto menu
)
echo Creating HDFS directory structure for 'users' table...
docker compose exec -T namenode hdfs dfs -mkdir -p /data/raw/users
docker compose exec -T namenode hdfs dfs -mkdir -p /data/active/users
docker compose exec -T namenode hdfs dfs -mkdir -p /data/quarantine/users
echo.
echo Copying mock data (users.csv) to HDFS container...
docker cp spark/users.csv sdoqap-namenode:/tmp/users.csv
echo Loading mock data into HDFS raw location...
docker compose exec -T namenode hdfs dfs -put -f /tmp/users.csv /data/raw/users/users.csv
echo.
echo HDFS Ingestion and Directory setup complete!
echo Verified raw data file in HDFS:
docker compose exec -T namenode hdfs dfs -ls /data/raw/users
echo.
pause
cls
goto menu

:run_spark
echo.
echo =======================================================================
echo [Action] Submitting PySpark Quality Validation Engine
echo =======================================================================
:: Check if Spark and ES are running
docker compose ps spark-master | findstr "Up" >nul
if %errorlevel% neq 0 (
    echo [ERROR] Spark is NOT running! Please start Phase 2 first.
    pause
    cls
    goto menu
)
docker compose ps elasticsearch | findstr "healthy" >nul
if %errorlevel% neq 0 (
    echo [ERROR] Elasticsearch is NOT running or not healthy! Please start Phase 2 first.
    pause
    cls
    goto menu
)
echo Installing Spark external dependencies (requests)...
docker compose exec -T spark-master pip install requests >nul 2>&1
docker compose exec -T spark-worker pip install requests >nul 2>&1
echo Submitting PySpark job...
docker compose exec -T spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py
echo.
echo PySpark quality check run finished! Check Elasticsearch for scorecard index logs.
echo.
pause
cls
goto menu

:status
echo.
echo =======================================================================
echo [Action] Current Container Status
echo =======================================================================
docker compose ps
echo.
pause
cls
goto menu

:stopall
echo.
echo =======================================================================
echo [Action] Stopping and Cleaning All Services
echo =======================================================================
docker compose down
echo.
echo All containers stopped and removed.
echo.
pause
cls
goto menu

:exit
echo.
echo Exiting SDOQAP Manager. Goodbye!
exit /b
