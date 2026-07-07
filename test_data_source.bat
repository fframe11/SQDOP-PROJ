@echo off
setlocal enabledelayedexpansion
title SDOQAP Dataset/API Test Runner
cls
cd /d "%~dp0"

echo =======================================================================
echo   SDOQAP Dataset/API Test Runner
echo =======================================================================
echo   Use this script after start_system.bat has started the platform.
echo.
echo   Dataset input:
echo     Put CSV files in this project folder, then type the file name.
echo     You can also put CSV files in user_inputs\datasets.
echo.
echo   API input:
echo     Select API mode and paste/type the API URL in this terminal.
echo =======================================================================
echo.

if not exist "user_inputs\datasets" mkdir "user_inputs\datasets"
if not exist "user_inputs\apis" mkdir "user_inputs\apis"

if not exist ".env" (
    echo [ERROR] .env file not found. Run scripts\maintenance\install_sdoqap.bat first.
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

call :check_services
if %errorlevel% neq 0 (
    pause
    exit /b 1
)

:menu
cls
echo =======================================================================
echo   Select Test Source
echo =======================================================================
echo   1. Test with local CSV dataset
echo   2. Test with API URL
echo   3. Test with Reddit Live Stream
echo   4. Open Central Portal
echo   5. Show HDFS raw datasets
echo   6. Exit
echo =======================================================================
set /p choice="Select option (1-6): "

if "%choice%"=="1" goto dataset_test
if "%choice%"=="2" goto api_test
if "%choice%"=="3" goto reddit_test
if "%choice%"=="4" goto open_portal
if "%choice%"=="5" goto show_hdfs
if "%choice%"=="6" exit /b 0

echo Invalid option.
pause
goto menu

:dataset_test
cls
echo =======================================================================
echo   Local CSV Dataset Test
echo =======================================================================
echo Place your CSV file in:
echo   this project folder ^(same level as this script^)
echo or:
echo   user_inputs\datasets
echo.
echo You may enter either:
echo   - file name only: orders.csv
echo   - file name from user_inputs\datasets: orders.csv
echo   - full path: C:\path\to\orders.csv
echo.
set /p table_name="Enter table name for HDFS/Spark (example: orders): "
if "%table_name%"=="" (
    echo [ERROR] Table name is required.
    pause
    goto menu
)

set /p dataset_file="Enter CSV file name or full path: "
if "%dataset_file%"=="" (
    echo [ERROR] CSV file is required.
    pause
    goto menu
)

set "input_file=%dataset_file%"
if not exist "%input_file%" set "input_file=user_inputs\datasets\%dataset_file%"

if not exist "%input_file%" (
    echo [ERROR] File not found: %dataset_file%
    echo Put the file in this project folder, put it in user_inputs\datasets, or enter a full path.
    pause
    goto menu
)

call :load_csv_to_hdfs "%table_name%" "%input_file%"
if %errorlevel% neq 0 (
    pause
    goto menu
)

call :run_spark_check "%table_name%"
pause
goto menu

:api_test
cls
echo =======================================================================
echo   API URL Test
echo =======================================================================
echo The script downloads the API response and converts JSON array/object data
echo to CSV when possible. The output is saved in:
echo   user_inputs\apis
echo.
echo Paste/type your API URL directly in this terminal.
echo.
set /p table_name="Enter table name for HDFS/Spark (example: api_orders): "
if "%table_name%"=="" (
    echo [ERROR] Table name is required.
    pause
    goto menu
)

set /p api_url="Enter API URL: "
if "%api_url%"=="" (
    echo [ERROR] API URL is required.
    pause
    goto menu
)

set "api_output=user_inputs\apis\%table_name%.csv"
set "SDOQAP_API_URL=%api_url%"
set "SDOQAP_API_OUTPUT=%api_output%"

echo Downloading API data...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$url=$env:SDOQAP_API_URL; $out=$env:SDOQAP_API_OUTPUT; $resp=Invoke-WebRequest -Uri $url -UseBasicParsing; $content=$resp.Content; $trim=$content.TrimStart(); if($trim.StartsWith('{') -or $trim.StartsWith('[')) { $json=$content | ConvertFrom-Json; $records=$null; if($json -is [array]) { $records=$json } elseif($json.result -and $json.result.records) { $records=$json.result.records } elseif($json.records) { $records=$json.records } elseif($json.data) { $records=$json.data } elseif($json.items) { $records=$json.items } else { $records=@($json) }; $records | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $out } else { Set-Content -Path $out -Value $content -Encoding UTF8 }"

if %errorlevel% neq 0 (
    echo [ERROR] API download/conversion failed.
    pause
    goto menu
)

if not exist "%api_output%" (
    echo [ERROR] API output file was not created.
    pause
    goto menu
)

echo API data saved to %api_output%
call :load_csv_to_hdfs "%table_name%" "%api_output%"
if %errorlevel% neq 0 (
    pause
    goto menu
)

call :run_spark_check "%table_name%"
pause
goto menu

:reddit_test
cls
echo =======================================================================
echo   Reddit Live Stream Test
echo =======================================================================
echo   This mode triggers real-time data scraping from Reddit and streams it
echo   into the SDOQAP data pipeline (Reddit -> Kafka -> Spark -> HDFS/ES).
echo.
docker ps --format "{{.Names}}" | findstr /I "kafka" >nul
if %errorlevel% neq 0 (
    echo [WARNING] Kafka service is not running.
    echo   This test requires a running Kafka broker to stream real-time data.
    echo   Please ensure your Kafka service is started, or proceed with simulation.
    echo.
)
set /p subreddits="Enter subreddits to scrape (example: python,bigdata): "
if "%subreddits%"=="" set subreddits=python

echo.
echo [INGEST] Spawning Reddit Live stream for subreddits '%subreddits%'...
echo [INGEST] Real-time stream will run for 10 seconds to show capability.
echo.

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Write-Host '[INGEST] Starting background Reddit stream ingestion...'; ^
   $ingestProc = Start-Process python -ArgumentList '-u scripts/reddit_stream.py %subreddits%' -NoNewWindow -PassThru; ^
   Write-Host '[SPARK] Starting background Spark streaming job...'; ^
   $sparkProc = Start-Process docker -ArgumentList 'exec -t sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/streaming_job.py' -NoNewWindow -PassThru; ^
   Write-Host '[SYSTEM] Ingestion and processing running in real-time...'; ^
   for ($i=40; $i -gt 0; $i--) { Write-Host \"[TIMER] $i seconds remaining...\"; Start-Sleep -Seconds 1; }; ^
   Write-Host '[SYSTEM] Terminating streaming jobs...'; ^
   try { Stop-Process -Id $ingestProc.Id -Force -ErrorAction SilentlyContinue } catch {}; ^
   try { Stop-Process -Id $sparkProc.Id -Force -ErrorAction SilentlyContinue } catch {}; ^
   Write-Host '[SYSTEM] Querying Elasticsearch for ingested Reddit data...'; ^
   $esResp = curl.exe -s -u 'elastic:sdoqap_secure' http://localhost:9200/reddit/_count; ^
   if ($esResp) { ^
       $json = $esResp | ConvertFrom-Json; ^
       Write-Host \"[SUCCESS] Documents indexed in ES 'reddit' index: $($json.count)\"; ^
   } else { ^
       Write-Host '[WARNING] Could not retrieve ES count.'; ^
   }"
echo.
pause
goto menu

:open_portal
start http://localhost/
goto menu

:show_hdfs
cls
echo =======================================================================
echo   HDFS Raw Datasets
echo =======================================================================
docker exec sdoqap-namenode hdfs dfs -ls /data/raw
echo.
pause
goto menu

:check_services
echo [CHECK] Verifying required services...
docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker daemon is not running.
    echo Please start Docker Desktop, then run start_system.bat.
    exit /b 1
)

docker ps --format "{{.Names}}" | findstr /I "sdoqap-namenode" >nul
if %errorlevel% neq 0 (
    echo [ERROR] sdoqap-namenode is not running. Run start_system.bat first.
    exit /b 1
)

docker ps --format "{{.Names}}" | findstr /I "sdoqap-spark-master" >nul
if %errorlevel% neq 0 (
    echo [ERROR] sdoqap-spark-master is not running. Run start_system.bat first.
    exit /b 1
)

docker ps --format "{{.Names}}" | findstr /I "sdoqap-elasticsearch" >nul
if %errorlevel% neq 0 (
    echo [ERROR] sdoqap-elasticsearch is not running. Run start_system.bat first.
    exit /b 1
)

curl -s -o NUL http://localhost:!API_PORT!/healthz
if %errorlevel% neq 0 (
    echo [ERROR] FastAPI portal is not healthy on port !API_PORT!.
    exit /b 1
)

echo Required services are running.
timeout /t 1 >nul
exit /b 0

:load_csv_to_hdfs
set "table=%~1"
set "file=%~2"
set "container_file=/tmp/%table%.csv"
set "hdfs_dir=/data/raw/%table%"
set "hdfs_file=%hdfs_dir%/%table%.csv"

echo.
echo [INGEST] Loading %file% into HDFS table '%table%'...
docker cp "%file%" sdoqap-namenode:%container_file%
if %errorlevel% neq 0 (
    echo [ERROR] Failed to copy file into NameNode container.
    exit /b 1
)

docker exec sdoqap-namenode hdfs dfs -mkdir -p %hdfs_dir%
docker exec sdoqap-namenode hdfs dfs -put -f %container_file% %hdfs_file%
if %errorlevel% neq 0 (
    echo [ERROR] Failed to put CSV file into HDFS.
    exit /b 1
)

docker exec sdoqap-namenode rm -f %container_file%

docker exec -t -e HADOOP_USER_NAME=root sdoqap-namenode hdfs dfs -chown -R spark:spark /data >nul 2>&1
docker exec -t -e HADOOP_USER_NAME=root sdoqap-namenode hdfs dfs -chmod -R 777 /data >nul 2>&1

echo HDFS raw file created:
docker exec sdoqap-namenode hdfs dfs -ls %hdfs_dir%
exit /b 0

:run_spark_check
set "table=%~1"
echo.
echo [SPARK] Installing dependencies inside Spark containers...
docker exec sdoqap-spark-master pip install --no-deps /opt/spark-apps/wheels/certifi-2026.6.17-py3-none-any.whl /opt/spark-apps/wheels/charset_normalizer-3.4.7-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl /opt/spark-apps/wheels/idna-3.18-py3-none-any.whl /opt/spark-apps/wheels/urllib3-2.7.0-py3-none-any.whl /opt/spark-apps/wheels/requests-2.34.2-py3-none-any.whl >nul 2>&1
docker exec sdoqap-spark-worker pip install --no-deps /opt/spark-apps/wheels/certifi-2026.6.17-py3-none-any.whl /opt/spark-apps/wheels/charset_normalizer-3.4.7-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl /opt/spark-apps/wheels/idna-3.18-py3-none-any.whl /opt/spark-apps/wheels/urllib3-2.7.0-py3-none-any.whl /opt/spark-apps/wheels/requests-2.34.2-py3-none-any.whl >nul 2>&1

echo [SPARK] Running quality check for table '%table%'...
docker exec sdoqap-spark-master spark-submit --master spark://spark-master:7077 --packages io.delta:delta-core_2.12:2.4.0 /opt/spark-apps/spark_quality_engine.py %table%
if %errorlevel% neq 0 (
    echo [ERROR] Spark quality check failed for table '%table%'.
    echo Check logs with: docker logs sdoqap-spark-master
    exit /b 1
)

echo.
echo [DONE] Quality check completed for '%table%'.
echo Open the central portal:
echo   http://localhost/
exit /b 0
