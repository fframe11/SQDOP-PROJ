@echo off
setlocal enabledelayedexpansion

rem ------------------------------------------------------------
rem Verify Pipeline Script
rem ------------------------------------------------------------

rem Run the root startup script
pushd "%~dp0..\.." >nul
call start_system.bat
if %errorlevel% neq 0 (
    echo [ERROR] start_system.bat failed with exit code %errorlevel%.
    popd >nul
    exit /b %errorlevel%
)

rem Check HDFS raw data directory
wsl -d Ubuntu -u root docker exec sdoqap-namenode hdfs dfs -ls /data/raw >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] HDFS /data/raw directory missing or empty.
    exit /b 1
) else (
    echo [INFO] HDFS /data/raw directory exists.
)

rem Check Postgres sales_records count
for /f "tokens=*" %%A in ('docker exec sdoqap-postgres psql -U sdoqap -d sdoqap_oltp -t -c "SELECT COUNT(*) FROM sales_records;"') do set COUNT=%%A
if defined COUNT (
    echo [INFO] sales_records count: %COUNT%
) else (
    echo [WARNING] Could not retrieve sales_records count.
)

rem Check Spark logs for errors (last 50 lines)
docker logs sdoqap-spark-master --tail 50 | findstr /i "error"
if %errorlevel% equ 0 (
    echo [WARNING] Spark logs contain errors.
) else (
    echo [INFO] Spark logs look clean.
)


echo [SUCCESS] Verification completed.
popd >nul
exit /b 0
