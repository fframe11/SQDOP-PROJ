@echo off
setlocal enabledelayedexpansion

rem -------------------------------------------------
rem Run full system startup and data source test
rem -------------------------------------------------

rem Create a log file
set LOG_FILE=%~dp0test_results_%RANDOM%.log

rem Step 1: Wait for Docker daemon to be ready
:wait_docker
docker ps >nul 2>&1
if %errorlevel% neq 0 (
  timeout /t 5 >nul
  goto wait_docker
)
echo Docker daemon is running.
rem Step 1: Start the system
call "%~dp0..\start_system.bat" >> "%LOG_FILE%" 2>&1
if %errorlevel% neq 0 (
  echo [ERROR] start_system.bat failed with exit code %errorlevel% >> "%LOG_FILE%"
  exit /b %errorlevel%
) else (
  echo [INFO] start_system.bat completed successfully >> "%LOG_FILE%"
)

rem Step 2: Run data source test (dataset & API)
rem The test_data_source.bat script already handles prompting for dataset vs API.
call "%~dp0..\test_data_source.bat" >> "%LOG_FILE%" 2>&1
if %errorlevel% neq 0 (
  echo [ERROR] test_data_source.bat failed with exit code %errorlevel% >> "%LOG_FILE%"
  exit /b %errorlevel%
) else (
  echo [INFO] test_data_source.bat completed successfully >> "%LOG_FILE%"
)

rem Step 3: Success indicator
echo ------------------------------------------------- >> "%LOG_FILE%"
echo ALL TESTS PASSED >> "%LOG_FILE%"
exit /b 0
