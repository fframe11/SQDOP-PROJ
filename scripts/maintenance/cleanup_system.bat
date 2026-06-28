@echo off
setlocal enabledelayedexpansion
title SDOQAP System Cool-Down and Cleanup Utility
cls

echo =======================================================================
echo   SDOQAP System Cool-Down and Cleanup Utility
echo =======================================================================
echo   This script prepares your laptop to run heavy big data workloads
echo   (Spark, ES, HDFS) without overheating, thermal throttling, or freezing.
echo =======================================================================
echo.

:menu
echo Select cleanup level:
echo   1. Quick Cool-Down (Stop containers + Release WSL RAM cache)
echo   2. Standard Clean (Stop containers + Apply CPU limit + Prune Docker cache + Clean Temp)
echo   3. Deep Clean (Includes Volume Prune - WARNING: Deletes HDFS/ES data)
echo   4. View WSL Virtual Disk Compaction Guide (Shrink SSD space)
echo   5. Exit
echo =======================================================================
set /p choice="Select an option (1-5): "

if "%choice%"=="1" goto quick_cool
if "%choice%"=="2" goto standard_clean
if "%choice%"=="3" goto deep_clean
if "%choice%"=="4" goto disk_guide
if "%choice%"=="5" goto exit
echo Invalid selection. Please try again.
pause
cls
goto menu

:quick_cool
echo.
echo =======================================================================
echo [Action] Quick Cool-Down
echo =======================================================================
echo Stopping all running Docker containers...
docker compose down
echo.
echo Reclaiming WSL2 RAM cache (Shutting down WSL)...
echo Windows will release WSL-allocated RAM back to the host system.
wsl --shutdown
echo.
echo Quick Cool-Down complete! Your RAM is fully released and CPU is idling.
echo.
pause
cls
goto menu

:standard_clean
echo.
echo =======================================================================
echo [Action] Standard System Optimization
echo =======================================================================
echo 1. Stopping Docker containers...
docker compose down
echo.
echo 2. Configuring WSL2 CPU limits to prevent CPU 100%% overheating...
if not exist "C:\Users\ffram\.wslconfig" (
    echo Creating .wslconfig with memory=10GB, swap=16GB, and processors=4 CPU limit...
    echo [wsl2]> "C:\Users\ffram\.wslconfig"
    echo memory=10GB>> "C:\Users\ffram\.wslconfig"
    echo swap=16GB>> "C:\Users\ffram\.wslconfig"
    echo processors=4>> "C:\Users\ffram\.wslconfig"
    echo CPU limit set successfully in new .wslconfig!
) else (
    findstr "processors" "C:\Users\ffram\.wslconfig" >nul
    if %errorlevel% neq 0 (
        echo Limiting WSL2 to 4 CPU processors (leaving 12 for Windows host)...
        echo processors=4>> "C:\Users\ffram\.wslconfig"
        echo CPU limit set successfully in .wslconfig!
    ) else (
        echo WSL2 CPU limit is already configured in .wslconfig.
    )
)
echo.
echo 3. Pruning unused Docker containers, networks, and build cache...
docker system prune -f
echo.
echo 4. Cleaning Windows temporary files to free up disk space...
del /q /f /s "%TEMP%\*" >nul 2>&1
echo Windows temporary files cleaned.
echo.
echo 5. Restarting WSL to apply changes...
wsl --shutdown
echo.
echo Standard System Optimization Complete!
echo CPU core limit (4 cores) and Memory cap (10GB) are ready to activate.
echo WSL has been shutdown; it will restart automatically when you launch Docker Desktop or run container commands.
echo.
pause
cls
goto menu

:deep_clean
echo.
echo =======================================================================
echo [WARNING] Deep Clean will wipe all HDFS datasets and Elasticsearch indexes!
echo =======================================================================
set /p confirm="Are you sure you want to proceed? (Y/N): "
if /i "%confirm%" neq "Y" (
    echo Deep clean cancelled.
    pause
    cls
    goto menu
)
echo.
echo Stopping and removing all Docker containers and volumes...
docker compose down -v
echo.
echo Pruning all Docker system caches and unused images...
docker system prune -a -f
echo.
echo Pruning Docker volumes...
docker volume prune -f
echo.
echo Reclaiming WSL2 RAM cache...
wsl --shutdown
echo.
echo Deep Clean Complete! All data and caches have been wiped.
echo.
pause
cls
goto menu

:disk_guide
cls
echo =======================================================================
echo WSL2 Virtual Disk (ext4.vhdx) Compaction Guide
echo =======================================================================
echo WSL virtual disks grow dynamically but do not shrink automatically.
echo If your hard disk is full, follow these manual steps to compact it:
echo.
echo 1. Close Docker Desktop completely.
echo 2. Open PowerShell as Administrator.
echo 3. Run the following command to stop WSL:
echo    wsl --shutdown
echo 4. Start the Windows diskpart tool:
echo    diskpart
echo 5. Inside diskpart, run the following commands (replace username with yours):
echo    select vdisk file="C:\Users\ffram\AppData\Local\Docker\wsl\data\ext4.vhdx"
echo    attach vdisk readonly
echo    compact vdisk
echo    detach vdisk
echo    exit
echo.
echo This will reclaim gigabytes of unused space from your SSD!
echo =======================================================================
echo.
pause
cls
goto menu

:exit
echo.
echo Exiting Cleanup Utility. Have a cool run!
exit /b
