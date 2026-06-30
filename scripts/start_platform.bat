@echo off
rem ---------------------------------------------------------------
rem  Start Data Engineering Platform (Docker Compose)
rem ---------------------------------------------------------------
setlocal

rem Build all images first
docker compose -f docker-compose.yml build

rem Bring up the stack in detached mode
docker compose -f docker-compose.yml up -d

rem Wait for core services to become healthy (Kafka, Spark Master, HDFS, Elasticsearch)
echo Waiting for services to become healthy... 
for /L %%i in (1,1,30) do (
  docker inspect --format="{{.State.Health.Status}}" kafka | findstr /i "healthy" >nul && (
    echo Kafka is healthy
    goto :CHECK_SPARK
  )
  timeout /t 5 >nul
)

:CHECK_SPARK
for /L %%i in (1,1,30) do (
  docker inspect --format="{{.State.Health.Status}}" spark-master | findstr /i "healthy" >nul && (
    echo Spark Master is healthy
    goto :CHECK_HDFS
  )
  timeout /t 5 >nul
)

:CHECK_HDFS
for /L %%i in (1,1,30) do (
  docker inspect --format="{{.State.Health.Status}}" hdfs-namenode | findstr /i "healthy" >nul && (
    echo HDFS NameNode is healthy
    goto :DONE
  )
  timeout /t 5 >nul
)

:DONE
echo All core services are up. You can now run the ingestion script or UI.
endlocal
