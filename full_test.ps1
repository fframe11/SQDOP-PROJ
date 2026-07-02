# PowerShell integration test for DataEng stack

# Dynamic Container Resolution
$kafkaContainer = "sdoqap-kafka"
$null = docker inspect $kafkaContainer 2>$null
if ($LASTEXITCODE -ne 0) { $kafkaContainer = "kafka" }

$sparkMaster = "sdoqap-spark-master"
$null = docker inspect $sparkMaster 2>$null
if ($LASTEXITCODE -ne 0) { $sparkMaster = "spark-master" }

$hdfsNamenode = "sdoqap-namenode"
$null = docker inspect $hdfsNamenode 2>$null
if ($LASTEXITCODE -ne 0) { $hdfsNamenode = "hdfs-namenode" }

function Wait-ForKafka {
  global $kafkaContainer
  Write-Host "Waiting for Kafka broker ($kafkaContainer)..."
  for ($i=1; $i -le 30; $i++) {
    $result = docker exec $kafkaContainer bash -c "nc -z localhost 9092" 2>$null
    if ($LASTEXITCODE -eq 0) { Write-Host "Kafka is up"; return }
    Start-Sleep -Seconds 5
  }
  throw "Kafka did not become reachable"
}

function Run-RedditStream {
  Write-Host "Ensuring 'kafka-python' is installed on host..."
  python -m pip install kafka-python --quiet 2>$null
  Write-Host "Running Reddit ingestion..."
  python "$PSScriptRoot/scripts/reddit_stream.py"
}

function Check-SparkRunning {
  global $sparkMaster
  $sparkRunning = docker exec $sparkMaster bash -c "curl -s http://localhost:8080/api/v1/applications | grep RUNNING" 2>$null
  if (-not $sparkRunning) { throw "Spark job not RUNNING" } else { Write-Host "Spark job is RUNNING" }
}

function Check-HDFSParquet {
  global $hdfsNamenode
  $count = docker exec $hdfsNamenode bash -c "hdfs dfs -count -q /data/reddit/parquet | awk '{print $2}'" 2>$null
  Write-Host "Parquet files in HDFS: $count"
  if (-not $count -or [int]$count -lt 1) { throw "No parquet files found" }
}

function Check-ES {
  $esResponse = curl -s -u "elastic:sdoqap_secure" http://localhost:9200/reddit/posts/_count
  $json = $esResponse | ConvertFrom-Json
  $cnt = $json.count
  Write-Host "Documents indexed in ES: $cnt"
  if (-not $cnt -or [int]$cnt -lt 1) { throw "No documents indexed" }
}

Wait-ForKafka
Run-RedditStream
Start-Sleep -Seconds 15
Check-SparkRunning
Check-HDFSParquet
Check-ES
Write-Host "All checks passed!"
