# PowerShell integration test for DataEng stack

function Wait-ForKafka {
  Write-Host "Waiting for Kafka broker..."
  for ($i=1; $i -le 30; $i++) {
    $result = docker exec kafka bash -c "nc -z localhost 9092" 2>$null
    if ($LASTEXITCODE -eq 0) { Write-Host "Kafka is up"; return }
    Start-Sleep -Seconds 5
  }
  throw "Kafka did not become reachable"
}

function Run-RedditStream {
  Write-Host "Running Reddit ingestion..."
  python C:/DataEngProj/scripts/reddit_stream.py
}

function Check-SparkRunning {
  $sparkRunning = docker exec spark-master bash -c "curl -s http://localhost:8080/api/v1/applications | grep RUNNING" 2>$null
  if (-not $sparkRunning) { throw "Spark job not RUNNING" } else { Write-Host "Spark job is RUNNING" }
}

function Check-HDFSParquet {
  $count = docker exec hdfs-namenode bash -c "hdfs dfs -count -q /data/reddit/parquet | awk '{print $2}'" 2>$null
  Write-Host "Parquet files in HDFS: $count"
  if (-not $count -or [int]$count -lt 1) { throw "No parquet files found" }
}

function Check-ES {
  $esResponse = curl -s http://localhost:9200/reddit/posts/_count
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
