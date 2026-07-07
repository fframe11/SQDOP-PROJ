try {
    $res = Invoke-RestMethod -Uri "http://localhost/api/v1/export/tables" -UseBasicParsing
    $tables = $res.tables.name
} catch {
    $tables = @("products","gov_data","sales_records")
}
$report = @()
foreach ($tbl in $tables) {
    $start = Get-Date
    Write-Host "Running quality engine for $tbl"
    docker exec sdoqap-spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_quality_engine.py $tbl
    $exit = $LASTEXITCODE
    $end = Get-Date
    $duration = ($end - $start).TotalSeconds
    $report += "| $tbl | $duration | $exit |"
}
$header = "| Table | Duration (s) | Exit Code |"
$sep = "|-------|--------------|----------|"
$reportContent = $header + "`n" + $sep + "`n" + ($report -join "`n")
Set-Content -Path "C:\DataEngProj\quality_metrics.md" -Value $reportContent
