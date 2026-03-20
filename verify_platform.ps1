# Energy Platform Verification Script (PowerShell)
# Run from the root of the project

Write-Host "--- Energy Platform End-to-End Verification ---" -ForegroundColor Cyan

# 1. Check Containers
Write-Host "`n1. Checking Container Status..." -ForegroundColor Yellow
docker compose ps

# 2. Check Kafka Topic
Write-Host "`n2. Checking Kafka Topic (energy_topic)..." -ForegroundColor Yellow
$offsets = docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic energy_topic --time -1
if ($offsets) {
    Write-Host "Kafka is receiving data: $offsets" -ForegroundColor Green
} else {
    Write-Host "Kafka topic is empty or not found!" -ForegroundColor Red
}

# 3. Check Cassandra Data
Write-Host "`n3. Checking Cassandra Ingestion (energy_data)..." -ForegroundColor Yellow
$count = docker exec cassandra cqlsh -e "SELECT count(*) FROM energy_analysis.energy_data;"
if ($count -match "\d+") {
    Write-Host "Cassandra contains data:`n$count" -ForegroundColor Green
} else {
    Write-Host "Cassandra table is empty!" -ForegroundColor Red
}

# 4. Check API Metrics
Write-Host "`n4. Checking API Connectivity..." -ForegroundColor Yellow
try {
    $resp = Invoke-RestMethod -Uri "http://localhost:8000/metrics" -Method Get
    Write-Host "API is responsive. Latest metrics count: $($resp.Count)" -ForegroundColor Green
} catch {
    Write-Host "API is unreachable or returned an error." -ForegroundColor Red
}

Write-Host "`n--- Verification Complete ---" -ForegroundColor Cyan
