# Creates the 5 clickstream topics (safe to run more than once).
# Run from anywhere:  .\scripts\create_topics.ps1
Set-Location (Split-Path $PSScriptRoot)

$prefix = "clickstream"
if (Test-Path ".env") {
    $line = Select-String -Path ".env" -Pattern '^KAFKA_TOPIC_PREFIX=(.+)$'
    if ($line) { $prefix = $line.Matches[0].Groups[1].Value.Trim() }
}

foreach ($table in "events", "cart_items", "checkouts", "payment_attempts", "orders") {
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
        --topic "$prefix.$table" --bootstrap-server localhost:9092 `
        --partitions 3 --replication-factor 1
}

Write-Host "`nTopics now in Kafka:"
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
