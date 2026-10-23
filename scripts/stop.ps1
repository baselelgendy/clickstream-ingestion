# Stops the pipeline (containers). Kafka's data is kept for next time.
#   .\scripts\stop.ps1          stop
#   .\scripts\stop.ps1 -Reset   stop AND delete Kafka's stored messages (full clean start)
param([switch]$Reset)
Set-Location (Split-Path $PSScriptRoot)

if ($Reset) { docker compose down -v } else { docker compose down }
