# One command to run the whole local pipeline: generate (if needed) -> Kafka -> replay -> S3.
#   .\scripts\start.ps1
# It checks everything that usually goes wrong BEFORE starting, then runs docker compose.
Set-Location (Split-Path $PSScriptRoot)

function Fail($msg) { Write-Host "`n[X] $msg" -ForegroundColor Red; exit 1 }

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Fail "Docker is not installed." }
docker info *> $null
if ($LASTEXITCODE -ne 0) { Fail "Docker Desktop is not running. Open it, wait until it says 'running', then try again." }

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Fail "Created .env from .env.example. Open it, set S3_BUCKET, then run this script again."
}
$bucket = Select-String -Path ".env" -Pattern '^S3_BUCKET=(.+)$'
if (-not $bucket) { Fail "S3_BUCKET is empty in .env" }

if (-not (Test-Path "$env:USERPROFILE\.aws\credentials")) {
    Fail "No AWS credentials found. Run 'aws configure' first (keys are read from $env:USERPROFILE\.aws)."
}

$haveCsv = Test-Path "data\generated_csvs\08_events.csv"
$haveKaggle = (Get-ChildItem "data\kaggle_data" -Recurse -Include *.csv, *.zip -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0
if (-not $haveCsv -and -not $haveKaggle) {
    Fail "No data yet. Put the Kaggle CSV files in data\kaggle_data\ (see README), then run again."
}

Write-Host "Starting the pipeline..." -ForegroundColor Cyan
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { Fail "docker compose failed, see the message above." }

Write-Host "`nFollowing the logs (Ctrl+C only stops the viewing; the pipeline keeps running)." -ForegroundColor Cyan
Write-Host "Stop everything with: .\scripts\stop.ps1`n"
docker compose logs -f prepare replay dimensions consumer
