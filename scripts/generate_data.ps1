# Runs the teammate's generator, then validates the CSVs.
#   .\scripts\generate_data.ps1          full size (10M events, needs lots of disk + time)
#   .\scripts\generate_data.ps1 -Small   10,000 events, to check everything works first
# Needs the Kaggle CSVs in data\kaggle_data (see README).
param([switch]$Small)
Set-Location (Split-Path $PSScriptRoot)

$common = @("--input", "data/kaggle_data", "--output-dir", "data/generated_csvs")
if ($Small) {
    $size = @("--events", "10000", "--sessions", "1000", "--devices", "600", "--customers", "150",
              "--cart-items", "700", "--checkouts", "300", "--payment-attempts", "200",
              "--orders", "100", "--products", "100", "--categories", "10", "--calendar-days", "30")
} else {
    $size = @()
}

python generator/generate_clickstream_kaggle.py @common @size
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
python generator/validate_clickstream_csvs.py --input-dir data/generated_csvs
