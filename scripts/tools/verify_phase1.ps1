# Phase 1 Check
$ProjectRoot = "C:\Users\Dice\keiba-ledger"
$PG_BIN = "C:\Program Files\PostgreSQL\18\bin"
Set-Location $ProjectRoot
$env:PATH = "$PG_BIN;$env:PATH"

Write-Host "=== Phase 1 Check ==="

# .env
if (-not (Test-Path ".env")) {
    Copy-Item .env.example .env
    Write-Host "[!] .env created. Edit LOCAL_DB_PASSWORD"
}
Write-Host "[1] .env OK"

# PostgreSQL
$psql = Join-Path $PG_BIN "psql.exe"
& $psql -U postgres -c "SELECT 1"
if ($LASTEXITCODE -ne 0) { Write-Host "[!] PostgreSQL: Check password, service"; exit 1 }
Write-Host "[2] PostgreSQL OK"

# DB
& $psql -U postgres -c "CREATE DATABASE keiba" 2>$null
& $psql -U postgres -d keiba -f "scripts\setup\create_tables.sql" 2>$null
Write-Host "[3] Schema OK"

# Python
python scripts/setup/setup_db.py --test 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[!] Python DB: Edit .env LOCAL_DB_PASSWORD"
} else {
    Write-Host "[4] Python DB OK"
}

Write-Host "`nNext: py -3.11-32 scripts/fetch/initial_fetch.py --from 2024-01-01 --no-odds --limit 50"
