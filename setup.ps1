# ============================================================
# setup.ps1 - RetailFlow One-Time Setup (Windows PowerShell)
# ============================================================
# Run once after cloning the project to initialize data.
# Requirement: Docker must be running (docker compose up -d)
#
# Usage:
#   .\setup.ps1             - Run all 4 steps
#   .\setup.ps1 -Step seed  - Run only the seed step
# ============================================================
param(
    [ValidateSet("all", "seed", "batch", "fetch-rates", "register-cdc")]
    [string]$Step = "all"
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "`n$Message" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host $Message -ForegroundColor Green
}

function Write-Fail {
    param([string]$Message)
    Write-Host $Message -ForegroundColor Red
}

function Invoke-Step {
    param([string]$Name, [scriptblock]$Action)
    Write-Step $Name
    try {
        & $Action
        if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) {
            Write-Fail "X Failed at: $Name (exit code $LASTEXITCODE)"
            exit $LASTEXITCODE
        }
        Write-Success "v Completed: $Name"
    }
    catch {
        Write-Fail "X Error at $Name : $_"
        exit 1
    }
}

# ── Steps ──────────────────────────────────────────

# Auto-detect Python in virtual environment (venv) if it exists
$PYTHON = if (Test-Path ".\venv\Scripts\python.exe") { ".\venv\Scripts\python.exe" } else { "python" }

$steps = @{
    "seed"            = {
        Invoke-Step "=== [2/4] Seeding Product Catalog into PostgreSQL ===" {
            & $PYTHON scripts/seeds/seed_product_catalog.py
        }
    }
    "batch"           = {
        Invoke-Step "=== [1/4] Batch Ingestion CSV -> MinIO ===" {
            & $PYTHON scripts/ingestion/batch/batch_ingestion_job.py
        }
    }
    "fetch-rates"     = {
        Invoke-Step "=== [3/4] Fetching Exchange Rates ===" {
            & $PYTHON scripts/ingestion/fetch/fetch_exchange_rates.py
        }
    }
    "register-cdc"    = {
        Invoke-Step "=== [4/5] Registering Debezium CDC Connector ===" {
            $body = Get-Content -Raw "config/debezium/ecommerce-postgres-connector.json"
            $response = Invoke-RestMethod `
                -Method Post `
                -Uri "http://localhost:8083/connectors" `
                -ContentType "application/json" `
                -Body $body
            Write-Host ($response | ConvertTo-Json -Depth 5)
        }
    }
    "setup-lifecycle" = {
        Invoke-Step "=== [5/5] Setup MinIO Lifecycle Policy ===" {
            & $PYTHON scripts/utils/setup_minio_lifecycle.py
        }
    }
}

# ── Execution ────────────────────────────────────────────────

Write-Host "============================================" -ForegroundColor Yellow
Write-Host "   RetailFlow - One-Time Project Setup      " -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "Requirement: Docker is running (docker compose up -d)"
Write-Host "Step: $Step`n"

if ($Step -eq "all") {
    & $steps["batch"]
    & $steps["seed"]
    & $steps["fetch-rates"]
    & $steps["register-cdc"]
    & $steps["setup-lifecycle"]
}
else {
    & $steps[$Step]
}

Write-Host "`n============================================" -ForegroundColor Green
Write-Host "   v Setup Completed!                        " -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host "   MinIO UI   : http://localhost:9001"
Write-Host "   Kafka UI   : http://localhost:8080"
Write-Host "   Debezium   : http://localhost:8083"
Write-Host ""
Write-Host "To enable User Simulation:"
Write-Host "   docker compose -f docker/docker-compose.yml --profile simulation up simulation -d" -ForegroundColor White
Write-Host ""
