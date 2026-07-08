# ============================================================
# setup.ps1 — RetailFlow One-Time Setup (Windows PowerShell)
# ============================================================
# Chạy một lần sau khi pull project về để khởi tạo dữ liệu.
# Yêu cầu: Docker đang chạy (docker compose up -d đã được thực hiện)
#
# Cách dùng:
#   .\setup.ps1             — Chạy toàn bộ 4 bước
#   .\setup.ps1 -Step seed  — Chỉ chạy bước seed
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
            Write-Fail "❌ Thất bại tại: $Name (exit code $LASTEXITCODE)"
            exit $LASTEXITCODE
        }
        Write-Success "✅ Hoàn thành: $Name"
    }
    catch {
        Write-Fail "❌ Lỗi tại $Name : $_"
        exit 1
    }
}

# ── Các bước setup ──────────────────────────────────────────

$steps = @{
    "seed" = {
        Invoke-Step "=== [1/4] Seeding Product Catalog vào PostgreSQL ===" {
            python scripts/seeds/seed_product_catalog.py
        }
    }
    "batch" = {
        Invoke-Step "=== [2/4] Batch Ingestion CSV → MinIO ===" {
            python scripts/ingestion/batch/batch_ingestion_job.py
        }
    }
    "fetch-rates" = {
        Invoke-Step "=== [3/4] Fetching Exchange Rates ===" {
            python scripts/ingestion/fetch/fetch_exchange_rates.py
        }
    }
    "register-cdc" = {
        Invoke-Step "=== [4/5] Đăng ký Debezium CDC Connector ===" {
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
        Invoke-Step "=== [5/5] Thiết lập MinIO Lifecycle Policy (bảo vệ ổ đĩa) ===" {
            python scripts/utils/setup_minio_lifecycle.py
        }
    }
}

# ── Thực thi ────────────────────────────────────────────────

Write-Host "============================================" -ForegroundColor Yellow
Write-Host "   RetailFlow — One-Time Project Setup      " -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "Yêu cầu: Docker đang chạy (docker compose up -d)"
Write-Host "Step: $Step`n"

if ($Step -eq "all") {
    & $steps["seed"]
    & $steps["batch"]
    & $steps["fetch-rates"]
    & $steps["register-cdc"]
    & $steps["setup-lifecycle"]
} else {
    & $steps[$Step]
}

Write-Host "`n============================================" -ForegroundColor Green
Write-Host "   ✅ Setup Hoàn Tất!                        " -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host "   MinIO UI   : http://localhost:9001"
Write-Host "   Kafka UI   : http://localhost:8080"
Write-Host "   Debezium   : http://localhost:8083"
Write-Host ""
Write-Host "Để bật Simulation (giả lập hành vi người dùng):"
Write-Host "   docker compose -f docker/docker-compose.yml --profile simulation up simulation -d" -ForegroundColor White
Write-Host ""
