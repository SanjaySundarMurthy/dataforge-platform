# ============================================================
# DataForge Platform - One-Click Setup (Windows PowerShell)
# ============================================================
param(
    [ValidateSet("full", "preflight", "deps", "services", "data", "test")]
    [string]$Mode = "full"
)

$ErrorActionPreference = "Stop"

function Write-Banner {
    Write-Host ""
    Write-Host "  ╔══════════════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host "  ║       DataForge Platform - Setup Script          ║" -ForegroundColor Cyan
    Write-Host "  ║   Enterprise Data Engineering Platform           ║" -ForegroundColor Cyan
    Write-Host "  ╚══════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Info  { param($msg) Write-Host "[INFO]  $msg" -ForegroundColor Green }
function Write-Warn  { param($msg) Write-Host "[WARN]  $msg" -ForegroundColor Yellow }

function Test-Tool {
    param([string]$Name)
    $cmd = Get-Command $Name -ErrorAction SilentlyContinue
    if ($cmd) {
        Write-Info "$Name found: $($cmd.Source)"
        return $true
    }
    Write-Warn "$Name is not installed."
    return $false
}

# ----------------------------------------------------------
# Pre-flight checks
# ----------------------------------------------------------
function Invoke-Preflight {
    Write-Info "Running pre-flight checks..."
    $missing = 0
    if (-not (Test-Tool "docker"))  { $missing++ }
    if (-not (Test-Tool "python"))  { $missing++ }
    if (-not (Test-Tool "pip"))     { $missing++ }

    if ($missing -gt 0) {
        Write-Warn "$missing required tool(s) missing."
        $yn = Read-Host "Continue anyway? (y/N)"
        if ($yn -ne "y") { exit 1 }
    }
    Write-Info "Pre-flight checks passed."
}

# ----------------------------------------------------------
# Create .env from example
# ----------------------------------------------------------
function Initialize-Env {
    if (-not (Test-Path ".env")) {
        if (Test-Path ".env.example") {
            Copy-Item ".env.example" ".env"
            Write-Info "Created .env from .env.example - review and update values."
        }
    } else {
        Write-Info ".env already exists, skipping."
    }
}

# ----------------------------------------------------------
# Install Python dependencies
# ----------------------------------------------------------
function Install-PythonDeps {
    Write-Info "Installing Python dependencies..."
    try { pip install pyspark==3.5.0 delta-spark==3.0.0 pytest pytest-cov --quiet 2>$null }
    catch { Write-Warn "Spark deps install failed (Java 17 may be needed)" }

    try { pip install -r api/requirements.txt --quiet 2>$null }
    catch { Write-Warn "API deps install failed" }

    try { pip install -r data-generator/requirements.txt --quiet 2>$null }
    catch { Write-Warn "Generator deps install failed" }

    Write-Info "Python dependencies installed."
}

# ----------------------------------------------------------
# Start Docker services
# ----------------------------------------------------------
function Start-Services {
    Write-Info "Starting Docker services..."
    docker compose -f docker/docker-compose.yml up -d --build
    Write-Info "Core services started."

    $yn = Read-Host "Start monitoring stack too? (y/N)"
    if ($yn -eq "y") {
        docker compose -f docker/docker-compose.monitoring.yml up -d --build
        Write-Info "Monitoring stack started."
    }
}

# ----------------------------------------------------------
# Generate sample data
# ----------------------------------------------------------
function New-SampleData {
    Write-Info "Generating sample data..."
    try {
        pip install faker psycopg2-binary --quiet 2>$null
        python data-generator/src/generate.py --output-dir ./data/landing --rows 1000
    } catch {
        Write-Warn "Data generation failed - run manually later."
    }
    Write-Info "Sample data generation complete."
}

# ----------------------------------------------------------
# Run tests
# ----------------------------------------------------------
function Invoke-Tests {
    Write-Info "Running tests..."
    Push-Location spark-jobs
    try { python -m pytest tests/ -v --tb=short }
    catch { Write-Warn "Spark tests failed" }
    Pop-Location

    Push-Location api
    try { python -m pytest tests/ -v --tb=short }
    catch { Write-Warn "API tests failed" }
    Pop-Location

    Write-Info "Tests complete."
}

# ----------------------------------------------------------
# Print URLs
# ----------------------------------------------------------
function Write-Urls {
    Write-Host ""
    Write-Host "  ╔══════════════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host "  ║               Service URLs                       ║" -ForegroundColor Cyan
    Write-Host "  ╠══════════════════════════════════════════════════╣" -ForegroundColor Cyan
    Write-Host "  ║  API:           http://localhost:8000             ║" -ForegroundColor White
    Write-Host "  ║  API Docs:      http://localhost:8000/docs        ║" -ForegroundColor White
    Write-Host "  ║  Spark Master:  http://localhost:8080             ║" -ForegroundColor White
    Write-Host "  ║  Grafana:       http://localhost:3000             ║" -ForegroundColor White
    Write-Host "  ║  Prometheus:    http://localhost:9090             ║" -ForegroundColor White
    Write-Host "  ║  Alertmanager:  http://localhost:9093             ║" -ForegroundColor White
    Write-Host "  ╚══════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  DataForge Platform is ready!" -ForegroundColor Green
}

# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
Write-Banner

switch ($Mode) {
    "preflight" { Invoke-Preflight }
    "deps"      { Install-PythonDeps }
    "services"  { Start-Services; Write-Urls }
    "data"      { New-SampleData }
    "test"      { Invoke-Tests }
    "full" {
        Invoke-Preflight
        Initialize-Env
        Install-PythonDeps
        Start-Services
        New-SampleData
        Invoke-Tests
        Write-Urls
    }
}
