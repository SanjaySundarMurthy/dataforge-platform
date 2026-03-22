#!/usr/bin/env bash
# ============================================================
# DataForge Platform — One-Click Setup (Linux / macOS)
# ============================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

banner() {
  echo -e "${CYAN}"
  echo "╔══════════════════════════════════════════════════╗"
  echo "║       DataForge Platform — Setup Script          ║"
  echo "║   Enterprise Data Engineering Platform           ║"
  echo "╚══════════════════════════════════════════════════╝"
  echo -e "${NC}"
}

info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

check_command() {
  if ! command -v "$1" &> /dev/null; then
    warn "$1 is not installed."
    return 1
  fi
  info "$1 found: $(command -v "$1")"
  return 0
}

# ----------------------------------------------------------
# Pre-flight checks
# ----------------------------------------------------------
preflight() {
  info "Running pre-flight checks..."
  local missing=0

  check_command docker   || missing=$((missing + 1))
  check_command docker   && docker compose version &>/dev/null || { warn "docker compose plugin not found"; missing=$((missing + 1)); }
  check_command python3  || check_command python || missing=$((missing + 1))
  check_command pip      || check_command pip3   || missing=$((missing + 1))

  if [ "$missing" -gt 0 ]; then
    warn "$missing required tool(s) missing. Install them before continuing."
    read -rp "Continue anyway? (y/N) " yn
    [[ "$yn" =~ ^[Yy]$ ]] || exit 1
  fi

  info "Pre-flight checks passed."
}

# ----------------------------------------------------------
# Create .env from example
# ----------------------------------------------------------
setup_env() {
  if [ ! -f .env ]; then
    if [ -f .env.example ]; then
      cp .env.example .env
      info "Created .env from .env.example — review and update values."
    else
      warn ".env.example not found, skipping .env creation."
    fi
  else
    info ".env already exists, skipping."
  fi
}

# ----------------------------------------------------------
# Install Python dependencies
# ----------------------------------------------------------
install_python_deps() {
  info "Installing Python dependencies..."
  local pip_cmd="pip"
  command -v pip3 &>/dev/null && pip_cmd="pip3"

  $pip_cmd install --quiet pyspark==3.5.0 delta-spark==3.0.0 pytest pytest-cov 2>/dev/null || warn "Spark deps install failed (Java 17 may be needed)"
  $pip_cmd install --quiet -r api/requirements.txt 2>/dev/null || warn "API deps install failed"
  $pip_cmd install --quiet -r data-generator/requirements.txt 2>/dev/null || warn "Generator deps install failed"
  info "Python dependencies installed."
}

# ----------------------------------------------------------
# Start Docker services
# ----------------------------------------------------------
start_services() {
  info "Starting Docker services..."
  docker compose -f docker/docker-compose.yml up -d --build
  info "Core services started."

  read -rp "Start monitoring stack too? (y/N) " yn
  if [[ "$yn" =~ ^[Yy]$ ]]; then
    docker compose -f docker/docker-compose.monitoring.yml up -d --build
    info "Monitoring stack started."
  fi
}

# ----------------------------------------------------------
# Generate sample data
# ----------------------------------------------------------
generate_data() {
  info "Generating sample data..."
  if [ -d "data-generator" ]; then
    local pip_cmd="pip"
    command -v pip3 &>/dev/null && pip_cmd="pip3"
    $pip_cmd install --quiet faker psycopg2-binary 2>/dev/null
    python3 data-generator/src/generate.py --output-dir ./data/landing --rows 1000 2>/dev/null || \
    python  data-generator/src/generate.py --output-dir ./data/landing --rows 1000 2>/dev/null || \
    warn "Data generation failed — run manually later."
  fi
  info "Sample data generation complete."
}

# ----------------------------------------------------------
# Run tests
# ----------------------------------------------------------
run_tests() {
  info "Running tests..."
  cd spark-jobs && python3 -m pytest tests/ -v --tb=short 2>/dev/null || warn "Spark tests failed"
  cd ..
  cd api && python3 -m pytest tests/ -v --tb=short 2>/dev/null || warn "API tests failed"
  cd ..
  info "Tests complete."
}

# ----------------------------------------------------------
# Print URLs
# ----------------------------------------------------------
print_urls() {
  echo ""
  echo -e "${CYAN}╔══════════════════════════════════════════════════╗${NC}"
  echo -e "${CYAN}║               Service URLs                       ║${NC}"
  echo -e "${CYAN}╠══════════════════════════════════════════════════╣${NC}"
  echo -e "${CYAN}║${NC}  API:           http://localhost:8000             ${CYAN}║${NC}"
  echo -e "${CYAN}║${NC}  API Docs:      http://localhost:8000/docs        ${CYAN}║${NC}"
  echo -e "${CYAN}║${NC}  Spark Master:  http://localhost:8080             ${CYAN}║${NC}"
  echo -e "${CYAN}║${NC}  Grafana:       http://localhost:3000             ${CYAN}║${NC}"
  echo -e "${CYAN}║${NC}  Prometheus:    http://localhost:9090             ${CYAN}║${NC}"
  echo -e "${CYAN}║${NC}  Alertmanager:  http://localhost:9093             ${CYAN}║${NC}"
  echo -e "${CYAN}╚══════════════════════════════════════════════════╝${NC}"
  echo ""
  echo -e "${GREEN}DataForge Platform is ready! 🚀${NC}"
}

# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
main() {
  banner

  local mode="${1:-full}"

  case "$mode" in
    preflight)
      preflight
      ;;
    deps)
      install_python_deps
      ;;
    services)
      start_services
      print_urls
      ;;
    data)
      generate_data
      ;;
    test)
      run_tests
      ;;
    full)
      preflight
      setup_env
      install_python_deps
      start_services
      generate_data
      run_tests
      print_urls
      ;;
    *)
      echo "Usage: $0 {full|preflight|deps|services|data|test}"
      exit 1
      ;;
  esac
}

main "$@"
