#!/usr/bin/env bash
# ============================================================
# DataForge Platform — Teardown Script
# Stops all services and cleans up resources
# ============================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }

echo -e "${RED}"
echo "╔══════════════════════════════════════════════════╗"
echo "║       DataForge Platform — Teardown              ║"
echo "╚══════════════════════════════════════════════════╝"
echo -e "${NC}"

mode="${1:-services}"

case "$mode" in
  services)
    info "Stopping services..."
    docker compose -f docker/docker-compose.yml down 2>/dev/null || true
    docker compose -f docker/docker-compose.monitoring.yml down 2>/dev/null || true
    info "Services stopped."
    ;;
  volumes)
    info "Stopping services and removing volumes..."
    docker compose -f docker/docker-compose.yml down -v 2>/dev/null || true
    docker compose -f docker/docker-compose.monitoring.yml down -v 2>/dev/null || true
    info "Services stopped, volumes removed."
    ;;
  all)
    info "Full cleanup: services, volumes, generated data..."
    docker compose -f docker/docker-compose.yml down -v 2>/dev/null || true
    docker compose -f docker/docker-compose.monitoring.yml down -v 2>/dev/null || true
    rm -rf data/landing data/bronze data/silver data/gold 2>/dev/null || true
    info "Full cleanup complete."
    ;;
  *)
    echo "Usage: $0 {services|volumes|all}"
    exit 1
    ;;
esac

info "Teardown complete."
