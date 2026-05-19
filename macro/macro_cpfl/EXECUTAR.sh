#!/bin/bash
# ============================================================================
# EXECUTAR.sh  -  Inicia o orquestrador CPFL (Linux)
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Ativa venv se existir
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
fi

echo "Iniciando orquestrador CPFL (modo terminal)..."
python3 executar_automatico.py --continuar "$@"
