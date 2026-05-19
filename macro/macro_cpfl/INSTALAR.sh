#!/bin/bash
# ============================================================================
# INSTALAR.sh  -  Configuração do ambiente para a macro CPFL (Linux)
# ============================================================================
# Executa uma única vez no computador de destino.
# Cria um venv local, instala dependências com versões pinadas.
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================================================"
echo "  INSTALAÇÃO - Macro CPFL (Orquestrador com Banco)"
echo "======================================================================"
echo ""

# --- Verificar Python ---
if ! command -v python3 &> /dev/null; then
    echo "[ERRO] python3 não encontrado."
    echo "       Instale: sudo apt install python3 python3-venv python3-pip"
    exit 1
fi

echo "[OK] Python encontrado:"
python3 --version
echo ""

# --- Criar venv ---
VENV_DIR="$SCRIPT_DIR/venv"
if [ -d "$VENV_DIR" ]; then
    echo "[INFO] venv já existe em: $VENV_DIR"
    echo "       Para reinstalar, delete a pasta venv e rode novamente."
else
    echo "[INFO] Criando venv em: $VENV_DIR"
    python3 -m venv "$VENV_DIR"
    echo "[OK] venv criado."
fi
echo ""

# --- Ativar venv e instalar dependências ---
echo "[INFO] Instalando dependências (versões pinadas)..."
source "$VENV_DIR/bin/activate"

pip install --upgrade pip > /dev/null 2>&1
pip install -r "$SCRIPT_DIR/requirements_pinned.txt"

if [ $? -ne 0 ]; then
    echo ""
    echo "[ERRO] Falha ao instalar dependências."
    echo "       Verifique sua conexão com a internet."
    exit 1
fi

echo ""
echo "[OK] Todas as dependências instaladas."
echo ""

# --- Verificar chromedriver ---
CHROMEDRIVER="$SCRIPT_DIR/valida_pn_gmp-main/chromedriver"
if [ ! -f "$CHROMEDRIVER" ]; then
    echo "[AVISO] chromedriver não encontrado em: $CHROMEDRIVER"
    echo ""
    echo "  Você precisa baixar o chromedriver compatível com seu Chrome/Chromium:"
    echo "    https://googlechromelabs.github.io/chrome-for-testing/"
    echo ""
    echo "  Depois coloque o binário em:"
    echo "    $SCRIPT_DIR/valida_pn_gmp-main/chromedriver"
    echo ""
    echo "  E dê permissão de execução:"
    echo "    chmod +x $CHROMEDRIVER"
    echo ""
else
    chmod +x "$CHROMEDRIVER"
    echo "[OK] chromedriver encontrado e com permissão de execução."
fi

# --- Testar ambiente ---
echo ""
echo "======================================================================"
echo "  TESTANDO AMBIENTE..."
echo "======================================================================"
echo ""

python3 "$SCRIPT_DIR/testar_ambiente.py"

echo ""
echo "======================================================================"
echo "  INSTALAÇÃO CONCLUÍDA"
echo "======================================================================"
echo ""
echo "  Para executar a macro:"
echo "    ./EXECUTAR.sh"
echo "    ou: python3 executar_automatico.py"
echo ""
