#!/usr/bin/env python3
"""
executar_cpfl.py  -  Runner CLI da macro CPFL (ValidaPN GMP)
==============================================================
Modo automático, sem interface gráfica.  Chamado pelo
executar_automatico.py no ciclo contínuo do orquestrador.

Uso:
    python executar_cpfl.py
    python executar_cpfl.py --arquivo caminho/lote.csv --saida caminho/resultado.csv
    python executar_cpfl.py --max-intervencoes 5
"""
import argparse
import queue
import sys
import time
import threading
from pathlib import Path

# Garante que o pacote é importável mesmo quando executado diretamente
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from config import config_usuario           # credenciais GMP (.env)
from core.validador import Validador


# ---------------------------------------------------------------------------
# Paths padrão
# ---------------------------------------------------------------------------
PROJETO_DIR  = HERE.parents[2]             # raiz do projeto
DADOS_DIR    = PROJETO_DIR / "macro" / "dados_cpfl"
LOTE_CSV     = DADOS_DIR / "lote_pendente.csv"
RESULTADO_CSV= DADOS_DIR / "resultado_lote.csv"


# ---------------------------------------------------------------------------
# Callbacks para modo headless
# ---------------------------------------------------------------------------

def _make_callback_progresso(total_hint: list):
    """Retorna callback que imprime progresso no stdout (parseable pelo painel)."""
    def cb(info: dict):
        status = info.get("status", "")
        if status == "processando":
            pct  = info.get("progresso", 0)
            atual = info.get("linha_atual", 0)
            total = info.get("total_linhas", total_hint[0] or "?")
            print(f"[PROG] {atual}/{total} ({pct:.1f}%)", flush=True)
        elif status == "concluido":
            print("[STATUS] Processamento concluído.", flush=True)
        else:
            print(f"[STATUS] {info}", flush=True)
    return cb


class _AutoIntervencao:
    """Resolve intervenções automaticamente, tentando reiniciar até max_tentativas vezes."""
    def __init__(self, max_tentativas: int = 3):
        self.max_tentativas = max_tentativas
        self._contagem: dict = {}

    def __call__(self, info: dict):
        mensagem = info.get("mensagem", "intervenção desconhecida")
        q: queue.Queue = info["resposta_queue"]
        chave = mensagem[:60]
        self._contagem[chave] = self._contagem.get(chave, 0) + 1

        print(f"[INTERVENCAO] {mensagem}  (tentativa {self._contagem[chave]}/{self.max_tentativas})",
              flush=True)

        if self._contagem[chave] >= self.max_tentativas:
            print("[INTERVENCAO] Máximo de tentativas atingido. Parando.", flush=True)
            q.put("parar")
        else:
            time.sleep(5)
            q.put("reiniciar")


# ---------------------------------------------------------------------------
# Ponto de entrada CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Runner CLI da macro CPFL (ValidaPN GMP)")
    parser.add_argument("--arquivo", default=str(LOTE_CSV),
                        help="Caminho do CSV de entrada (CPF;UC)")
    parser.add_argument("--saida",   default=str(RESULTADO_CSV),
                        help="Caminho do CSV de saída (CPF;UC;PN;ATIVO;ERRO)")
    parser.add_argument("--max-intervencoes", type=int, default=3,
                        help="Máximo de tentativas automáticas por tipo de intervenção")
    args = parser.parse_args()

    arquivo = Path(args.arquivo)
    saida   = Path(args.saida)

    if not arquivo.exists():
        print(f"[ERRO] Arquivo de lote não encontrado: {arquivo}", flush=True)
        sys.exit(1)

    saida.parent.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Lote  : {arquivo}", flush=True)
    print(f"[INFO] Saida : {saida}", flush=True)
    print(f"[INFO] Usuário GMP: {config_usuario.get('usuario', '???')}", flush=True)

    # Estima total para o progresso (rápido — só conta linhas)
    try:
        with open(arquivo, encoding="latin-1") as f:
            total = sum(1 for _ in f) - 1
    except Exception:
        total = 0
    total_hint = [total]
    print(f"[INFO] Total de registros no lote: {total:,}", flush=True)

    # Instancia validador
    cb_progresso    = _make_callback_progresso(total_hint)
    cb_intervencao  = _AutoIntervencao(max_tentativas=args.max_intervencoes)

    validador = Validador(
        config_usuario=config_usuario,
        caminho_csv=str(arquivo),
        callback_progresso=cb_progresso,
        callback_intervencao=cb_intervencao,
        caminho_resultado=str(saida),
    )

    print("[INFO] Iniciando macro...", flush=True)
    inicio = time.time()
    validador.processamento()
    elapsed = time.time() - inicio

    h, r = divmod(int(elapsed), 3600)
    m, s = divmod(r, 60)
    print(f"[INFO] Macro encerrada. Tempo total: {h:02d}:{m:02d}:{s:02d}", flush=True)


if __name__ == "__main__":
    main()
