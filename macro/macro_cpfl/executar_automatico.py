#!/usr/bin/env python3
"""
executar_automatico.py  -  Orquestrador contínuo da macro CPFL
==============================================================
Coordena o ciclo completo de automação:

  PASSO 1  [EXTRACT]   etl/extraction/macro_cpfl/03_buscar_lote_cpfl.py
                        -> Busca lote priorizado do banco (pendente, mais antigo primeiro),
                           exporta macro/dados_cpfl/lote_pendente.csv

  PASSO 2  [MACRO]     valida_pn_gmp-main/executar_cpfl.py
                        -> Selenium: acessa portal GMP, valida PN para cada CPF/UC
                        -> Salva macro/dados_cpfl/resultado_lote.csv

  PASSO 3  [LOAD]      etl/load/macro_cpfl/04_processar_retorno_cpfl.py
                        -> Interpreta resultados, atualiza tabela_macros_cpfl no banco
                        -> Arquiva os arquivos de lote

Uso:
    python executar_automatico.py                           # modo continuo
    python executar_automatico.py --tamanho 500            # lote menor
    python executar_automatico.py --pausa 120              # pausa entre ciclos (s)
    python executar_automatico.py --max-erros 3            # para após N erros seguidos
    python executar_automatico.py --continuar              # retoma sem limpeza inicial
"""

import argparse
import subprocess
import sys
import time
import shutil
import os
from pathlib import Path

# --- Caminhos ----------------------------------------------------------------
HERE        = Path(__file__).resolve().parent
PROJETO_DIR = HERE.parents[1]               # raiz do projeto
MACRO_DIR   = HERE / "valida_pn_gmp-main"
DADOS_DIR   = HERE.parent / "dados_cpfl"    # macro/dados_cpfl/

# Python: usa venv do valida_pn_gmp-main se existir, senão o do sistema
_venv_py    = MACRO_DIR / "venv" / "Scripts" / "python.exe"
_sys_py     = shutil.which("python") or shutil.which("python3") or sys.executable
MACRO_PY    = str(_venv_py) if _venv_py.exists() else _sys_py
ETL_PY      = _sys_py    # Python do sistema tem pymysql/pandas

SCRIPT_MACRO   = MACRO_DIR / "executar_cpfl.py"
SCRIPT_EXTRACT = PROJETO_DIR / "etl" / "extraction" / "macro_cpfl" / "03_buscar_lote_cpfl.py"
SCRIPT_LOAD    = PROJETO_DIR / "etl" / "load" / "macro_cpfl" / "04_processar_retorno_cpfl.py"

LOTE_CSV      = DADOS_DIR / "lote_pendente.csv"
RESULTADO_CSV = DADOS_DIR / "resultado_lote.csv"

SEP = "=" * 70


# --- Helpers -----------------------------------------------------------------

def _run(cmd: list, cwd: Path, descricao: str, timeout_s: int = 0) -> int:
    """Executa subprocesso e retornar o exit code. stdout vai para o próprio stdout."""
    print(f"\n{SEP}", flush=True)
    print(f">>> {descricao}", flush=True)
    print(f"    Comando: {' '.join(str(c) for c in cmd)}", flush=True)
    print(SEP, flush=True)

    kwargs = dict(
        cwd=str(cwd),
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    try:
        if timeout_s > 0:
            result = subprocess.run(cmd, timeout=timeout_s, **kwargs)
        else:
            result = subprocess.run(cmd, **kwargs)
        return result.returncode
    except subprocess.TimeoutExpired:
        print(f"[ERRO] Timeout ({timeout_s}s) em: {descricao}", flush=True)
        return -1
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f"[ERRO] Exceção ao executar {descricao}: {e}", flush=True)
        return -1


def _ciclo(tamanho: int, max_intervencoes: int) -> bool:
    """Executa um ciclo completo (extract → macro → load). Retorna True se OK."""

    # PASSO 1 — extrair lote
    rc = _run(
        [ETL_PY, "-u", str(SCRIPT_EXTRACT), "--tamanho", str(tamanho)],
        cwd=PROJETO_DIR,
        descricao="PASSO 1/3 — Buscar lote do banco",
        timeout_s=120,
    )
    if rc != 0:
        print(f"[ERRO] Passo 1 encerrou com código {rc}.", flush=True)
        return False

    if not LOTE_CSV.exists() or LOTE_CSV.stat().st_size < 10:
        print("[INFO] Lote vazio — nenhum registro pendente. Encerrando ciclo.", flush=True)
        return False

    # PASSO 2 — rodar macro Selenium
    rc = _run(
        [MACRO_PY, "-u", str(SCRIPT_MACRO),
         "--arquivo", str(LOTE_CSV),
         "--saida",   str(RESULTADO_CSV),
         "--max-intervencoes", str(max_intervencoes)],
        cwd=str(MACRO_DIR),
        descricao="PASSO 2/3 — Executar macro (portal GMP)",
    )
    if rc != 0:
        print(f"[AVISO] Macro encerrou com código {rc}. Tentando salvar resultados parciais.", flush=True)

    # PASSO 3 — carregar resultados no banco
    if not RESULTADO_CSV.exists() or RESULTADO_CSV.stat().st_size < 10:
        print("[AVISO] Nenhum resultado encontrado para salvar no banco.", flush=True)
        return False

    rc = _run(
        [ETL_PY, "-u", str(SCRIPT_LOAD)],
        cwd=PROJETO_DIR,
        descricao="PASSO 3/3 — Processar retorno e atualizar banco",
        timeout_s=300,
    )
    if rc != 0:
        print(f"[ERRO] Passo 3 encerrou com código {rc}.", flush=True)
        return False

    print("\n[OK] Ciclo completo.", flush=True)
    return True


# --- Main --------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Orquestrador contínuo da macro CPFL")
    parser.add_argument("--tamanho",          type=int, default=500,
                        help="Registros por lote (padrão: 500)")
    parser.add_argument("--pausa",            type=int, default=60,
                        help="Pausa em segundos entre ciclos (padrão: 60)")
    parser.add_argument("--max-erros",        type=int, default=3,
                        help="Para após N erros consecutivos (padrão: 3)")
    parser.add_argument("--max-intervencoes", type=int, default=3,
                        help="Max tentativas automáticas por intervenção GMP (padrão: 3)")
    parser.add_argument("--continuar",        action="store_true",
                        help="Retoma sem limpar arquivos de lote anteriores")
    args = parser.parse_args()

    # Verifica scripts essenciais
    for s, label in [(SCRIPT_MACRO, "executar_cpfl.py"),
                     (SCRIPT_EXTRACT, "03_buscar_lote_cpfl.py"),
                     (SCRIPT_LOAD,    "04_processar_retorno_cpfl.py")]:
        if not s.exists():
            print(f"[ERRO] Script não encontrado: {s}", flush=True)
            sys.exit(1)

    DADOS_DIR.mkdir(parents=True, exist_ok=True)

    # Limpa arquivos de lote anteriores (modo reiniciar)
    if not args.continuar:
        for f in (LOTE_CSV, RESULTADO_CSV):
            if f.exists():
                f.unlink()
                print(f"[INFO] Removido: {f.name}", flush=True)

    erros_consecutivos = 0
    ciclo_num = 0

    print(f"\n{'#'*70}", flush=True)
    print(f"# CPFL - Orquestrador Macro  |  lote={args.tamanho}  pausa={args.pausa}s  max_erros={args.max_erros}", flush=True)
    print(f"{'#'*70}\n", flush=True)

    try:
        while True:
            ciclo_num += 1
            print(f"\n{'='*70}", flush=True)
            print(f"CICLO #{ciclo_num}  |  {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
            print(f"{'='*70}", flush=True)

            ok = _ciclo(tamanho=args.tamanho, max_intervencoes=args.max_intervencoes)

            if ok:
                erros_consecutivos = 0
                print(f"\n[OK] Ciclo #{ciclo_num} concluído. Pausa de {args.pausa}s...", flush=True)
            else:
                erros_consecutivos += 1
                print(f"\n[AVISO] Ciclo #{ciclo_num} com problemas. "
                      f"Erros consecutivos: {erros_consecutivos}/{args.max_erros}", flush=True)
                if erros_consecutivos >= args.max_erros:
                    print(f"[ERRO] Limite de erros consecutivos atingido. Encerrando.", flush=True)
                    sys.exit(1)

            try:
                time.sleep(args.pausa)
            except KeyboardInterrupt:
                print("\n[INFO] Interrompido pelo usuário.", flush=True)
                sys.exit(0)

    except KeyboardInterrupt:
        print("\n[INFO] Orquestrador encerrado pelo usuário.", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
