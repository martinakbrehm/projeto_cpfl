#!/usr/bin/env python3
"""
executar_automatico.py  -  Orquestrador contínuo da macro CPFL
==============================================================
Coordena o ciclo completo de automação mantendo a SESSÃO DO PORTAL ABERTA
entre lotes (para não precisar resolver captcha a cada ciclo).

  PASSO 1  [EXTRACT]   etl/extraction/macro_cpfl/03_buscar_lote_cpfl.py
                        -> Busca lote priorizado do banco (pendente, mais antigo primeiro),
                           exporta macro/dados_cpfl/lote_pendente.csv

  PASSO 2  [MACRO]     Validador (in-process, sessão persistente)
                        -> Selenium: acessa portal GMP, valida PN para cada CPF/UC
                        -> Salva macro/dados_cpfl/resultado_lote.csv

  PASSO 3  [LOAD]      etl/load/macro_cpfl/04_processar_retorno_cpfl.py
                        -> Interpreta resultados, atualiza tabela_macros_cpfl no banco
                        -> Arquiva os arquivos de lote

A conexão com o portal (Chrome + login) é feita UMA VEZ no início.
Se cair, tenta reconectar. O captcha só precisa ser resolvido 1x.

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
import queue
from pathlib import Path

# --- Caminhos ----------------------------------------------------------------
HERE        = Path(__file__).resolve().parent
PROJETO_DIR = HERE.parents[1]               # raiz do projeto
MACRO_DIR   = HERE / "valida_pn_gmp-main"
DADOS_DIR   = HERE.parent / "dados_cpfl"    # macro/dados_cpfl/

# Adiciona MACRO_DIR ao sys.path para importar Validador diretamente
sys.path.insert(0, str(MACRO_DIR))

# Python: usa venv local (macro_cpfl/venv) para TUDO — banco + selenium
# Prioridade: venv local > venv do valida_pn_gmp-main > python do sistema
# Detecta OS para path do venv
import platform
_IS_WIN = platform.system() == "Windows"
_VENV_BIN = "Scripts" if _IS_WIN else "bin"
_PY_NAME  = "python.exe" if _IS_WIN else "python"

_venv_local = HERE / "venv" / _VENV_BIN / _PY_NAME
_venv_macro = MACRO_DIR / "venv" / _VENV_BIN / _PY_NAME
_sys_py     = shutil.which("python3") or shutil.which("python") or sys.executable

if _venv_local.exists():
    _PYTHON = str(_venv_local)
elif _venv_macro.exists():
    _PYTHON = str(_venv_macro)
else:
    _PYTHON = _sys_py

# Usa o MESMO Python para ETL (extract/load são subprocessos)
ETL_PY = _PYTHON

SCRIPT_EXTRACT = PROJETO_DIR / "etl" / "extraction" / "macro_cpfl" / "03_buscar_lote_cpfl.py"
SCRIPT_LOAD    = PROJETO_DIR / "etl" / "load" / "macro_cpfl" / "04_processar_retorno_cpfl.py"

LOTE_CSV      = DADOS_DIR / "lote_pendente.csv"
RESULTADO_CSV = DADOS_DIR / "resultado_lote.csv"

SEP = "=" * 70


# --- Callbacks para o Validador (in-process) ---------------------------------

def _make_callback_progresso():
    """Callback que imprime progresso no stdout."""
    def cb(info: dict):
        status = info.get("status", "")
        if status == "processando":
            pct = info.get("progresso", 0)
            atual = info.get("linha_atual", 0)
            total = info.get("total_linhas", "?")
            print(f"  [PROG] {atual}/{total} ({pct:.1f}%)", flush=True)
        elif status == "concluido":
            print("  [OK] Lote processado.", flush=True)
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

        print(f"  [INTERVENCAO] {mensagem}  (tentativa {self._contagem[chave]}/{self.max_tentativas})",
              flush=True)

        if self._contagem[chave] >= self.max_tentativas:
            print("  [INTERVENCAO] Máximo de tentativas atingido. Parando lote.", flush=True)
            q.put("parar")
        else:
            time.sleep(5)
            q.put("reiniciar")

    def reset(self):
        """Reseta contadores entre lotes."""
        self._contagem.clear()


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


# --- Sessão persistente do portal --------------------------------------------

class SessaoPortal:
    """Gerencia uma sessão persistente do portal GMP (Chrome aberto entre lotes)."""

    def __init__(self, max_intervencoes: int = 3):
        from config import config_usuario as _cfg
        self.config_usuario = _cfg
        self.max_intervencoes = max_intervencoes
        self._validador = None
        self._conectado = False

    @property
    def conectado(self) -> bool:
        """Verifica se o driver ainda está vivo."""
        if not self._validador or not self._validador.portal.driver:
            return False
        try:
            # Teste rápido: acessar título da janela
            _ = self._validador.portal.driver.title
            return True
        except Exception:
            return False

    def conectar(self):
        """Abre o Chrome, faz login (captcha manual 1x) e mantém sessão."""
        from core.validador import Validador

        print(f"\n{SEP}", flush=True)
        print(">>> CONECTANDO AO PORTAL GMP (resolva o captcha no navegador)", flush=True)
        print(SEP, flush=True)

        cb_progresso = _make_callback_progresso()
        cb_intervencao = _AutoIntervencao(max_tentativas=self.max_intervencoes)

        # Cria Validador com CSV dummy (será substituído a cada lote)
        self._validador = Validador(
            config_usuario=self.config_usuario,
            caminho_csv=str(LOTE_CSV),
            callback_progresso=cb_progresso,
            callback_intervencao=cb_intervencao,
            caminho_resultado=str(RESULTADO_CSV),
        )

        # Inicia driver e faz login (captcha aqui)
        self._validador.portal.iniciar_driver()
        self._validador.portal.login(
            self.config_usuario["usuario"],
            self.config_usuario["senha"],
        )
        self._conectado = True
        print("[OK] Conectado ao portal GMP. Sessão mantida entre lotes.", flush=True)

    def processar_lote(self, arquivo_csv: Path, arquivo_saida: Path) -> bool:
        """Processa um lote usando a sessão existente (sem fechar o browser)."""
        from core.gerenciador_dados import GerenciadorDados

        if not self.conectado:
            print("[AVISO] Sessão caiu. Reconectando...", flush=True)
            self.conectar()

        # Recria GerenciadorDados para o novo lote
        self._validador.dados = GerenciadorDados(str(arquivo_csv), str(arquivo_saida))
        self._validador.rodando = True

        # Reset contadores de intervenção entre lotes
        if hasattr(self._validador.callback_intervencao, 'reset'):
            self._validador.callback_intervencao.reset()

        try:
            self._validador._executar_fluxo()
            return True
        except Exception as e:
            print(f"[ERRO] Erro durante processamento do lote: {e}", flush=True)
            # Não fecha o browser! Tenta manter sessão
            return False

    def fechar(self):
        """Fecha o browser (só no encerramento final do orquestrador)."""
        if self._validador and self._validador.portal.driver:
            try:
                self._validador.portal.finalizar()
            except Exception:
                pass
        self._conectado = False


# --- Ciclo principal ---------------------------------------------------------

def _ciclo(sessao: SessaoPortal, tamanho: int) -> bool:
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

    # PASSO 2 — rodar macro (in-process, sessão persistente)
    print(f"\n{SEP}", flush=True)
    print(">>> PASSO 2/3 — Executar macro (portal GMP) [sessão persistente]", flush=True)
    print(SEP, flush=True)

    ok = sessao.processar_lote(LOTE_CSV, RESULTADO_CSV)
    if not ok:
        print("[AVISO] Macro encerrou com erro. Tentando salvar resultados parciais.", flush=True)

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
    for s, label in [(SCRIPT_EXTRACT, "03_buscar_lote_cpfl.py"),
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

    print(f"\n{'#'*70}", flush=True)
    print(f"# CPFL - Orquestrador Macro  |  lote={args.tamanho}  pausa={args.pausa}s  max_erros={args.max_erros}", flush=True)
    print(f"# SESSÃO PERSISTENTE: Chrome fica aberto entre lotes (captcha 1x)", flush=True)
    print(f"{'#'*70}\n", flush=True)

    # Abre sessão do portal (captcha resolvido aqui, 1 vez)
    sessao = SessaoPortal(max_intervencoes=args.max_intervencoes)
    sessao.conectar()

    erros_consecutivos = 0
    ciclo_num = 0

    try:
        while True:
            ciclo_num += 1
            print(f"\n{'='*70}", flush=True)
            print(f"CICLO #{ciclo_num}  |  {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
            print(f"{'='*70}", flush=True)

            ok = _ciclo(sessao=sessao, tamanho=args.tamanho)

            if ok:
                erros_consecutivos = 0
                print(f"\n[OK] Ciclo #{ciclo_num} concluído. Pausa de {args.pausa}s...", flush=True)
            else:
                erros_consecutivos += 1
                print(f"\n[AVISO] Ciclo #{ciclo_num} com problemas. "
                      f"Erros consecutivos: {erros_consecutivos}/{args.max_erros}", flush=True)
                if erros_consecutivos >= args.max_erros:
                    print(f"[ERRO] Limite de erros consecutivos atingido. Encerrando.", flush=True)
                    break

            try:
                time.sleep(args.pausa)
            except KeyboardInterrupt:
                print("\n[INFO] Interrompido pelo usuário.", flush=True)
                break

    except KeyboardInterrupt:
        print("\n[INFO] Orquestrador encerrado pelo usuário.", flush=True)
    finally:
        sessao.fechar()
        print("[INFO] Browser fechado. Fim.", flush=True)


if __name__ == "__main__":
    main()
