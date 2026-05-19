#!/usr/bin/env python3
"""
testar_ambiente.py  -  Valida que todas as dependências estão OK
================================================================
Testa:
  1. Imports necessários (pymysql, pandas, selenium)
  2. Conexão ao banco MySQL (bd_Automacoes_time_dados_cpfl)
  3. ChromeDriver (execução headless)
  4. Estrutura de pastas / scripts

Execute antes de rodar a macro para garantir que tudo funciona.
"""
import sys
import os
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJETO_DIR = HERE.parents[1]  # raiz do projeto
MACRO_DIR = HERE / "valida_pn_gmp-main"

SEP = "=" * 60
ERROS = []


def _ok(msg):
    print(f"  [OK] {msg}")


def _erro(msg):
    print(f"  [ERRO] {msg}")
    ERROS.append(msg)


def _aviso(msg):
    print(f"  [AVISO] {msg}")


# ---------------------------------------------------------------------------
# 1. Imports
# ---------------------------------------------------------------------------
def testar_imports():
    print(f"\n{SEP}")
    print("1. TESTANDO IMPORTS")
    print(SEP)

    modulos = {
        "pymysql": "PyMySQL (banco de dados)",
        "pandas": "Pandas (dados)",
        "selenium": "Selenium (automação web)",
        "cryptography": "Cryptography (autenticação MySQL)",
    }
    for mod, desc in modulos.items():
        try:
            __import__(mod)
            _ok(f"{desc} — importado")
        except ImportError as e:
            _erro(f"{desc} — NÃO INSTALADO ({e})")


# ---------------------------------------------------------------------------
# 2. Conexão ao banco
# ---------------------------------------------------------------------------
def testar_banco():
    print(f"\n{SEP}")
    print("2. TESTANDO CONEXÃO AO BANCO")
    print(SEP)

    try:
        sys.path.insert(0, str(PROJETO_DIR))
        from config import db_cpfl
        _ok("config.py importado (credenciais carregadas)")
    except ImportError:
        _erro("config.py não encontrado na raiz do projeto!")
        _aviso("Copie config.example.py → config.py e preencha as credenciais.")
        return

    import pymysql

    cfg = db_cpfl(connect_timeout=10)
    print(f"  Host: {cfg['host']}")
    print(f"  DB:   {cfg['database']}")

    try:
        conn = pymysql.connect(**cfg)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM tabela_macros_cpfl WHERE status='pendente'")
        count = cur.fetchone()[0]
        _ok(f"Conexão OK — {count:,} registros pendentes no banco")
        cur.close()
        conn.close()
    except pymysql.err.OperationalError as e:
        code = e.args[0] if e.args else ""
        if code == 2003:
            _erro(f"Não conseguiu conectar ao host (timeout). Verifique rede/firewall.")
        elif code == 1045:
            _erro(f"Acesso negado (usuário/senha). Verifique config.py.")
        else:
            _erro(f"Erro de conexão: {e}")
    except Exception as e:
        _erro(f"Erro inesperado ao conectar: {e}")


# ---------------------------------------------------------------------------
# 3. ChromeDriver / Selenium
# ---------------------------------------------------------------------------
def testar_chromedriver():
    print(f"\n{SEP}")
    print("3. TESTANDO CHROMEDRIVER")
    print(SEP)

    import platform
    is_win = platform.system() == "Windows"
    chromedriver_name = "chromedriver.exe" if is_win else "chromedriver"
    chromedriver_path = MACRO_DIR / chromedriver_name

    if not chromedriver_path.exists():
        # Tenta encontrar no PATH
        import shutil as _shutil
        found = _shutil.which("chromedriver")
        if found:
            chromedriver_path = Path(found)
            _ok(f"chromedriver encontrado no PATH: {chromedriver_path}")
        else:
            _erro(f"{chromedriver_name} não encontrado em: {MACRO_DIR}")
            _aviso("Baixe o chromedriver compatível com seu Chrome de:")
            _aviso("  https://googlechromelabs.github.io/chrome-for-testing/")
            _aviso(f"Coloque {chromedriver_name} dentro de valida_pn_gmp-main/")
            return
    else:
        _ok(f"{chromedriver_name} encontrado: {chromedriver_path}")

    # Tenta verificar versão do Chrome/Chromium
    try:
        import subprocess
        if is_win:
            chrome_paths = [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            ]
            chrome_exe = None
            for p in chrome_paths:
                if os.path.exists(p):
                    chrome_exe = p
                    break
        else:
            import shutil as _shutil
            chrome_exe = _shutil.which("google-chrome") or _shutil.which("chromium-browser") or _shutil.which("chromium")

        if chrome_exe:
            result = subprocess.run([chrome_exe, "--version"], capture_output=True, text=True, timeout=5)
            chrome_ver = result.stdout.strip()
            _ok(f"Chrome/Chromium instalado: {chrome_ver}")
        else:
            _aviso("Chrome/Chromium não encontrado nos caminhos padrão")
    except Exception:
        _aviso("Não foi possível verificar versão do Chrome")

    # Tenta instanciar driver headless
    try:
        os.chdir(str(MACRO_DIR))
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service

        service = Service(executable_path=str(chromedriver_path))
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])

        driver = webdriver.Chrome(service=service, options=options)
        driver.get("about:blank")
        _ok("Selenium + ChromeDriver funcionando (headless)")
        driver.quit()
    except Exception as e:
        _erro(f"Falha ao iniciar ChromeDriver: {e}")
        _aviso(f"Certifique-se de que o {chromedriver_name} é compatível com sua versão do Chrome.")
    finally:
        os.chdir(str(HERE))


# ---------------------------------------------------------------------------
# 4. Estrutura de arquivos
# ---------------------------------------------------------------------------
def testar_estrutura():
    print(f"\n{SEP}")
    print("4. TESTANDO ESTRUTURA DO PROJETO")
    print(SEP)

    arquivos_obrigatorios = [
        (PROJETO_DIR / "config.py", "Credenciais do projeto"),
        (HERE / "executar_automatico.py", "Orquestrador principal"),
        (MACRO_DIR / "executar_cpfl.py", "Runner da macro"),
        (MACRO_DIR / "core" / "validador.py", "Core: Validador"),
        (MACRO_DIR / "core" / "portal_gmp.py", "Core: Portal GMP"),
        (MACRO_DIR / "core" / "gerenciador_dados.py", "Core: Gerenciador de dados"),
        (MACRO_DIR / "utils" / "scraping.py", "Utils: Scraping/driver"),
        (PROJETO_DIR / "etl" / "extraction" / "macro_cpfl" / "03_buscar_lote_cpfl.py", "ETL: Buscar lote"),
        (PROJETO_DIR / "etl" / "load" / "macro_cpfl" / "04_processar_retorno_cpfl.py", "ETL: Processar retorno"),
    ]

    for path, descricao in arquivos_obrigatorios:
        if path.exists():
            _ok(f"{descricao}")
        else:
            _erro(f"{descricao} — NÃO ENCONTRADO: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"\n{'#' * 60}")
    print("# TESTE DE AMBIENTE — Macro CPFL")
    print(f"# Python: {sys.version.split()[0]}")
    print(f"# Executável: {sys.executable}")
    print(f"{'#' * 60}")

    testar_imports()
    testar_banco()
    testar_chromedriver()
    testar_estrutura()

    print(f"\n{SEP}")
    if ERROS:
        print(f"RESULTADO: {len(ERROS)} ERRO(S) ENCONTRADO(S)")
        print(SEP)
        for i, e in enumerate(ERROS, 1):
            print(f"  {i}. {e}")
        print(f"\nCorreja os erros acima antes de executar a macro.")
        sys.exit(1)
    else:
        print("RESULTADO: TUDO OK — AMBIENTE PRONTO")
        print(SEP)
        print("\n  Pode executar: python executar_automatico.py")
        sys.exit(0)


if __name__ == "__main__":
    main()
