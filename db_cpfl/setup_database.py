"""
setup_database.py  –  CPFL
==========================
Aplica o schema db_cpfl/schema.sql no banco bd_Automacoes_time_dados_cpfl.

Uso:
    python db_cpfl/setup_database.py
    python db_cpfl/setup_database.py --dry-run   # só valida, não executa
"""

import argparse
import sys
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config import db_cpfl  # noqa: E402

SCHEMA = Path(__file__).with_name("schema.sql")
SEP = "=" * 70


def split_statements(sql: str) -> list[str]:
    """
    Divide o SQL em statements individuais respeitando DELIMITER //.
    Necessário para triggers e stored procedures.
    """
    statements = []
    delimiter = ";"
    current = []

    for line in sql.splitlines():
        stripped = line.strip()

        # Troca de delimiter
        if stripped.upper().startswith("DELIMITER"):
            parts = stripped.split()
            if len(parts) >= 2:
                delimiter = parts[1]
            continue

        current.append(line)

        # Verifica se a linha termina com o delimiter atual
        if stripped.endswith(delimiter):
            stmt = "\n".join(current).strip()
            # Remove o delimiter do final
            if stmt.endswith(delimiter):
                stmt = stmt[: -len(delimiter)].strip()
            if stmt:
                statements.append(stmt)
            current = []

    # Captura qualquer restante
    stmt = "\n".join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


def main():
    parser = argparse.ArgumentParser(description="Aplica schema CPFL no banco")
    parser.add_argument("--dry-run", action="store_true",
                        help="Exibe os statements sem executar")
    args = parser.parse_args()

    print(SEP)
    print("SETUP DATABASE  –  bd_Automacoes_time_dados_cpfl")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteração será gravada")
    print(SEP)

    sql_raw = SCHEMA.read_text(encoding="utf-8")
    statements = [s for s in split_statements(sql_raw) if s.strip()]

    print(f"  Schema  : {SCHEMA}")
    print(f"  Statements encontrados: {len(statements)}")
    print()

    if args.dry_run:
        for i, stmt in enumerate(statements, 1):
            preview = stmt[:120].replace("\n", " ")
            print(f"  [{i:03}] {preview}...")
        print("\n[DRY-RUN] Nenhuma alteração foi executada.")
        return

    conn = pymysql.connect(**db_cpfl(autocommit=True))
    try:
        cur = conn.cursor()
        ok = err = 0
        for i, stmt in enumerate(statements, 1):
            try:
                cur.execute(stmt)
                ok += 1
                preview = stmt[:80].replace("\n", " ")
                print(f"  [OK  {i:03}] {preview}")
            except pymysql.err.OperationalError as e:
                # Ignora "already exists" e similares
                if e.args[0] in (1050, 1060, 1061, 1062, 1091, 1304):
                    ok += 1
                    print(f"  [SKIP {i:03}] já existe – ignorado")
                else:
                    err += 1
                    print(f"  [ERRO {i:03}] {e}")
            except Exception as e:
                err += 1
                print(f"  [ERRO {i:03}] {e}")

        print()
        print(SEP)
        print(f"  Resultado: {ok} OK  |  {err} ERROS")
        print(SEP)

        if err:
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
