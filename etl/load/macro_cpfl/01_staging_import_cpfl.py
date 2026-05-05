"""
01_staging_import_cpfl.py
=========================
Passo 1 do pipeline CPFL.

Lê arquivos CSV da pasta dados/ com as colunas:
  Nome ; CPF/CNPJ ; Cidade ; Estado ; End Logradouro ; CEP ;
  Unidade (consumidora) ; DDD1 ; Telefone 1 ; ... ; DDD7 ; Telefone 7

e carrega nas tabelas de staging:
  staging_imports      → um registro por arquivo
  staging_import_rows  → uma linha por CPF+UC (validada e normalizada)

NÃO toca em tabela_macros_cpfl — responsabilidade do passo 2.
Idempotente: re-execuções ignoram arquivos já importados.

Uso:
    python etl/load/macro_cpfl/01_staging_import_cpfl.py
    python etl/load/macro_cpfl/01_staging_import_cpfl.py --pasta dados/05-05-2026
    python etl/load/macro_cpfl/01_staging_import_cpfl.py --arquivo dados/Assisty_CPFL_PARTE1.csv
    python etl/load/macro_cpfl/01_staging_import_cpfl.py --dry-run
"""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_cpfl  # noqa: E402

DB_CONFIG = db_cpfl(autocommit=False)
DADOS_DIR = ROOT / "dados"
SEP = "=" * 70


# ---------------------------------------------------------------------------
# Normalização
# ---------------------------------------------------------------------------

def normalizar_cpf(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    s = s.zfill(11)
    return s if len(s) == 11 else None


def normalizar_uc(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    return s.zfill(10) if s else None


# ---------------------------------------------------------------------------
# Leitura do CSV
# ---------------------------------------------------------------------------

def ler_arquivo(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath, dtype=str, sep=";", encoding="utf-8",
                     on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]

    # Aliases → nomes internos padronizados
    aliases = {
        "CPF/CNPJ":             "cpf",
        "Nome":                 "nome",
        "Unidade (consumidora)":"uc",
        "Cidade":               "cidade",
        "Estado":               "uf",
        "End Logradouro":       "endereco",
        "CEP":                  "cep",
    }
    df = df.rename(columns=aliases)

    # Telefones: concatena DDD+Numero em colunas tel_1..tel_7
    for i in range(1, 8):
        ddd_col = f"DDD{i}"
        tel_col = f"Telefone {i}"
        out_col = f"tel_{i}"
        if ddd_col in df.columns and tel_col in df.columns:
            df[out_col] = (
                df[ddd_col].fillna("").str.strip().str.replace(r"\D", "", regex=True) +
                df[tel_col].fillna("").str.strip().str.replace(r"\D", "", regex=True)
            )
            df[out_col] = df[out_col].where(df[out_col].str.len() >= 8, other=None)
        elif tel_col in df.columns:
            df[out_col] = df[tel_col].str.strip().str.replace(r"\D", "", regex=True)
            df[out_col] = df[out_col].where(df[out_col].str.len() >= 8, other=None)

    return df


# ---------------------------------------------------------------------------
# Processamento por arquivo
# ---------------------------------------------------------------------------

def processar_arquivo(conn, filepath: Path, dry_run: bool) -> dict:
    cur = conn.cursor()
    df = ler_arquivo(filepath)

    if "cpf" not in df.columns or "uc" not in df.columns:
        print(f"  [ERRO] Colunas CPF/CNPJ ou Unidade (consumidora) ausentes em {filepath.name}. Ignorado.")
        cur.close()
        return {"skipped": True, "total": 0, "valid": 0, "invalid": 0}

    n_total = len(df)
    filename_curto = str(filepath.relative_to(ROOT)).replace("\\", "/")

    # Idempotência
    cur.execute(
        "SELECT id, status FROM staging_imports WHERE filename=%s LIMIT 1",
        (filename_curto,),
    )
    existente = cur.fetchone()
    if existente:
        sid, st = existente
        if st == "processing":
            print(f"  [RECOVERY] {filepath.name} preso em 'processing' (id={sid}). Limpando...")
            if not dry_run:
                cur.execute("DELETE FROM staging_import_rows WHERE staging_id=%s", (sid,))
                cur.execute("DELETE FROM staging_imports WHERE id=%s", (sid,))
                conn.commit()
        else:
            print(f"  [SKIP] {filepath.name} já existe (id={sid}, status={st})")
            cur.close()
            return {"skipped": True, "total": 0, "valid": 0, "invalid": 0}

    print(f"\n  Arquivo : {filepath.name}")
    print(f"  Linhas  : {n_total:,}")

    if not dry_run:
        cur.execute(
            """INSERT INTO staging_imports
               (filename, target_macro_table, total_rows, status, imported_by, started_at)
               VALUES (%s, 'tabela_macros_cpfl', %s, 'processing', 'pipeline_cpfl', NOW())""",
            (filename_curto, n_total),
        )
        staging_id = cur.lastrowid
        conn.commit()
    else:
        staging_id = 0

    n_valid = n_invalid = 0
    buf = []

    for idx, row in df.iterrows():
        norm_cpf = normalizar_cpf(row.get("cpf"))
        norm_uc  = normalizar_uc(row.get("uc"))

        if norm_cpf and norm_uc:
            status_val = "valid"
            msg = None
            n_valid += 1
        else:
            status_val = "invalid"
            msg = "CPF inválido" if not norm_cpf else "UC inválida"
            n_invalid += 1

        buf.append((
            staging_id, int(idx),
            str(row.get("cpf", "") or "")[:50],
            norm_cpf, norm_uc,
            status_val, msg,
        ))

        if len(buf) >= 500 and not dry_run:
            cur.executemany(
                """INSERT INTO staging_import_rows
                   (staging_id, row_idx, raw_cpf, normalized_cpf, normalized_uc,
                    validation_status, validation_message)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                buf,
            )
            conn.commit()
            buf = []

    if buf and not dry_run:
        cur.executemany(
            """INSERT INTO staging_import_rows
               (staging_id, row_idx, raw_cpf, normalized_cpf, normalized_uc,
                validation_status, validation_message)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            buf,
        )
        conn.commit()

    if not dry_run:
        cur.execute(
            """UPDATE staging_imports
               SET status='completed', rows_success=%s, rows_failed=%s, finished_at=NOW()
               WHERE id=%s""",
            (n_valid, n_invalid, staging_id),
        )
        conn.commit()

    print(f"  Válidas : {n_valid:,}  |  Inválidas: {n_invalid:,}")
    cur.close()
    return {"skipped": False, "staging_id": staging_id,
            "total": n_total, "valid": n_valid, "invalid": n_invalid}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Staging import CPFL — Passo 1")
    parser.add_argument("--pasta",   default=None,
                        help="Pasta com CSVs (padrão: dados/)")
    parser.add_argument("--arquivo", default=None,
                        help="Importar somente este arquivo CSV")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(SEP)
    print("PASSO 01 CPFL  –  Staging import")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteração será gravada")
    print(SEP)

    if args.arquivo:
        p = Path(args.arquivo)
        arquivos = [p if p.is_absolute() else ROOT / p]
    else:
        pasta = Path(args.pasta) if args.pasta else DADOS_DIR
        if not pasta.is_absolute():
            pasta = ROOT / pasta
        arquivos = sorted(pasta.glob("*.csv"))

    if not arquivos:
        print("[ERRO] Nenhum CSV encontrado.")
        sys.exit(1)

    print(f"  Arquivos encontrados: {len(arquivos)}")

    conn = pymysql.connect(**DB_CONFIG)
    try:
        totais = {"arquivos": 0, "linhas": 0, "validas": 0, "invalidas": 0}
        for arq in arquivos:
            res = processar_arquivo(conn, arq, args.dry_run)
            if not res.get("skipped"):
                totais["arquivos"] += 1
                totais["linhas"]   += res["total"]
                totais["validas"]  += res["valid"]
                totais["invalidas"] += res["invalid"]

        print(f"\n{SEP}")
        print("PASSO 01 CPFL CONCLUÍDO")
        print(f"  Arquivos processados : {totais['arquivos']}")
        print(f"  Linhas totais        : {totais['linhas']:,}")
        print(f"  Válidas              : {totais['validas']:,}")
        print(f"  Inválidas            : {totais['invalidas']:,}")
        print(SEP)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
