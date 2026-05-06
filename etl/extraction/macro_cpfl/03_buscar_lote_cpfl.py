"""
03_buscar_lote_cpfl.py
======================
ETAPA AUTOMÁTICA — Passo 1 do ciclo da macro CPFL.

Responsabilidade:
  1. Consulta tabela_macros_cpfl com prioridade:
       mais antigo primeiro
  2. Marca registros selecionados como 'processando'.
  3. Exporta lote como CSV no formato esperado pela macro CPFL:
         CPF;UC          (separador ponto-e-vírgula, sem cabeçalho extra)
     → macro/dados_cpfl/lote_pendente.csv
  4. Salva metadados em macro/dados_cpfl/lote_meta.json para que
     04_processar_retorno_cpfl.py possa correlacionar os resultados.

Uso:
    python etl/extraction/macro_cpfl/03_buscar_lote_cpfl.py
    python etl/extraction/macro_cpfl/03_buscar_lote_cpfl.py --tamanho 1000
    python etl/extraction/macro_cpfl/03_buscar_lote_cpfl.py --dry-run
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_cpfl  # noqa: E402

DB_CONFIG = db_cpfl(autocommit=False)

LOTE_CSV  = ROOT / "macro" / "dados_cpfl" / "lote_pendente.csv"
LOTE_META = ROOT / "macro" / "dados_cpfl" / "lote_meta.json"

TAMANHO_PADRAO = 2000
SEP = "=" * 70

# ---------------------------------------------------------------------------
# Query de prioridade
# ---------------------------------------------------------------------------
SQL_BUSCAR_LOTE = """
SELECT
    tm.id          AS macro_id,
    c.cpf          AS cpf,
    cu.uc          AS uc,
    tm.status      AS status_atual
FROM tabela_macros_cpfl tm
JOIN clientes    c  ON c.id  = tm.cliente_id
JOIN cliente_uc  cu ON cu.id = tm.cliente_uc_id
WHERE tm.status = 'pendente'
ORDER BY
    tm.data_update ASC,
    tm.id          ASC
LIMIT %s
"""

SQL_MARCAR_PROCESSANDO = """
UPDATE tabela_macros_cpfl
SET status = 'processando',
    data_update = NOW()
WHERE id IN ({placeholders})
  AND status = 'pendente'
"""


def buscar_lote(conn, tamanho: int, dry_run: bool) -> pd.DataFrame:
    cur = conn.cursor(pymysql.cursors.DictCursor)
    print(f"  Consultando lote de até {tamanho:,} registros...")
    cur.execute(SQL_BUSCAR_LOTE, (tamanho,))
    rows = cur.fetchall()

    if not rows:
        print("  [INFO] Nenhum registro pendente ou a reprocessar.")
        cur.close()
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    resumo = df.groupby("status_atual").size()
    print(f"\n  Lote obtido: {len(df):,} registros")
    for status, qtd in resumo.items():
        print(f"    {status:<12} | {qtd:>6,}")

    if not dry_run:
        ids = df["macro_id"].tolist()
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(SQL_MARCAR_PROCESSANDO.format(placeholders=placeholders), ids)
        conn.commit()
        print(f"\n  [OK] {cur.rowcount:,} registros marcados como 'processando'")

    cur.close()
    return df


def exportar_csv(df: pd.DataFrame, dry_run: bool):
    """Exporta CPF;UC no formato esperado pela macro CPFL."""
    LOTE_CSV.parent.mkdir(parents=True, exist_ok=True)

    df_macro = df[["cpf", "uc"]].copy()
    # A macro CPFL espera separador ponto-e-vírgula
    df_macro.columns = ["CPF", "UC"]

    if dry_run:
        print(f"\n  [DRY-RUN] CSV seria exportado: {LOTE_CSV}")
        print(f"  [DRY-RUN] {len(df_macro):,} linhas")
        print(df_macro.head(3).to_string(index=False))
        return

    df_macro.to_csv(LOTE_CSV, index=False, sep=";", encoding="utf-8")
    print(f"\n  [OK] CSV exportado → {LOTE_CSV}  ({len(df_macro):,} linhas)")


def salvar_meta(df: pd.DataFrame, dry_run: bool):
    """Salva metadados do lote para correlação em 04_processar_retorno_cpfl."""
    meta = {
        "gerado_em": datetime.now().isoformat(),
        "total":     len(df),
        "dry_run":   dry_run,
        "registros": df[["macro_id", "cpf", "uc", "status_atual"]].to_dict(orient="records"),
    }

    if dry_run:
        print(f"  [DRY-RUN] META seria salvo em: {LOTE_META}")
        return

    LOTE_META.parent.mkdir(parents=True, exist_ok=True)
    with open(LOTE_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Meta salvo → {LOTE_META}")


def main():
    parser = argparse.ArgumentParser(
        description="Passo 1 da macro CPFL: busca lote priorizado do banco"
    )
    parser.add_argument("--tamanho", type=int, default=TAMANHO_PADRAO,
                        help=f"Tamanho máximo do lote (padrão: {TAMANHO_PADRAO})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Consulta sem marcar como 'processando' e sem exportar")
    args = parser.parse_args()

    print(SEP)
    print(f"PASSO 03 CPFL  –  Buscar lote macro  |  tamanho={args.tamanho:,}")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteração será gravada")
    print(SEP)

    conn = pymysql.connect(**DB_CONFIG)
    try:
        df = buscar_lote(conn, args.tamanho, args.dry_run)
        if df.empty:
            print("\n[INFO] Lote vazio — macro CPFL não será executada.")
            sys.exit(0)

        exportar_csv(df, args.dry_run)
        salvar_meta(df, args.dry_run)

        print(f"\n{SEP}")
        print("PASSO 03 CPFL CONCLUÍDO — lote pronto para a macro")
        print(SEP)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
