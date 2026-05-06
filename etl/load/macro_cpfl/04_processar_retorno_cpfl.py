"""
04_processar_retorno_cpfl.py
============================
ETAPA AUTOMÁTICA — Passo 3 do ciclo da macro CPFL.

Responsabilidade:
  1. Lê o arquivo de resultado gerado pela macro CPFL:
       macro/dados_cpfl/resultado_lote.csv
     Colunas esperadas: CPF;UC;PN;ATIVO;ERRO
  2. Cruza com lote_meta.json para obter o macro_id de cada CPF+UC.
  3. Interpreta ATIVO+ERRO via interpretar_resposta_cpfl.py:
       → (resposta_id, novo_status, pn)
  4. Insere UM NOVO REGISTRO em tabela_macros_cpfl com o resultado.
     O registro original (pendente/processando) é revertido para 'pendente',
     preservando o histórico de quando a combinação CPF+UC foi enfileirada.
  5. Registros que ficaram em 'processando' mas não vieram no resultado
     (macro parou no meio) são devolvidos para 'pendente'.
  6. Arquiva os arquivos de lote com timestamp em macro/dados_cpfl/arquivo/.

Fluxo de status:
  pendente → processando   (passo 03)
  processando → novo registro (ativo | inativo)
              + original revertido para pendente

Uso:
    python etl/load/macro_cpfl/04_processar_retorno_cpfl.py
    python etl/load/macro_cpfl/04_processar_retorno_cpfl.py --dry-run
"""

import argparse
import json
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_cpfl  # noqa: E402

# Transformation layer
sys.path.insert(0, str(ROOT / "etl" / "transformation" / "macro_cpfl"))
from interpretar_resposta_cpfl import interpretar_linha  # noqa: E402

DB_CONFIG = db_cpfl(autocommit=False)

LOTE_META     = ROOT / "macro" / "dados_cpfl" / "lote_meta.json"
RESULTADO_CSV = ROOT / "macro" / "dados_cpfl" / "resultado_lote.csv"
ARQUIVO_DIR   = ROOT / "macro" / "dados_cpfl" / "arquivo"

BATCH = 500
SEP   = "=" * 70

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

SQL_INSERT_RESULTADO = """
INSERT INTO tabela_macros_cpfl
    (cliente_id, cliente_uc_id,
     resposta_id, pn, status, extraido,
     data_criacao, data_update, data_extracao)
SELECT
    cliente_id, COALESCE(%s, cliente_uc_id),
    %s, %s, %s, 0,
    NOW(), NOW(), NOW()
FROM tabela_macros_cpfl
WHERE id = %s AND status = 'processando'
"""

SQL_REVERTER_ORIGINAL = """
UPDATE tabela_macros_cpfl
SET status = 'pendente', data_update = NOW()
WHERE id = %s AND status = 'processando'
"""

SQL_RECUPERAR_ORFAOS = """
UPDATE tabela_macros_cpfl
SET status = 'pendente', data_update = NOW()
WHERE status = 'processando'
  AND data_update < NOW() - INTERVAL 2 HOUR
"""


# ---------------------------------------------------------------------------
# Carga do resultado
# ---------------------------------------------------------------------------

def carregar_resultado() -> pd.DataFrame:
    if not RESULTADO_CSV.exists():
        print(f"[ERRO] Arquivo de resultado não encontrado: {RESULTADO_CSV}")
        sys.exit(1)

    df = pd.read_csv(RESULTADO_CSV, dtype=str, sep=";", encoding="utf-8")
    df.columns = [c.strip() for c in df.columns]

    # Colunas obrigatórias
    for col in ("CPF", "UC"):
        if col not in df.columns:
            print(f"[ERRO] Coluna obrigatória '{col}' ausente no resultado.")
            sys.exit(1)

    # Colunas opcionais com default
    for col, default in [("PN", ""), ("ATIVO", ""), ("ERRO", "")]:
        if col not in df.columns:
            df[col] = default

    # Normaliza CPF e UC para cruzamento com meta
    df["_cpf_norm"] = df["CPF"].apply(
        lambda v: re.sub(r"\D", "", str(v or "")).zfill(11) if v else ""
    )
    df["_uc_norm"] = df["UC"].apply(
        lambda v: re.sub(r"\D", "", str(v or "")).zfill(10) if v else ""
    )
    return df


def carregar_meta() -> dict:
    if not LOTE_META.exists():
        print(f"[ERRO] Meta não encontrado: {LOTE_META}")
        sys.exit(1)
    with open(LOTE_META, encoding="utf-8") as f:
        return json.load(f)


def build_indice_meta(meta: dict) -> dict[tuple, int]:
    """Monta índice (cpf_norm, uc_norm) → macro_id."""
    import re as _re
    indice = {}
    for reg in meta.get("registros", []):
        cpf = _re.sub(r"\D", "", str(reg.get("cpf", ""))).zfill(11)
        uc  = _re.sub(r"\D", "", str(reg.get("uc", ""))).zfill(10)
        indice[(cpf, uc)] = reg["macro_id"]
    return indice


# ---------------------------------------------------------------------------
# Processamento
# ---------------------------------------------------------------------------

def processar(conn, df: pd.DataFrame, indice: dict, dry_run: bool):
    cur = conn.cursor()

    n_ok = n_sem_meta = n_erro = 0
    ids_processados = set()

    for _, row in df.iterrows():
        chave = (row["_cpf_norm"], row["_uc_norm"])
        macro_id = indice.get(chave)

        if macro_id is None:
            n_sem_meta += 1
            print(f"  [AVISO] CPF={row['CPF']} UC={row['UC']} não encontrado no meta — ignorado")
            continue

        rid, status, pn = interpretar_linha(dict(row))

        if dry_run:
            print(f"  [DRY-RUN] macro_id={macro_id} → status={status} pn={pn}")
            ids_processados.add(macro_id)
            n_ok += 1
            continue

        try:
            # Obtém cliente_uc_id do registro original para propagar
            cur.execute("SELECT cliente_uc_id FROM tabela_macros_cpfl WHERE id=%s", (macro_id,))
            res_uc = cur.fetchone()
            uc_id = res_uc[0] if res_uc else None

            cur.execute(SQL_INSERT_RESULTADO, (uc_id, rid, pn, status, macro_id))
            cur.execute(SQL_REVERTER_ORIGINAL, (macro_id,))
            conn.commit()
            ids_processados.add(macro_id)
            n_ok += 1
        except Exception as e:
            conn.rollback()
            n_erro += 1
            print(f"  [ERRO] macro_id={macro_id}: {e}")

    print(f"\n  Processados : {n_ok}  |  Sem meta: {n_sem_meta}  |  Erros: {n_erro}")

    # Recupera órfãos (ficaram em 'processando' sem retorno)
    if not dry_run:
        cur.execute(SQL_RECUPERAR_ORFAOS)
        conn.commit()
        if cur.rowcount:
            print(f"  [RECOVERY] {cur.rowcount} registros 'processando' revertidos para 'pendente'")

    cur.close()
    return n_ok, n_sem_meta, n_erro


def arquivar(dry_run: bool):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ARQUIVO_DIR.mkdir(parents=True, exist_ok=True)
    for arq in (LOTE_META, RESULTADO_CSV):
        if arq.exists():
            dest = ARQUIVO_DIR / f"{arq.stem}_{ts}{arq.suffix}"
            if not dry_run:
                shutil.move(str(arq), dest)
                print(f"  [ARQUIVO] {arq.name} → {dest.name}")
            else:
                print(f"  [DRY-RUN] arquivaria {arq.name} → {dest.name}")


def main():
    parser = argparse.ArgumentParser(
        description="Passo 3 da macro CPFL: processar retorno"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(SEP)
    print("PASSO 04 CPFL  –  Processar retorno macro")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteração será gravada")
    print(SEP)

    df   = carregar_resultado()
    meta = carregar_meta()
    idx  = build_indice_meta(meta)

    print(f"  Resultado : {len(df):,} linhas")
    print(f"  Meta      : {meta.get('total', '?')} registros")

    conn = pymysql.connect(**DB_CONFIG)
    try:
        n_ok, n_sem, n_err = processar(conn, df, idx, args.dry_run)
        arquivar(args.dry_run)

        print(f"\n{SEP}")
        print(f"PASSO 04 CPFL CONCLUÍDO")
        print(f"  Gravados  : {n_ok}")
        print(f"  Sem meta  : {n_sem}")
        print(f"  Erros     : {n_err}")
        print(SEP)

        if n_err:
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
