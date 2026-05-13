"""
02_processar_staging_cpfl.py
============================
Passo 2 do pipeline CPFL.

Lê staging_imports com status='completed' que ainda tenham linhas não
processadas e insere nas tabelas de produção:

  clientes -> cliente_uc -> tabela_macros_cpfl -> telefones -> enderecos

Regras:
  • Se o cliente (CPF) não existe em `clientes`, cria.
  • Se a UC não existe em `cliente_uc` para esse cliente, cria.
  • Se a combinação CPF+UC já tem registro em `tabela_macros_cpfl`
    com status NOT IN ('ativo','inativo'), o registro é ignorado (não duplica).
  • Telefones e endereços são inseridos sem duplicatas (chave por
    (cliente_id, telefone) e (cliente_uc_id, cep)).

Idempotente: pode ser re-executado sem duplicar registros.

Uso:
    python etl/load/macro_cpfl/02_processar_staging_cpfl.py
    python etl/load/macro_cpfl/02_processar_staging_cpfl.py --staging-id 3
    python etl/load/macro_cpfl/02_processar_staging_cpfl.py --dry-run
"""

import argparse
import re
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_cpfl  # noqa: E402

DB_CONFIG = db_cpfl(autocommit=False, read_timeout=600, write_timeout=600)

RESPOSTA_PENDENTE = 4   # id em `respostas` -> 'Aguardando processamento'
BATCH             = 5_000
SEP               = "=" * 70

# Índices secundários de tabela_macros_cpfl que são dropados antes do bulk
# e recriados depois para máxima velocidade de INSERT.
_MACROS_SECONDARY_INDEXES = [
    ("idx_cpfl_macros_status_data",     "(status, data_update, cliente_id)"),
    ("idx_cpfl_macros_cliente_data",    "(cliente_id, data_update)"),
    ("idx_cpfl_macros_extraido_status", "(extraido, status, data_update)"),
    ("idx_cpfl_macros_resposta",        "(resposta_id)"),
    ("idx_cpfl_macros_data_extracao",   "(data_extracao)"),
    ("idx_cpfl_macros_pn",              "(pn)"),
]


# ---------------------------------------------------------------------------
# Bulk INSERT real (multi-values em uma única query)
# pymysql.executemany faz 1 query por linha — inaceitável para 3.7M rows.
# ---------------------------------------------------------------------------
MAX_RETRIES    = 3
RETRIABLE_ERRS = {1213, 2013, 2006, 1205}


def _bulk_insert(cur, conn, sql_prefix: str, rows: list):
    """
    Executa um INSERT multi-values em uma única query.
    sql_prefix: ex. "INSERT IGNORE INTO clientes (cpf, nome) VALUES"
    rows: lista de tuplas com os valores
    """
    if not rows:
        return
    placeholders = "(" + ",".join(["%s"] * len(rows[0])) + ")"
    values_clause = ",".join([placeholders] * len(rows))
    flat = [v for row in rows for v in row]

    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            cur.execute(f"{sql_prefix} {values_clause}", flat)
            return
        except pymysql.err.OperationalError as e:
            errno = e.args[0] if e.args else 0
            if errno not in RETRIABLE_ERRS or tentativa == MAX_RETRIES:
                raise
            wait = 2 ** tentativa
            print(f"    [RETRY] errno={errno} tentativa {tentativa}/{MAX_RETRIES}, aguardando {wait}s...")
            try:
                conn.rollback()
            except Exception:
                pass
            time.sleep(wait)
            try:
                conn.ping(reconnect=True)
                cur = conn.cursor()
            except Exception:
                pass


def _executemany_retry(cur, conn, sql, rows):
    """Mantido por compatibilidade — delega ao bulk insert."""
    prefix = sql.split("VALUES")[0].strip() + " VALUES"
    _bulk_insert(cur, conn, prefix, rows)


# ---------------------------------------------------------------------------
# Normalização
# ---------------------------------------------------------------------------

def norm_str(val, maxlen: int = 255) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s[:maxlen] if s else None


def norm_uf(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = re.sub(r"[^A-Za-z]", "", str(val).strip())
    return s[:2].upper() if len(s) >= 2 else None


def norm_telefone(val) -> tuple[int | None, str | None]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    if not s or len(s) < 8 or len(s) > 13:
        return None, None
    parte_num = s[-9:] if len(s) >= 9 and s[-9] in "9" else s[-8:]
    tipo = "celular" if len(parte_num) == 9 else "fixo"
    try:
        return int(s), tipo
    except ValueError:
        return None, None


# ---------------------------------------------------------------------------
# Leitura do CSV (mesma lógica do passo 1)
# ---------------------------------------------------------------------------

def ler_arquivo(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath, dtype=str, sep=";", encoding="utf-8",
                     on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]
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


def colunas_tel(df: pd.DataFrame) -> list[str]:
    return [f"tel_{i}" for i in range(1, 8) if f"tel_{i}" in df.columns]


# ---------------------------------------------------------------------------
# Conexão e carregamento de maps de deduplicação
# ---------------------------------------------------------------------------

def conectar():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()
    for var in ("wait_timeout", "interactive_timeout"):
        cur.execute(f"SET SESSION {var} = 28800")
    cur.execute("SET SESSION net_read_timeout = 600")
    cur.execute("SET SESSION net_write_timeout = 600")
    # Acelera bulk inserts
    cur.execute("SET SESSION foreign_key_checks = 0")
    cur.execute("SET SESSION unique_checks = 0")
    cur.close()
    return conn


def _drop_secondary_indexes(conn):
    """Dropa índices secundários de tabela_macros_cpfl para acelerar bulk INSERT."""
    cur = conn.cursor()
    for nome, _ in _MACROS_SECONDARY_INDEXES:
        try:
            cur.execute(f"ALTER TABLE tabela_macros_cpfl DROP INDEX {nome}")
        except Exception:
            pass
    conn.commit()
    cur.close()
    print("  [PERF] Índices secundários de tabela_macros_cpfl removidos para bulk insert.")


def _recreate_secondary_indexes(conn):
    """Recria os índices secundários após o bulk INSERT."""
    cur = conn.cursor()
    for nome, cols in _MACROS_SECONDARY_INDEXES:
        try:
            cur.execute(f"ALTER TABLE tabela_macros_cpfl ADD INDEX {nome} {cols}")
        except Exception:
            pass
    conn.commit()
    cur.close()
    print("  [PERF] Índices secundários de tabela_macros_cpfl recriados.")


def lookup_chunk(cur, cpfs: set, ucs_por_cpf: dict) -> tuple[dict, dict, set, set, set]:
    """Busca no banco apenas os CPFs/UCs presentes no chunk atual.
    Muito mais eficiente que carregar tudo em memória.
    """
    if not cpfs:
        return {}, {}, set(), set(), set()

    ph = ",".join(["%s"] * len(cpfs))
    cpfs_list = list(cpfs)

    cur.execute(f"SELECT cpf, id FROM clientes WHERE cpf IN ({ph})", cpfs_list)
    cpf_map = {r[0]: r[1] for r in cur.fetchall()}

    cids = list(cpf_map.values())
    uc_map: dict = {}
    macros_set: set = set()
    tel_set: set = set()
    end_set: set = set()

    if cids:
        ph_c = ",".join(["%s"] * len(cids))

        cur.execute(
            f"SELECT cliente_id, uc, id FROM cliente_uc WHERE cliente_id IN ({ph_c})",
            cids,
        )
        uc_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}

        cur.execute(
            f"SELECT cliente_id, COALESCE(cliente_uc_id,0) "
            f"FROM tabela_macros_cpfl WHERE cliente_id IN ({ph_c}) AND status NOT IN ('ativo','inativo')",
            cids,
        )
        macros_set = {(r[0], r[1]) for r in cur.fetchall()}

        cur.execute(
            f"SELECT cliente_id, telefone FROM telefones "
            f"WHERE cliente_id IN ({ph_c}) AND telefone IS NOT NULL",
            cids,
        )
        tel_set = {(r[0], int(r[1])) for r in cur.fetchall()}

        cur.execute(
            f"SELECT COALESCE(cliente_uc_id,0), COALESCE(cep,'') "
            f"FROM enderecos WHERE cliente_id IN ({ph_c})",
            cids,
        )
        end_set = {(r[0], str(r[1]).strip()) for r in cur.fetchall()}

    return cpf_map, uc_map, macros_set, tel_set, end_set


# ---------------------------------------------------------------------------
# Processamento de um staging_imports
# ---------------------------------------------------------------------------

def processar_staging(conn, staging_id: int, dry_run: bool) -> dict:
    cur = conn.cursor()

    cur.execute("SELECT filename FROM staging_imports WHERE id=%s", (staging_id,))
    row = cur.fetchone()
    if not row:
        print(f"  [ERRO] staging_id={staging_id} não encontrado.")
        cur.close()
        return {}

    filepath = ROOT / row[0]

    # Linhas válidas ainda não processadas
    cur.execute(
        "SELECT row_idx, normalized_cpf, normalized_uc FROM staging_import_rows "
        "WHERE staging_id=%s AND validation_status='valid' AND processed_at IS NULL",
        (staging_id,),
    )
    valid_rows: dict[int, tuple[str, str]] = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    if not valid_rows:
        print(f"  [INFO] staging_id={staging_id}: nenhuma linha válida pendente.")
        cur.close()
        return {"staging_id": staging_id, "processadas": 0}

    df = ler_arquivo(filepath)
    tel_cols = colunas_tel(df)

    print(f"\n  staging_id={staging_id}  |  {filepath.name}")
    print(f"  linhas_válidas={len(valid_rows):,}")

    cur.close()
    conn.commit()

    data_criacao = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    stats = {k: 0 for k in
             ("clientes_novos", "uc_novas", "macros_novas",
              "telefones", "enderecos", "processadas", "erros")}

    cur_w = conn.cursor()
    df_validas = df[df.index.isin(valid_rows.keys())]
    all_idxs   = list(df_validas.index)

    for chunk_start in range(0, len(all_idxs), BATCH):
        chunk_idxs = all_idxs[chunk_start: chunk_start + BATCH]
        chunk_df   = df_validas.loc[chunk_idxs]

        # ── Parsear chunk ────────────────────────────────────────────────
        parsed = []
        for idx, row in chunk_df.iterrows():
            norm_c, norm_u = valid_rows.get(idx, (None, None))
            if not norm_c or not norm_u:
                stats["erros"] += 1
                continue

            tels = []
            for col in tel_cols:
                tv, tipo = norm_telefone(row.get(col))
                if tv:
                    tels.append((tv, tipo))

            parsed.append({
                "idx":       idx,
                "cpf":       norm_c,
                "uc":        norm_u,
                "nome":      norm_str(row.get("nome"), 255),
                "tels":      tels,
                "endereco":  norm_str(row.get("endereco"), 255),
                "cidade":    norm_str(row.get("cidade"), 100),
                "uf":        norm_uf(row.get("uf")),
                "cep":       norm_str(row.get("cep"), 20),
            })

        # ── Lookup lazy no banco apenas para os CPFs deste chunk ─────────
        if not parsed:
            continue

        cpfs_chunk_all = {d["cpf"] for d in parsed}
        cpf_map, uc_map, macros_set, tel_set, end_set = lookup_chunk(
            cur_w, cpfs_chunk_all, {}
        )

        # Deduplicar CPF+UC dentro do chunk
        seen = set()
        parsed_novo = []
        for d in parsed:
            k = (d["cpf"], d["uc"])
            if k not in seen:
                seen.add(k)
                parsed_novo.append(d)
        if not parsed_novo:
            continue

        # ── FASE 1: clientes ─────────────────────────────────────────────
        cpfs_chunk = {d["cpf"] for d in parsed_novo}
        novos_cpfs = cpfs_chunk - cpf_map.keys()

        if not dry_run:
            if novos_cpfs:
                nome_por_cpf = {d["cpf"]: d["nome"] for d in parsed_novo}
                _executemany_retry(cur_w, conn,
                    "INSERT IGNORE INTO clientes (cpf, nome, data_criacao) VALUES (%s,%s,%s)",
                    [(c, nome_por_cpf[c], data_criacao) for c in novos_cpfs],
                )
                stats["clientes_novos"] += cur_w.rowcount
                # Busca apenas os CPFs recém-inseridos (não os já no map)
                ph = ",".join(["%s"] * len(novos_cpfs))
                cur_w.execute(f"SELECT id, cpf FROM clientes WHERE cpf IN ({ph})",
                              list(novos_cpfs))
                cpf_map.update({r[1]: r[0] for r in cur_w.fetchall()})
        else:
            for i, d in enumerate(parsed_novo):
                if d["cpf"] not in cpf_map:
                    cpf_map[d["cpf"]] = -(chunk_start + i + 1)
                    stats["clientes_novos"] += 1

        # ── FASE 2: cliente_uc ───────────────────────────────────────────
        chaves_uc = set()
        for d in parsed_novo:
            cid = cpf_map.get(d["cpf"])
            if cid:
                chaves_uc.add((cid if cid > 0 else 0, d["uc"]))

        novas_ucs = chaves_uc - uc_map.keys()

        if not dry_run:
            if novas_ucs:
                _executemany_retry(cur_w, conn,
                    "INSERT IGNORE INTO cliente_uc (cliente_id, uc, data_criacao) VALUES (%s,%s,%s)",
                    [(cid, u, data_criacao) for cid, u in novas_ucs],
                )
                stats["uc_novas"] += cur_w.rowcount
                # Busca apenas as UCs recém-inseridas
                cids_novos = {t[0] for t in novas_ucs}
                ph = ",".join(["%s"] * len(cids_novos))
                cur_w.execute(
                    f"SELECT id, cliente_id, uc FROM cliente_uc WHERE cliente_id IN ({ph})",
                    list(cids_novos),
                )
                for r in cur_w.fetchall():
                    uc_map[(r[1], r[2])] = r[0]
        else:
            for i, chave in enumerate(novas_ucs):
                uc_map[chave] = -(chunk_start + i + 1)
                stats["uc_novas"] += 1

        # ── FASE 3: tabela_macros_cpfl ───────────────────────────────────
        rows_macros = []
        for d in parsed_novo:
            cid = cpf_map.get(d["cpf"])
            if not cid:
                continue
            cid_k = cid if cid > 0 else 0
            uc_id = uc_map.get((cid_k, d["uc"])) or 0
            chave = (cid_k, uc_id)
            if chave not in macros_set:
                rows_macros.append((cid, uc_id or None, RESPOSTA_PENDENTE, 'pendente', data_criacao))
                macros_set.add(chave)
                stats["macros_novas"] += 1

        if rows_macros and not dry_run:
            _executemany_retry(cur_w, conn,
                "INSERT INTO tabela_macros_cpfl "
                "(cliente_id, cliente_uc_id, resposta_id, status, data_criacao) "
                "VALUES (%s,%s,%s,%s,%s)",
                rows_macros,
            )

        # ── FASE 4: telefones ────────────────────────────────────────────
        rows_tels = []
        for d in parsed_novo:
            cid = cpf_map.get(d["cpf"])
            if not cid:
                continue
            cid_k = cid if cid > 0 else 0
            for tv, tipo in d["tels"]:
                chave_tel = (cid_k, tv)
                if chave_tel not in tel_set:
                    rows_tels.append((cid, tv, tipo, data_criacao))
                    tel_set.add(chave_tel)
                    stats["telefones"] += 1

        if rows_tels and not dry_run:
            _executemany_retry(cur_w, conn,
                "INSERT INTO telefones (cliente_id, telefone, tipo, data_criacao) "
                "VALUES (%s,%s,%s,%s)",
                rows_tels,
            )

        # ── FASE 5: enderecos ────────────────────────────────────────────
        rows_ends = []
        for d in parsed_novo:
            if not d["endereco"]:
                continue
            cid = cpf_map.get(d["cpf"])
            if not cid:
                continue
            cid_k = cid if cid > 0 else 0
            uc_id = uc_map.get((cid_k, d["uc"]))
            if not uc_id:
                continue
            uc_id_real = uc_id if not dry_run else 1
            cep_key    = d["cep"] or ""
            chave_end  = (uc_id_real, cep_key)
            if chave_end not in end_set:
                rows_ends.append((
                    cid, uc_id_real,
                    d["endereco"], d["cidade"], d["uf"], d["cep"],
                    data_criacao,
                ))
                end_set.add(chave_end)
                stats["enderecos"] += 1

        if rows_ends and not dry_run:
            _executemany_retry(cur_w, conn,
                "INSERT INTO enderecos "
                "(cliente_id, cliente_uc_id, endereco, cidade, uf, cep, data_criacao) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                rows_ends,
            )

        # ── FASE 6: checkpoint por batch ─────────────────────────────
        # Marca TODAS as linhas do chunk (incluindo duplicatas já existentes)
        chunk_processed = list(chunk_idxs)
        stats["processadas"] += len(parsed_novo)  # conta só as realmente novas

        if not dry_run:
            conn.commit()
            # Marca processed_at imediatamente após o commit do batch
            ph = ",".join(["%s"] * len(chunk_processed))
            cur_w.execute(
                f"UPDATE staging_import_rows SET processed_at=NOW() "
                f"WHERE staging_id=%s AND row_idx IN ({ph})",
                [staging_id] + chunk_processed,
            )
            conn.commit()

        pct = stats["processadas"] / len(valid_rows) * 100
        print(
            f"    {stats['processadas']:>7,}/{len(valid_rows):,} ({pct:.0f}%)"
            f"  clientes={stats['clientes_novos']}"
            f"  uc={stats['uc_novas']}"
            f"  macros={stats['macros_novas']}"
            f"  tel={stats['telefones']}"
            f"  end={stats['enderecos']}"
        )

    # ── Atualiza contador final do staging ──────────────────────────────────
    if not dry_run:
        cur_w.execute(
            "UPDATE staging_imports SET rows_success=%s WHERE id=%s",
            (stats["processadas"], staging_id),
        )
        conn.commit()
        print(f"  [INFO] processed_at marcado em {stats['processadas']:,} linhas.")

    cur_w.close()
    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Processar staging CPFL -- Passo 2")
    parser.add_argument("--staging-id", type=int, default=None,
                        help="Processar apenas um staging_id específico")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(SEP)
    print("PASSO 02 CPFL  -  Processar staging -> produção")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteração será gravada")
    print(SEP)

    conn = conectar()
    cur = conn.cursor()

    if args.staging_id:
        ids = [args.staging_id]
    else:
        cur.execute(
            """SELECT si.id FROM staging_imports si
               WHERE si.status = 'completed'
                 AND EXISTS (
                     SELECT 1 FROM staging_import_rows sir
                     WHERE sir.staging_id = si.id
                       AND sir.validation_status = 'valid'
                       AND sir.processed_at IS NULL
                 )
               ORDER BY si.id"""
        )
        ids = [r[0] for r in cur.fetchall()]
    cur.close()

    if not ids:
        print("  [INFO] Nenhum staging pendente para processar.")
        conn.close()
        return

    print(f"  Stagings pendentes: {len(ids)}")

    totais = {k: 0 for k in
              ("clientes_novos", "uc_novas", "macros_novas",
               "telefones", "enderecos", "processadas", "erros")}

    if not args.dry_run:
        _drop_secondary_indexes(conn)

    try:
        for sid in ids:
            stats = processar_staging(conn, sid, args.dry_run)
            for k in totais:
                totais[k] += stats.get(k, 0)
    finally:
        if not args.dry_run:
            _recreate_secondary_indexes(conn)
        conn.close()

    print(f"\n{SEP}")
    print("PASSO 02 CPFL CONCLUÍDO")
    labels = {
        "processadas":    "Linhas processadas ",
        "clientes_novos": "Clientes novos     ",
        "uc_novas":       "UCs novas          ",
        "macros_novas":   "Macros inseridas   ",
        "telefones":      "Telefones inseridos",
        "enderecos":      "Endereços inseridos",
        "erros":          "Erros              ",
    }
    for k, label in labels.items():
        print(f"  {label} : {totais[k]:>10,}")
    print(SEP)


if __name__ == "__main__":
    main()
