"""
05_importar_historico_cpfl.py
=============================
ETAPA MANUAL — Importação de resultados históricos da macro CPFL.

Contexto:
  Os arquivos PARTE6 e PARTE7 tiveram seus CPF+UC rodados manualmente no
  portal GMP antes da automação existir. Os resultados foram salvos em CSV
  mas nunca foram inseridos no banco via pipeline ETL.

  Este script lê esses arquivos históricos e insere os resultados no banco
  exatamente como se tivessem sido processados pela macro automatizada
  (passo 04), preservando as datas originais dos registros pendentes.

Responsabilidade:
  1. Lê arquivos CSV da pasta dados/historicos/:
       Colunas: CPF;UC;PN;ATIVO;ERRO
  2. Carrega em memória todos os registros 'pendente' relevantes do banco
     via lookup bulk por CPF (performance — evita query por linha).
  3. Interpreta ATIVO+ERRO via interpretar_resposta_cpfl.py.
  4. Insere EM LOTE novos registros com status ativo/inativo, usando as
     MESMAS datas do registro pendente original (data_criacao, data_update,
     data_extracao) — garante consistencia visual no dashboard.
  5. O registro 'pendente' original permanece intocado (historico preservado).
  6. Registros do CSV nao encontrados no banco sao reportados mas ignorados.
  7. CPF+UC que ja tem resultado ativo/inativo no banco sao pulados.

Datas:
  data_criacao  = data_criacao  do registro pendente original
  data_update   = data_update   do registro pendente original
  data_extracao = data_criacao  do registro pendente original

Uso:
    python etl/load/macro_cpfl/05_importar_historico_cpfl.py
    python etl/load/macro_cpfl/05_importar_historico_cpfl.py --dry-run
    python etl/load/macro_cpfl/05_importar_historico_cpfl.py --arquivo dados/historicos/Assisty_CPFL_PARTE6_new_RESULTADO.csv
    python etl/load/macro_cpfl/05_importar_historico_cpfl.py --verbose
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_cpfl  # noqa: E402

sys.path.insert(0, str(ROOT / "etl" / "transformation" / "macro_cpfl"))
from interpretar_resposta_cpfl import interpretar_linha  # noqa: E402

DB_CONFIG     = db_cpfl(autocommit=False)
HISTORICO_DIR = ROOT / "dados" / "historicos"
BATCH_INSERT  = 500
SEP           = "=" * 70

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

SQL_BUSCAR_PENDENTES_BULK = """
    SELECT
        c.cpf,
        cu.uc,
        tm.id           AS macro_id,
        tm.cliente_id,
        tm.cliente_uc_id,
        tm.data_criacao,
        tm.data_update
    FROM tabela_macros_cpfl tm
    JOIN clientes c    ON c.id  = tm.cliente_id
    JOIN cliente_uc cu ON cu.id = tm.cliente_uc_id
    WHERE c.cpf IN ({placeholders})
      AND tm.status = 'pendente'
    ORDER BY tm.data_criacao ASC
"""

SQL_JA_PROCESSADOS_BULK = """
    SELECT c.cpf, cu.uc
    FROM tabela_macros_cpfl tm
    JOIN clientes c    ON c.id  = tm.cliente_id
    JOIN cliente_uc cu ON cu.id = tm.cliente_uc_id
    WHERE c.cpf IN ({placeholders})
      AND tm.status IN ('ativo', 'inativo')
"""

SQL_INSERT_RESULTADO = """
    INSERT INTO tabela_macros_cpfl
        (cliente_id, cliente_uc_id,
         resposta_id, pn, status, extraido,
         data_criacao, data_update, data_extracao)
    VALUES
        (%s, %s,
         %s, %s, %s, 0,
         %s, %s, %s)
"""


# ---------------------------------------------------------------------------
# Normalizacao
# ---------------------------------------------------------------------------

def norm_cpf(val) -> str:
    return re.sub(r"\D", "", str(val or "").split(".")[0]).zfill(11)


def norm_uc(val) -> str:
    return re.sub(r"\D", "", str(val or "").split(".")[0]).zfill(10)


# ---------------------------------------------------------------------------
# Leitura dos arquivos historicos
# ---------------------------------------------------------------------------

def listar_arquivos(pasta: Path, arquivo_especifico: Path | None) -> list[Path]:
    if arquivo_especifico:
        if not arquivo_especifico.exists():
            print(f"[ERRO] Arquivo nao encontrado: {arquivo_especifico}")
            sys.exit(1)
        return [arquivo_especifico]
    arquivos = sorted(pasta.glob("*.csv"))
    if not arquivos:
        print(f"[ERRO] Nenhum arquivo .csv encontrado em {pasta}")
        sys.exit(1)
    return arquivos


def ler_historico(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath, dtype=str, sep=";", encoding="utf-8",
                     on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]

    for col in ("CPF", "UC"):
        if col not in df.columns:
            print(f"[ERRO] Coluna obrigatoria '{col}' ausente em {filepath.name}")
            sys.exit(1)

    for col, default in [("PN", ""), ("ATIVO", ""), ("ERRO", "")]:
        if col not in df.columns:
            df[col] = default

    df["_cpf"] = df["CPF"].apply(norm_cpf)
    df["_uc"]  = df["UC"].apply(norm_uc)

    before = len(df)
    df = df[(df["_cpf"].str.len() == 11) & (df["_uc"].str.len() == 10)]
    dropped = before - len(df)
    if dropped:
        print(f"  [AVISO] {dropped} linhas descartadas por CPF/UC invalido")

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Lookup bulk no banco
# ---------------------------------------------------------------------------

def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def buscar_pendentes(cur, cpfs: list[str]) -> dict:
    """
    Retorna dict (cpf, uc) -> (macro_id, cliente_id, cliente_uc_id, data_criacao, data_update).
    Quando ha multiplos pendentes para o mesmo CPF+UC, usa o mais antigo (ORDER BY ASC).
    """
    resultado = {}
    for chunk in _chunks(cpfs, 1000):
        ph = ",".join(["%s"] * len(chunk))
        cur.execute(SQL_BUSCAR_PENDENTES_BULK.format(placeholders=ph), chunk)
        for cpf, uc, macro_id, cli_id, uc_id, dt_cria, dt_upd in cur.fetchall():
            chave = (cpf.zfill(11), uc.zfill(10))
            if chave not in resultado:  # guarda apenas o mais antigo
                resultado[chave] = (macro_id, cli_id, uc_id, dt_cria, dt_upd)
    return resultado


def buscar_ja_processados(cur, cpfs: list[str]) -> set:
    """Retorna set de (cpf, uc) que ja tem resultado ativo/inativo."""
    existentes = set()
    for chunk in _chunks(cpfs, 1000):
        ph = ",".join(["%s"] * len(chunk))
        cur.execute(SQL_JA_PROCESSADOS_BULK.format(placeholders=ph), chunk)
        for cpf, uc in cur.fetchall():
            existentes.add((cpf.zfill(11), uc.zfill(10)))
    return existentes


# ---------------------------------------------------------------------------
# Processamento
# ---------------------------------------------------------------------------

def _reconectar() -> pymysql.connections.Connection:
    """Cria uma nova conexao com o banco (usado apos timeout/desconexao)."""
    import time as _time
    for tentativa in range(1, 6):
        try:
            c = pymysql.connect(**DB_CONFIG)
            print(f"  [RECONEXAO] Conectado (tentativa {tentativa})")
            return c
        except Exception as e:
            print(f"  [RECONEXAO] Falha {tentativa}/5: {e}")
            _time.sleep(5 * tentativa)
    raise RuntimeError("Nao foi possivel reconectar ao banco apos 5 tentativas.")


def processar_arquivo(conn, df: pd.DataFrame, nome: str,
                      dry_run: bool, verbose: bool) -> dict:
    cur = conn.cursor()
    stats = {
        "total":         len(df),
        "inseridos":     0,
        "ja_processado": 0,
        "sem_pendente":  0,
        "erros":         0,
    }

    cpfs_unicos = df["_cpf"].unique().tolist()
    print(f"  CPFs unicos  : {len(cpfs_unicos):,}")
    print(f"  Carregando lookup de pendentes do banco...")

    pendentes      = buscar_pendentes(cur, cpfs_unicos)
    ja_processados = buscar_ja_processados(cur, cpfs_unicos)

    print(f"  Pendentes encontrados no banco : {len(pendentes):,}")
    print(f"  Ja processados no banco        : {len(ja_processados):,}")
    print(f"  Processando linhas...")

    batch_params = []

    for i, row in df.iterrows():
        chave = (row["_cpf"], row["_uc"])

        if chave in ja_processados:
            stats["ja_processado"] += 1
            if verbose:
                print(f"  [JA_PROCESSADO] CPF={chave[0]} UC={chave[1]}")
            continue

        if chave not in pendentes:
            stats["sem_pendente"] += 1
            if verbose:
                print(f"  [SEM_PENDENTE] CPF={chave[0]} UC={chave[1]}")
            continue

        macro_id, cliente_id, cliente_uc_id, data_criacao, data_update = pendentes[chave]

        rid, status, pn = interpretar_linha(dict(row))

        if dry_run:
            if verbose:
                print(f"  [DRY-RUN] CPF={chave[0]} UC={chave[1]} -> {status} pn={pn} data={data_criacao}")
            stats["inseridos"] += 1
            continue

        batch_params.append((
            cliente_id, cliente_uc_id,
            rid, pn or None, status,
            data_criacao,   # data_criacao
            data_update,    # data_update
            data_criacao,   # data_extracao
        ))
        stats["inseridos"] += 1

        if len(batch_params) >= BATCH_INSERT:
            tentativas = 0
            while tentativas < 5:
                try:
                    conn.ping(reconnect=True)
                    cur = conn.cursor()
                    cur.executemany(SQL_INSERT_RESULTADO, batch_params)
                    conn.commit()
                    print(f"  [{stats['inseridos']:,}/{stats['total']:,}] commit parcial ({len(batch_params)} registros)...")
                    batch_params = []
                    break
                except Exception as e:
                    tentativas += 1
                    print(f"  [ERRO] batch falhou (tentativa {tentativas}/5): {e}")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    if tentativas >= 5:
                        stats["erros"] += len(batch_params)
                        stats["inseridos"] -= len(batch_params)
                        batch_params = []
                    else:
                        import time as _t; _t.sleep(3 * tentativas)

    if batch_params and not dry_run:
        try:
            conn.ping(reconnect=True)
            cur = conn.cursor()
            cur.executemany(SQL_INSERT_RESULTADO, batch_params)
            conn.commit()
            print(f"  Commit final: {len(batch_params)} registros")
        except Exception as e:
            conn.rollback()
            stats["erros"] += len(batch_params)
            stats["inseridos"] -= len(batch_params)
            print(f"  [ERRO] commit final falhou: {e}")

    cur.close()
    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Passo 05 CPFL: importar resultados historicos da macro"
    )
    parser.add_argument(
        "--arquivo",
        type=Path,
        default=None,
        help="Caminho para um arquivo CSV especifico (default: todos em dados/historicos/)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula o processamento sem gravar no banco"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Exibe detalhes de cada linha processada"
    )
    args = parser.parse_args()

    print(SEP)
    print("PASSO 05 CPFL  -  Importar resultados historicos da macro")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteracao sera gravada no banco")
    print(SEP)

    arquivos = listar_arquivos(HISTORICO_DIR, args.arquivo)
    print(f"  Arquivos encontrados: {len(arquivos)}")
    for a in arquivos:
        print(f"    * {a.name}")
    print()

    conn = pymysql.connect(**DB_CONFIG)
    totais = {"total": 0, "inseridos": 0, "ja_processado": 0,
              "sem_pendente": 0, "erros": 0}

    try:
        for arq in arquivos:
            print(f"{'-'*50}")
            print(f"  Processando: {arq.name}")
            df = ler_historico(arq)
            print(f"  Linhas validas: {len(df):,}")

            stats = processar_arquivo(conn, df, arq.name, args.dry_run, args.verbose)

            print(f"  Resultado {arq.name}:")
            print(f"    Inseridos       : {stats['inseridos']:,}")
            print(f"    Ja processados  : {stats['ja_processado']:,}")
            print(f"    Sem pendente    : {stats['sem_pendente']:,}")
            print(f"    Erros           : {stats['erros']:,}")

            for k in totais:
                totais[k] += stats[k]

        print()
        print(SEP)
        print("PASSO 05 CPFL CONCLUIDO")
        print(f"  Total linhas    : {totais['total']:,}")
        print(f"  Inseridos       : {totais['inseridos']:,}")
        print(f"  Ja processados  : {totais['ja_processado']:,}")
        print(f"  Sem pendente    : {totais['sem_pendente']:,}")
        print(f"  Erros           : {totais['erros']:,}")
        print(SEP)

        if totais["erros"]:
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
