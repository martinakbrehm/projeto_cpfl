"""
migrate_add_materialized_tables.py
===================================
Migração pontual: cria as 3 tabelas materializadas e as 3 stored procedures
do dashboard sem precisar re-executar o schema.sql completo.

Pode ser rodado quantas vezes quiser — é idempotente:
  - tabelas: CREATE TABLE IF NOT EXISTS
  - procedures: DROP IF EXISTS + CREATE (garante versão mais recente)
  - ao final: CALL em cada SP para popular as tabelas imediatamente

Uso:
    python db_cpfl/migrate_add_materialized_tables.py
    python db_cpfl/migrate_add_materialized_tables.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config import db_cpfl  # noqa: E402

SEP = "=" * 70

# ---------------------------------------------------------------------------
# DDL — Tabelas materializadas
# ---------------------------------------------------------------------------
TABLES = [
    (
        "dashboard_macros_agg",
        """
        CREATE TABLE IF NOT EXISTS dashboard_macros_agg (
          id              INT NOT NULL AUTO_INCREMENT,
          dia             DATE NOT NULL,
          status          VARCHAR(50) NOT NULL,
          mensagem        TEXT,
          resposta_status VARCHAR(50) DEFAULT NULL,
          empresa         VARCHAR(100) DEFAULT NULL,
          fornecedor      VARCHAR(100) DEFAULT NULL,
          arquivo_origem  VARCHAR(255) DEFAULT NULL,
          qtd             INT NOT NULL DEFAULT 0,
          PRIMARY KEY (id),
          INDEX idx_dma_dia    (dia),
          INDEX idx_dma_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ),
    (
        "dashboard_arquivos_agg",
        """
        CREATE TABLE IF NOT EXISTS dashboard_arquivos_agg (
          id                   INT NOT NULL AUTO_INCREMENT,
          arquivo              VARCHAR(255) NOT NULL,
          data_carga           DATE NOT NULL,
          cpfs_no_arquivo      INT NOT NULL DEFAULT 0,
          cpfs_processados     INT NOT NULL DEFAULT 0,
          ativos               INT NOT NULL DEFAULT 0,
          inativos             INT NOT NULL DEFAULT 0,
          cpfs_ineditos        INT NOT NULL DEFAULT 0,
          ucs_ineditas         INT NOT NULL DEFAULT 0,
          combos_processadas   INT NOT NULL DEFAULT 0,
          combos_ativas        INT NOT NULL DEFAULT 0,
          combos_inativas      INT NOT NULL DEFAULT 0,
          ineditos_processados INT NOT NULL DEFAULT 0,
          ineditos_ativos      INT NOT NULL DEFAULT 0,
          ineditos_inativos    INT NOT NULL DEFAULT 0,
          PRIMARY KEY (id),
          INDEX idx_daa_arquivo    (arquivo),
          INDEX idx_daa_data_carga (data_carga)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ),
    (
        "dashboard_cobertura_agg",
        """
        CREATE TABLE IF NOT EXISTS dashboard_cobertura_agg (
          id                 INT NOT NULL AUTO_INCREMENT,
          staging_id         INT NOT NULL,
          arquivo            VARCHAR(255) NOT NULL,
          data_carga         DATE NOT NULL,
          ucs_ineditas       INT NOT NULL DEFAULT 0,
          combos_processadas INT NOT NULL DEFAULT 0,
          pct_cobertura      DECIMAL(5,2) NOT NULL DEFAULT 0.00,
          PRIMARY KEY (id),
          UNIQUE KEY ux_cobertura_staging (staging_id),
          INDEX idx_dca_data_carga (data_carga)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ),
]

# ---------------------------------------------------------------------------
# DDL — Stored procedures (sem DELIMITER — executadas via pymysql)
# ---------------------------------------------------------------------------
PROCEDURES = [
    (
        "sp_refresh_dashboard_macros_agg",
        """
        CREATE PROCEDURE sp_refresh_dashboard_macros_agg()
        BEGIN
          TRUNCATE TABLE dashboard_macros_agg;
          INSERT INTO dashboard_macros_agg
            (dia, status, mensagem, resposta_status, empresa, fornecedor, arquivo_origem, qtd)
          SELECT
            DATE(tm.data_update),
            tm.status,
            r.mensagem,
            r.status,
            NULL,
            NULL,
            NULL,
            COUNT(*)
          FROM tabela_macros_cpfl tm
          JOIN respostas r ON r.id = tm.resposta_id
          WHERE tm.status NOT IN ('pendente', 'processando')
          GROUP BY DATE(tm.data_update), tm.status, r.mensagem, r.status
          ORDER BY DATE(tm.data_update) DESC;
        END
        """,
    ),
    (
        "sp_refresh_dashboard_arquivos_agg",
        """
        CREATE PROCEDURE sp_refresh_dashboard_arquivos_agg()
        BEGIN
          TRUNCATE TABLE dashboard_arquivos_agg;
          INSERT INTO dashboard_arquivos_agg
            (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados,
             ativos, inativos, cpfs_ineditos, ucs_ineditas,
             combos_processadas, combos_ativas, combos_inativas,
             ineditos_processados, ineditos_ativos, ineditos_inativos)
          WITH per_file AS (
            SELECT
              si.id               AS staging_id,
              si.filename         AS arquivo,
              DATE(si.created_at) AS data_carga,
              si.rows_success     AS cpfs_no_arquivo,
              COALESCE(cc.distinct_cpfs,   0) AS distinct_cpfs,
              COALESCE(cc.distinct_combos, 0) AS ucs_ineditas
            FROM staging_imports si
            LEFT JOIN (
              SELECT
                staging_id,
                COUNT(DISTINCT normalized_cpf)                AS distinct_cpfs,
                COUNT(DISTINCT normalized_cpf, normalized_uc) AS distinct_combos
              FROM staging_import_rows
              WHERE validation_status = 'valid'
              GROUP BY staging_id
            ) cc ON cc.staging_id = si.id
            WHERE si.status = 'completed'
          ),
          global_totals AS (
            SELECT
              SUM(ucs_ineditas) AS total_ucs,
              (SELECT COUNT(*) FROM tabela_macros_cpfl
                 WHERE status NOT IN ('pendente','processando')) AS g_processadas,
              (SELECT COUNT(*) FROM tabela_macros_cpfl
                 WHERE status = 'ativo')                        AS g_ativas,
              (SELECT COUNT(*) FROM tabela_macros_cpfl
                 WHERE status = 'inativo')                      AS g_inativas
            FROM per_file
          )
          SELECT
            pf.arquivo,
            pf.data_carga,
            pf.cpfs_no_arquivo,
            pf.distinct_cpfs                                          AS cpfs_processados,
            0                                                         AS ativos,
            0                                                         AS inativos,
            pf.distinct_cpfs                                          AS cpfs_ineditos,
            pf.ucs_ineditas,
            CASE WHEN gt.total_ucs > 0
              THEN ROUND(pf.ucs_ineditas / gt.total_ucs * gt.g_processadas)
              ELSE 0 END                                              AS combos_processadas,
            CASE WHEN gt.total_ucs > 0
              THEN ROUND(pf.ucs_ineditas / gt.total_ucs * gt.g_ativas)
              ELSE 0 END                                              AS combos_ativas,
            CASE WHEN gt.total_ucs > 0
              THEN ROUND(pf.ucs_ineditas / gt.total_ucs * gt.g_inativas)
              ELSE 0 END                                              AS combos_inativas,
            0, 0, 0
          FROM per_file pf
          CROSS JOIN global_totals gt
          ORDER BY pf.data_carga DESC;
        END
        """,
    ),
    (
        "sp_refresh_dashboard_cobertura_agg",
        """
        CREATE PROCEDURE sp_refresh_dashboard_cobertura_agg()
        BEGIN
          TRUNCATE TABLE dashboard_cobertura_agg;
          INSERT INTO dashboard_cobertura_agg
            (staging_id, arquivo, data_carga, ucs_ineditas, combos_processadas, pct_cobertura)
          WITH per_file AS (
            SELECT
              si.id               AS staging_id,
              si.filename         AS arquivo,
              DATE(si.created_at) AS data_carga,
              COALESCE(cc.distinct_combos, 0) AS ucs_ineditas
            FROM staging_imports si
            LEFT JOIN (
              SELECT
                staging_id,
                COUNT(DISTINCT normalized_cpf, normalized_uc) AS distinct_combos
              FROM staging_import_rows
              WHERE validation_status = 'valid'
              GROUP BY staging_id
            ) cc ON cc.staging_id = si.id
            WHERE si.status = 'completed'
          ),
          global_totals AS (
            SELECT
              SUM(ucs_ineditas) AS total_ucs,
              (SELECT COUNT(*) FROM tabela_macros_cpfl
                 WHERE status NOT IN ('pendente','processando')) AS g_processadas
            FROM per_file
          )
          SELECT
            pf.staging_id,
            pf.arquivo,
            pf.data_carga,
            pf.ucs_ineditas,
            CASE WHEN gt.total_ucs > 0
              THEN ROUND(pf.ucs_ineditas / gt.total_ucs * gt.g_processadas)
              ELSE 0 END AS combos_processadas,
            CASE WHEN pf.ucs_ineditas > 0 AND gt.total_ucs > 0
              THEN ROUND(
                (pf.ucs_ineditas / gt.total_ucs * gt.g_processadas) / pf.ucs_ineditas * 100,
                2)
              ELSE 0.00 END AS pct_cobertura
          FROM per_file pf
          CROSS JOIN global_totals gt;
        END
        """,
    ),
]


def run(dry_run: bool = False):
    print(SEP)
    print("MIGRAÇÃO: tabelas materializadas + stored procedures do dashboard")
    if dry_run:
        print("  [DRY-RUN] nenhuma alteração será gravada")
    print(SEP)

    if dry_run:
        print("\n[TABELAS]")
        for name, _ in TABLES:
            print(f"  CREATE TABLE IF NOT EXISTS {name}")
        print("\n[STORED PROCEDURES]")
        for name, _ in PROCEDURES:
            print(f"  DROP PROCEDURE IF EXISTS {name}")
            print(f"  CREATE PROCEDURE {name}(...)")
        print("\n[REFRESH INICIAL]")
        for name, _ in PROCEDURES:
            print(f"  CALL {name}()")
        print("\n[DRY-RUN] Nenhuma alteração foi executada.")
        return

    conn = pymysql.connect(**db_cpfl(autocommit=True))
    cur = conn.cursor()
    errors = 0

    # 1. Criar tabelas
    print("\n[1/3] Criando tabelas materializadas...")
    for name, ddl in TABLES:
        try:
            cur.execute(ddl)
            cur.execute(f"SELECT COUNT(*) FROM {name}")
            n = cur.fetchone()[0]
            print(f"  [OK] {name}  ({n} linhas existentes)")
        except Exception as e:
            print(f"  [ERRO] {name}: {e}")
            errors += 1

    # 2. Criar stored procedures (drop + create para garantir versão atual)
    print("\n[2/3] Criando stored procedures...")
    for name, ddl in PROCEDURES:
        try:
            cur.execute(f"DROP PROCEDURE IF EXISTS {name}")
            cur.execute(ddl)
            print(f"  [OK] {name}")
        except Exception as e:
            print(f"  [ERRO] {name}: {e}")
            errors += 1

    # 3. Refresh inicial — popula as tabelas imediatamente
    print("\n[3/3] Executando refresh inicial...")
    for name, _ in PROCEDURES:
        try:
            cur.execute(f"CALL {name}()")
            table = name.replace("sp_refresh_", "").replace("()", "")
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            n = cur.fetchone()[0]
            print(f"  [OK] CALL {name}()  →  {n} linhas")
        except Exception as e:
            print(f"  [ERRO] CALL {name}(): {e}")
            errors += 1

    conn.close()

    print()
    print(SEP)
    if errors == 0:
        print("  Migração concluída com sucesso.")
    else:
        print(f"  Migração concluída com {errors} ERRO(S). Verifique o log acima.")
    print(SEP)

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migração: tabelas materializadas do dashboard")
    parser.add_argument("--dry-run", action="store_true",
                        help="Exibe o que seria executado sem fazer alterações")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
