import sys
import time
from pathlib import Path

import pymysql
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import db_cpfl  # noqa: E402

DB_CONFIG = db_cpfl()

_CACHE: dict = {}        # cache por tipo {'macro': df} — sem TTL, vive durante o processo
_CACHE_STATS: dict = {}  # cache para stats_por_arquivo / cobertura
_CACHE_STATS_TTL = 300   # segundos (5 min)

# ---------------------------------------------------------------------------
# Tabela materializada — populada pela stored procedure
# sp_refresh_dashboard_macros_agg() chamada ao final do ETL.
# SELECT simples em tabela indexada: latência <1ms.
# ---------------------------------------------------------------------------
# Query direta na tabela_macros_cpfl + respostas (sem tabela materializada)
SQLs = {
    "macro": """
        SELECT
            DATE(tm.data_update)  AS dia,
            tm.status             AS status,
            r.mensagem            AS mensagem,
            r.status              AS resposta_status,
            NULL                  AS empresa,
            NULL                  AS fornecedor,
            NULL                  AS arquivo_origem,
            COUNT(*)              AS qtd
        FROM tabela_macros_cpfl tm
        JOIN respostas r ON r.id = tm.resposta_id
        WHERE tm.status NOT IN ('pendente', 'processando')
        GROUP BY DATE(tm.data_update), tm.status, r.mensagem, r.status
        ORDER BY dia DESC
    """,
}



def carregar_dados(tipo: str = "macro") -> pd.DataFrame:
    """Carrega dados do banco de dados.

    tipo: 'macro'
    Resultado é cacheado em memória por tipo (sem TTL — vive durante o processo).
    """
    tipo = tipo if tipo in SQLs else "macro"
    if tipo in _CACHE:
        return _CACHE[tipo].copy()

    query = SQLs[tipo]
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        conn.close()
        df = pd.DataFrame(rows, columns=cols)
        # Não cachear DataFrames vazios — podem ser resultado de race condition
        # com o refresh (TRUNCATE + INSERT) da stored procedure
        if not df.empty:
            _CACHE[tipo] = df
        return df.copy()
    except Exception as e:
        print(f"[ERRO] Falha ao carregar dados ({tipo}): {e}")
        return pd.DataFrame()


def invalidar_cache(tipo: str = None):
    """Remove o cache para forçar recarga na próxima chamada.
    Se 'stats' for passado, invalida apenas o cache de stats_por_arquivo."""
    if tipo == "stats":
        _CACHE_STATS.clear()
    elif tipo:
        _CACHE.pop(tipo, None)
    else:
        _CACHE.clear()
        _CACHE_STATS.clear()


def refresh_dashboard_macros_agg() -> bool:
    """Invalida o cache para forçar recarga na próxima leitura.
    (CPFL: sem tabela materializada, dados lidos diretamente.)
    """
    invalidar_cache("macro")
    return True


# ---------------------------------------------------------------------------
# Tabela materializada de arquivos — populada por sp_refresh_dashboard_arquivos_agg
# SELECT simples em tabela física indexada: latência <1ms.
# A query complexa (ROW_NUMBER + staging_import_rows) roda apenas na stored
# procedure, nunca diretamente no dashboard.
# ---------------------------------------------------------------------------
_SQL_STATS_ARQUIVO_MAT = """
    SELECT
        si.filename                                      AS arquivo,
        DATE(si.created_at)                              AS data_carga,
        si.rows_success                                  AS cpfs_no_arquivo,
        COUNT(DISTINCT tm.cliente_id)                    AS cpfs_processados,
        SUM(IF(tm.status='ativo',  1, 0))                AS ativos,
        SUM(IF(tm.status='inativo',1, 0))                AS inativos,
        COUNT(DISTINCT tm.cliente_id)                    AS cpfs_ineditos,
        COUNT(*)                                         AS ucs_ineditas,
        SUM(IF(tm.status NOT IN ('pendente','processando'),1,0)) AS combos_processadas,
        SUM(IF(tm.status='ativo',  1, 0))                AS combos_ativas,
        SUM(IF(tm.status='inativo',1, 0))                AS combos_inativas,
        0 AS ineditos_processados,
        0 AS ineditos_ativos,
        0 AS ineditos_inativos
    FROM staging_imports si
    LEFT JOIN tabela_macros_cpfl tm
           ON DATE(tm.data_criacao) = DATE(si.created_at)
    WHERE si.status = 'completed'
    GROUP BY si.id, si.filename, si.created_at, si.rows_success
    ORDER BY si.created_at DESC
"""


def carregar_stats_por_arquivo() -> pd.DataFrame:
    """Retorna estatísticas de todos os arquivos de staging.
    Lê da tabela materializada dashboard_arquivos_agg (SELECT simples).
    Cacheado em memória por _CACHE_STATS_TTL segundos.
    """
    cached = _CACHE_STATS.get("stats")
    if cached is not None:
        df_cached, ts = cached
        if time.time() - ts < _CACHE_STATS_TTL:
            return df_cached.copy()

    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(_SQL_STATS_ARQUIVO_MAT)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        conn.close()
        df = pd.DataFrame(rows, columns=cols)
        # Não cachear DataFrames vazios — podem ser resultado de race condition
        # com o refresh (TRUNCATE + INSERT) da stored procedure
        if not df.empty:
            _CACHE_STATS["stats"] = (df, time.time())
        return df.copy()
    except Exception as e:
        print(f"[ERRO] carregar_stats_por_arquivo: {e}")
        return pd.DataFrame()


_SQL_COBERTURA = """
    SELECT
        si.filename          AS arquivo,
        DATE(si.created_at)  AS data_carga,
        si.rows_success      AS total_combos,
        0                    AS combos_novas,
        0                    AS combos_existentes
    FROM staging_imports si
    WHERE si.status = 'completed'
    ORDER BY si.created_at DESC
"""


def carregar_cobertura() -> pd.DataFrame:
    """Retorna tabela de novos vs existentes por arquivo de staging."""
    cached = _CACHE_STATS.get("cobertura")
    if cached is not None:
        df_cached, ts = cached
        if time.time() - ts < _CACHE_STATS_TTL:
            return df_cached.copy()
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(_SQL_COBERTURA)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        conn.close()
        df = pd.DataFrame(rows, columns=cols)
        # Não cachear DataFrames vazios — race condition com TRUNCATE
        if not df.empty:
            _CACHE_STATS["cobertura"] = (df, time.time())
        return df.copy()
    except Exception as e:
        print(f"[ERRO] carregar_cobertura: {e}")
        return pd.DataFrame()


def refresh_dashboard_arquivos_agg() -> bool:
    """Invalida o cache de stats para forçar recarga na próxima leitura."""
    invalidar_cache("stats")
    return True


