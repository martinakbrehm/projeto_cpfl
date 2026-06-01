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
_CACHE_STATS_TTL = 3600  # segundos (1 hora)

# ---------------------------------------------------------------------------
# Tabelas materializadas — populadas pelas stored procedures
# sp_refresh_dashboard_macros_agg() / sp_refresh_dashboard_arquivos_agg().
# SELECT simples em tabelas indexadas: latência <1ms.
# ---------------------------------------------------------------------------
SQLs = {
    "macro": "SELECT * FROM dashboard_macros_agg ORDER BY dia DESC",
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
    """Chama sp_refresh_dashboard_macros_agg() no banco e invalida o cache local."""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("CALL sp_refresh_dashboard_macros_agg()")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERRO] sp_refresh_dashboard_macros_agg: {e}")
        return False
    invalidar_cache("macro")
    return True


# ---------------------------------------------------------------------------
# Estatísticas por arquivo de staging — tabela materializada
# ---------------------------------------------------------------------------
_SQL_ARQUIVOS_AGG = "SELECT * FROM dashboard_arquivos_agg ORDER BY data_carga DESC"


def carregar_stats_por_arquivo() -> pd.DataFrame:
    """Retorna estatísticas de todos os arquivos de staging.
    Lê diretamente da tabela materializada dashboard_arquivos_agg.
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
            cur.execute(_SQL_ARQUIVOS_AGG)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        conn.close()

        df = pd.DataFrame(rows, columns=cols)
        if df.empty:
            return df

        # Converte colunas numéricas (podem vir como Decimal do MySQL)
        for col in df.columns:
            if col not in ("arquivo", "data_carga"):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        if not df.empty:
            _CACHE_STATS["stats"] = (df, time.time())
        return df.copy()
    except Exception as e:
        print(f"[ERRO] carregar_stats_por_arquivo: {e}")
        return pd.DataFrame()


def refresh_dashboard_arquivos_agg() -> bool:
    """Chama sp_refresh_dashboard_arquivos_agg() no banco e invalida o cache local."""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("CALL sp_refresh_dashboard_arquivos_agg()")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERRO] sp_refresh_dashboard_arquivos_agg: {e}")
        return False
    invalidar_cache("stats")
    return True


