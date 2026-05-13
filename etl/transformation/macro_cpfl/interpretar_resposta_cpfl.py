"""
interpretar_resposta_cpfl.py
============================
TRANSFORMATION -- Interpreta a resposta bruta da macro CPFL.

A macro CPFL retorna um CSV com colunas: CPF;UC;PN;ATIVO;ERRO

Mapeamento:
  ATIVO='S'                             -> ativo     (resposta_id=1)  "Instalacao ativa"
  ATIVO='N' + "Instalacao inativa"      -> inativo   (resposta_id=2)
  ATIVO='N' + "nao pertencem ao atual"  -> inativo   (resposta_id=3)
  ATIVO='N' + outros erros              -> inativo   (resposta_id=2)  <- default
  ATIVO='' / None                       -> pendente  (resposta_id=4)  <- volta para fila

Chamado por:
  etl/load/macro_cpfl/04_processar_retorno_cpfl.py
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Mapeamento fragmento de ERRO -> resposta_id
# (todos os casos ATIVO='N' resultam em status='inativo')
# ---------------------------------------------------------------------------
_ERROS_CPFL: list[tuple[str, int]] = [
    # fragmento (lower)                                        resposta_id
    ("nao pertencem ao atual titular",                        3),
    ("n\u00e3o pertencem ao atual titular",                   3),
    ("instala\u00e7\u00e3o inativa",                          2),
    ("instalacao inativa",                                    2),
]

_PADRAO_INATIVO  = (2, "inativo")   # N sem mensagem conhecida -> inativo generico
_PADRAO_PENDENTE = (4, "pendente")  # ATIVO vazio / nao chegou


def interpretar(ativo: str | None, erro: str | None) -> tuple[int, str, str | None]:
    """
    Interpreta os campos ATIVO e ERRO retornados pela macro CPFL.

    Retorna:
        (resposta_id, novo_status, None)
        O PN e tratado separadamente por 04_processar_retorno_cpfl.py.

    Parametros:
        ativo : 'S' | 'N' | '' | None
        erro  : mensagem de erro (campo ERRO do CSV) ou '' / None
    """
    ativo_norm = (ativo or "").strip().upper()
    erro_norm  = str(erro or "").strip()

    # Titularidade confirmada
    if ativo_norm == "S":
        return (1, "ativo", None)

    # ATIVO='N' -> analisa mensagem de erro para escolher resposta_id
    if ativo_norm == "N":
        erro_lower = erro_norm.lower()
        for fragmento, rid in _ERROS_CPFL:
            if fragmento in erro_lower:
                return (rid, "inativo", None)
        # Erro N sem mensagem conhecida -> inativo generico
        return (*_PADRAO_INATIVO, None)

    # ATIVO vazio / resposta nao chegou
    return (*_PADRAO_PENDENTE, None)


# ---------------------------------------------------------------------------
# Helpers para leitura em lote (usado por 04_processar_retorno_cpfl)
# ---------------------------------------------------------------------------

def interpretar_linha(row: dict) -> tuple[int, str, str | None]:
    """
    Interpreta uma linha do CSV resultado da macro CPFL.

    row deve ter chaves: 'ATIVO', 'ERRO', 'PN'  (case-insensitive via normalização no chamador)
    Retorna (resposta_id, novo_status, pn)
    """
    ativo = row.get("ATIVO") or row.get("ativo") or ""
    erro  = row.get("ERRO")  or row.get("erro")  or ""
    pn    = row.get("PN")    or row.get("pn")    or None

    rid, status, _ = interpretar(ativo, erro)

    # Normaliza PN: remove espaços, None se vazio
    pn_norm = str(pn).strip() if pn and str(pn).strip() else None

    return (rid, status, pn_norm)
