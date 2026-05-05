"""
interpretar_resposta_cpfl.py
============================
TRANSFORMATION — Interpreta a resposta bruta da macro CPFL.

A macro CPFL retorna um CSV com colunas: CPF;UC;PN;ATIVO;ERRO

Mapeamento:
  ATIVO='S'  (PN preenchido)  → consolidado  (resposta_id=1)
  ATIVO='N'  + "Instalação inativa"
                               → reprocessar  (resposta_id=2)
  ATIVO='N'  + "não pertencem ao atual titular"
                               → excluido     (resposta_id=3)
  ATIVO='N'  + "não cadastrado"
                               → excluido     (resposta_id=4)
  ATIVO=''   / None            → pendente     (resposta_id=5)  ← volta para fila
  demais erros / falha          → reprocessar  (resposta_id=6)

Chamado por:
  etl/load/macro_cpfl/04_processar_retorno_cpfl.py
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Mapeamento de fragmentos de mensagem de erro → (resposta_id, status)
# Ordem importa: mais específicas ANTES das genéricas.
# ---------------------------------------------------------------------------
_ERROS_CPFL: list[tuple[str, int, str]] = [
    # fragmento (lower)                               resposta_id  status
    ("não pertencem ao atual titular",               3,           "excluido"),
    ("nao pertencem ao atual titular",               3,           "excluido"),
    ("instalação inativa",                           2,           "reprocessar"),
    ("instalacao inativa",                           2,           "reprocessar"),
    ("não cadastrado",                               4,           "excluido"),
    ("nao cadastrado",                               4,           "excluido"),
    ("não encontrado",                               4,           "excluido"),
    ("nao encontrado",                               4,           "excluido"),
    ("cpf inválido",                                 4,           "excluido"),
    ("cpf invalido",                                 4,           "excluido"),
    ("erro",                                         6,           "reprocessar"),
]

_PADRAO_VAZIO        = (5, "pendente")    # ATIVO vazio / não chegou
_PADRAO_DESCONHECIDO = (6, "reprocessar") # erro não mapeado


def interpretar(ativo: str | None, erro: str | None) -> tuple[int, str, str | None]:
    """
    Interpreta os campos ATIVO e ERRO retornados pela macro CPFL.

    Retorna:
        (resposta_id, novo_status, pn_normalizado_or_None)
        — pn é passado separadamente; esta função não o manipula, mas
          o retorna para conveniência quando ATIVO='S'.

    Parâmetros:
        ativo  : 'S' | 'N' | '' | None
        erro   : mensagem de erro (campo ERRO do CSV) ou '' / None
    """
    ativo_norm = (ativo or "").strip().upper()
    erro_norm  = (erro  or "").strip()

    # Titularidade confirmada
    if ativo_norm == "S":
        return (1, "consolidado", None)

    # ATIVO='N' → analisa mensagem de erro
    if ativo_norm == "N":
        erro_lower = erro_norm.lower()
        for fragmento, rid, status in _ERROS_CPFL:
            if fragmento in erro_lower:
                return (rid, status, None)
        # Erro N sem mensagem conhecida → reprocessar
        return _PADRAO_DESCONHECIDO

    # ATIVO vazio / resposta não chegou
    if not ativo_norm:
        return (*_PADRAO_VAZIO, None)

    return _PADRAO_DESCONHECIDO


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
