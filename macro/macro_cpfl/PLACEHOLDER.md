# macro/macro_cpfl/
#
# Esta pasta receberá a macro de automação CPFL (equivalente a
# macro/macro/consulta_contrato.py para a Neoenergia).
#
# Arquivos esperados quando a macro for integrada:
#   consulta_cpfl.py          — script principal da macro
#   executar_automatico_cpfl.py — orquestrador dos 3 passos:
#       1. etl/extraction/macro_cpfl/03_buscar_lote_cpfl.py
#       2. consulta_cpfl.py
#       3. etl/load/macro_cpfl/04_processar_retorno_cpfl.py
#   EXECUTAR.bat              — atalho Windows
#   requirements.txt          — dependências específicas
#
# Fluxo de arquivos:
#   ENTRADA : macro/dados_cpfl/lote_pendente.csv   (CPF;UC)
#   SAÍDA   : macro/dados_cpfl/resultado_lote.csv  (CPF;UC;PN;ATIVO;ERRO)
