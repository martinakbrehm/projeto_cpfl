# projeto_orquestracao_cpfl

Pipeline de orquestração end-to-end para automação da validação de titularidade de clientes junto ao portal GMP da CPFL. O sistema integra ingestão de dados brutos, processamento ETL, execução de automação Selenium e exposição de métricas em dashboard analítico — tudo sobre um banco MySQL gerenciado na AWS RDS.

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                              │
│  Arquivos CSV (CPF, UC, nome)  →  dados/                            │
└───────────────────────┬─────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CAMADA DE INGESTÃO (ETL Load)                  │
│  01_staging_import_cpfl.py   — hash de arquivo, dedup, staging      │
│  02_processar_staging_cpfl.py — normalização CPF/UC, upsert         │
│                                 clientes + cliente_uc               │
└───────────────────────┬─────────────────────────────────────────────┘
                        │  tabela_macros_cpfl (status=pendente)
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CAMADA DE ORQUESTRAÇÃO                         │
│  03_buscar_lote_cpfl.py  — prioridade pendente > reprocessar,       │
│                            marca status=processando, exporta CSV    │
│  executar_cpfl.py        — runner Selenium headless (portal GMP)    │
│  04_processar_retorno_cpfl.py — interpreta ATIVO+ERRO, insere       │
│                                 resultado, arquiva lote             │
└───────────────────────┬─────────────────────────────────────────────┘
                        │  tabela_macros_cpfl (status=consolidado|excluido|reprocessar)
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CAMADA ANALÍTICA                               │
│  dashboard_macros_agg (tabela materializada)                        │
│  dashboard_arquivos_agg / dashboard_cobertura_agg                   │
│  Dashboard Dash/Plotly  →  http://127.0.0.1:8050                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Modelo de Dados

**Banco:** `bd_Automacoes_time_dados_cpfl` (MySQL 8, AWS RDS)

```
clientes ─────────────────────────────────────────────────┐
  id PK | cpf UNIQUE | nome | data_nascimento              │
                                                           │
cliente_uc ───────────────────────────────────────────────┤
  id PK | cliente_id FK | uc UNIQUE(cliente_id, uc)        │
                                                           │
tabela_macros_cpfl ───────────────────────────────────────┘
  id PK | cliente_id FK | cliente_uc_id FK                 
  resposta_id FK | pn | status ENUM | extraido             
  data_criacao | data_update | data_extracao               

respostas          → catálogo de respostas do portal GMP
telefones          → 1:N por cliente
enderecos          → 1:N por cliente/UC
staging_imports    → controle de importações (idempotência por hash)
staging_import_rows → linhas brutas com status de validação
```

**Ciclo de vida do status em `tabela_macros_cpfl`:**

```
pendente → processando → consolidado   (titularidade confirmada)
                       → reprocessar   (instalação inativa / erro temporário)
                       → excluido      (CPF/UC não pertence ao titular)
```

O modelo é **append-only**: cada ciclo insere um novo registro de resultado preservando o histórico completo de consultas por CPF+UC.

---

## Estrutura do Projeto

```
projeto_orquestracao_cpfl/
│
├── config.py                        # Credenciais — NÃO versionado (.gitignore)
├── config.example.py                # Template público de credenciais
│
├── db_cpfl/
│   ├── schema.sql                   # DDL completo: tabelas, índices, triggers
│   └── setup_database.py            # Aplica schema via pymysql (idempotente)
│
├── dados/                           # CSVs de entrada — NÃO versionados
│
├── etl/
│   ├── extraction/macro_cpfl/
│   │   └── 03_buscar_lote_cpfl.py   # Extração priorizada do banco → CSV
│   ├── load/macro_cpfl/
│   │   ├── 01_staging_import_cpfl.py    # Ingestão com dedup por hash de arquivo
│   │   ├── 02_processar_staging_cpfl.py # Validação, normalização, upsert
│   │   └── 04_processar_retorno_cpfl.py # Carga dos resultados da macro
│   └── transformation/macro_cpfl/
│       └── interpretar_resposta_cpfl.py # Regras: ATIVO+ERRO → (resposta_id, status, pn)
│
├── macro/
│   ├── dados_cpfl/                  # Arquivos de lote em trânsito — NÃO versionados
│   └── macro_cpfl/
│       ├── painel.py                # GUI tkinter: painel liga/desliga com log
│       ├── executar_automatico.py   # Orquestrador: loop extract→macro→load
│       ├── PAINEL.bat
│       ├── EXECUTAR.bat
│       └── valida_pn_gmp-main/      # Pacote Selenium — portal GMP
│           ├── executar_cpfl.py     # Entry point CLI (headless, sem tkinter)
│           ├── config.py            # Importa credenciais do config.py raiz
│           ├── core/                # PortalGMP, Validador, GerenciadorDados
│           ├── interface/           # UI standalone (uso manual)
│           └── utils/               # Scraping helpers, notificador
│
└── dashboard_macros/
    ├── dashboard.py                 # App Dash: layout, callbacks, autenticação
    ├── data/loader.py               # SQL + cache em memória (sem TTL)
    ├── service/orchestrator.py      # Filtros, agregações, build de tabelas
    └── refresh_scheduler.py         # Scheduler: refresh das tabelas materializadas
```

---

## Setup

### 1. Credenciais

```powershell
cp config.example.py config.py
# Edite config.py com as credenciais do banco MySQL e do portal GMP
```

`config.py` define:
- `db_cpfl()` — conexão ao banco CPFL (AWS RDS)
- `GMP_USUARIOS` — lista de usuários do portal `gmp.cpfl.com.br`
- `gmp_usuario(indice)` — helper para rotação de contas

> `config.py` está no `.gitignore`. **Nunca versionar.**

### 2. Banco de Dados

```powershell
python db_cpfl/setup_database.py
```

O script é idempotente (`CREATE TABLE IF NOT EXISTS`, `INSERT ... ON DUPLICATE KEY UPDATE`).

### 3. Dependências

```powershell
pip install pymysql pandas dash dash-auth plotly
pip install selenium python-dotenv  # para a macro
```

---

## Execução do Pipeline

### Etapa 1 — Importar arquivos CSV

```powershell
python etl/load/macro_cpfl/01_staging_import_cpfl.py
```

Faz hash dos arquivos em `dados/`, ignora reimportações, popula `staging_import_rows`.

### Etapa 2 — Processar staging

```powershell
python etl/load/macro_cpfl/02_processar_staging_cpfl.py
```

Normaliza CPF (LPAD 11 dígitos), valida UC, faz upsert em `clientes` e `cliente_uc`, enfileira registros `pendente` em `tabela_macros_cpfl`.

### Etapa 3 — Rodar a macro (ciclo contínuo)

**Pelo painel (recomendado):**

```powershell
cd macro\macro_cpfl
PAINEL.bat
```

**Pelo terminal:**

```powershell
cd macro\macro_cpfl
python executar_automatico.py --tamanho 500 --pausa 60
```

| Parâmetro | Padrão | Descrição |
|-----------|--------|-----------|
| `--tamanho` | 500 | Registros por lote |
| `--pausa` | 60 | Pausa (s) entre ciclos |
| `--max-erros` | 3 | Interrompe após N erros consecutivos |
| `--continuar` | — | Retoma lote existente sem limpeza |

O orquestrador executa automaticamente:
1. `03_buscar_lote_cpfl.py` — extrai lote priorizado e marca `processando`
2. `executar_cpfl.py` — Selenium consulta cada CPF+UC no portal GMP
3. `04_processar_retorno_cpfl.py` — interpreta resultados, insere em `tabela_macros_cpfl`, reverte registros não processados para `reprocessar`

### Etapa 4 — Dashboard

```powershell
python -m dashboard_macros
```

Acesse em [http://127.0.0.1:8050](http://127.0.0.1:8050). O dashboard lê de tabelas materializadas (`dashboard_macros_agg`, `dashboard_arquivos_agg`) atualizadas pelo scheduler interno.

---

## Decisões de Engenharia

| Decisão | Justificativa |
|---------|---------------|
| Modelo append-only em `tabela_macros_cpfl` | Preserva histórico completo; permite auditoria e reprocessamento sem perda de dados |
| Staging com hash de arquivo | Garante idempotência na ingestão — reimportar o mesmo CSV não gera duplicatas |
| Status `processando` explícito | Permite detectar e reverter lotes interrompidos (crash recovery) |
| Tabelas materializadas no dashboard | Desacopla leitura analítica da carga transacional; latência de query <1ms |
| Credenciais centralizadas em `config.py` | Fonte única de verdade; `.gitignore` protege todos os projetos dependentes |
| Runner CLI separado da GUI | `executar_cpfl.py` pode ser chamado por qualquer orquestrador; GUI é opcional |

---

## Git

```powershell
git remote set-url origin https://github.com/martinakbrehm/projeto_cpfl.git

git add -A
git commit -m "tipo: descrição"
git push
```
