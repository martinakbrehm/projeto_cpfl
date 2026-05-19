# Macro CPFL — Orquestrador com Banco

Automação do portal GMP (CPFL) integrada ao banco de dados.  
Busca lotes pendentes, valida PN via Selenium, e atualiza o resultado no banco.

---

## Pré-requisitos

| Item | Detalhes |
|------|----------|
| Python | 3.12.x |
| Chrome/Chromium | Versão 148.x (mesma do chromedriver) |
| Rede | Acesso ao banco MySQL (AWS RDS) e ao portal `gmp.cpfl.com.br` |
| Interface gráfica | Necessária — o portal tem captcha manual |

---

## Instalação

### Linux

```bash
cd projeto_orquestracao_cpfl/macro/macro_cpfl/

# 1. Dar permissão aos scripts
chmod +x INSTALAR.sh EXECUTAR.sh
chmod +x valida_pn_gmp-main/chromedriver

# 2. Rodar instalador (cria venv + testa tudo)
./INSTALAR.sh
```

### Windows

```
1. Navegar até: projeto_orquestracao_cpfl\macro\macro_cpfl\
2. Duplo clique em INSTALAR.bat
```

### O que o instalador faz:

1. Verifica se Python está disponível
2. Cria um `venv/` local
3. Instala dependências com versões exatas (`requirements_pinned.txt`)
4. Roda `testar_ambiente.py` que valida:
   - Imports (selenium, pymysql, pandas, cryptography)
   - Conexão ao banco
   - ChromeDriver funcional
   - Estrutura de arquivos

---

## Execução

### Linux

```bash
cd projeto_orquestracao_cpfl/macro/macro_cpfl/
./EXECUTAR.sh
```

### Windows

```
Duplo clique em EXECUTAR.bat
```

### O que acontece ao executar:

1. **Busca lote** — consulta banco, pega registros `pendente`, gera `lote_pendente.csv`
2. **Abre Chrome** — acessa portal GMP, preenche login
3. **Você resolve o captcha** — interação manual única por ciclo
4. **Processa automaticamente** — valida PN de cada CPF/UC do lote
5. **Salva no banco** — atualiza status (ativo/inativo) e arquiva resultado
6. **Repete** — busca próximo lote e reinicia o ciclo

---

## Diagnóstico

Se algo der errado, rode o teste de ambiente:

```bash
# Linux
source venv/bin/activate
python3 testar_ambiente.py

# Windows
venv\Scripts\activate
python testar_ambiente.py
```

---

## Estrutura

```
macro/macro_cpfl/
├── EXECUTAR.bat / .sh        ← Ponto de entrada
├── INSTALAR.bat / .sh        ← Setup (uma vez)
├── executar_automatico.py    ← Orquestrador (ciclo banco→macro→banco)
├── requirements_pinned.txt   ← Dependências com versões exatas
├── testar_ambiente.py        ← Diagnóstico
└── valida_pn_gmp-main/
    ├── chromedriver(.exe)    ← Driver do Chrome (Win + Linux)
    ├── config.py             ← Importa credenciais GMP do config.py raiz
    ├── executar_cpfl.py      ← Runner CLI da macro Selenium
    ├── core/
    │   ├── validador.py      ← Coordenador do fluxo
    │   ├── portal_gmp.py     ← Interação com portal GMP
    │   ├── gerenciador_dados.py  ← Leitura/escrita CSV
    │   └── tratador_erros.py     ← Retry + intervenções
    └── utils/
        └── scraping.py       ← Selenium helpers (driver, waits, etc)
```

---

## Credenciais

As credenciais ficam em `projeto_orquestracao_cpfl/config.py` (raiz do projeto).  
Este arquivo **não é versionado** (está no `.gitignore`).

Use `config.example.py` como referência para criar o seu.

---

## Parâmetros opcionais

```bash
./EXECUTAR.sh --tamanho 300        # Registros por lote (padrão: 500)
./EXECUTAR.sh --pausa 120          # Pausa entre ciclos em segundos (padrão: 60)
./EXECUTAR.sh --max-erros 5        # Para após N erros seguidos (padrão: 3)
```
