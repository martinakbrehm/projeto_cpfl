# ValidaPN GMP

Aplicação desktop em Python (Tkinter + Selenium) para validar automaticamente PN no portal GMP da CPFL a partir de uma planilha CSV.

## Objetivo

A ferramenta:

- lê um CSV de entrada com `CPF` e `UC`;
- acessa o portal GMP com login humano (incluindo captcha);
- consulta os dados no portal para cada linha;
- grava o resultado em um CSV de saída com colunas de retorno;
- permite pausa, retomada e intervenção humana em falhas críticas.

## Tecnologias usadas

- Python 3
- Tkinter (interface gráfica)
- Selenium (automação de navegador)
- webdriver-manager (instalação/gestão do ChromeDriver)
- pygame (alerta sonoro)
- python-dotenv (carregamento de credenciais via `.env`)
- cx_Freeze (empacotamento em `.exe`)

## Requisitos

- Windows
- Google Chrome instalado
- Python e dependências do `requirements.txt`
- Arquivo `.env` na raiz do projeto com:

```env
USUARIO1=seu_usuario
SENHA1=sua_senha
```

## Como executar (modo desenvolvimento)

1. Criar/ativar ambiente virtual.
2. Instalar dependências:

```bash
pip install -r requirements.txt
```

3. Rodar a aplicação:

```bash
python main.py
```

## Como gerar executável

```bash
python setup.py build
```

O executável é gerado em `build/V1.2/ValidaPN_GMP.exe` (ou pasta equivalente conforme versão/configuração de build).

## Formato esperado do CSV

- Delimitador: `;`
- Encoding de leitura: `latin-1`
- Colunas mínimas usadas pela automação:
  - `CPF`
  - `UC`

Na saída, o sistema grava as colunas:

- `CPF`
- `UC`
- `PN`
- `ATIVO`
- `ERRO`

## Fluxo de funcionamento

1. Usuário seleciona o CSV na interface.
2. Sistema verifica se já existe arquivo de resultado para retomar progresso.
3. Automação abre o Chrome e navega para login do GMP.
4. Usuário conclui autenticação/captcha.
5. Para cada linha do CSV, sistema consulta `UC + CPF` no portal.
6. Resultado é persistido no CSV de resultado.
7. Em erro crítico, abre janela de intervenção com opções:
   - Continuar
   - Relogar
   - Reiniciar
   - Parar

## Estrutura do projeto e explicação de cada arquivo

### Raiz

- `main.py`: ponto de entrada da aplicação. Cria a janela Tkinter e instancia a interface principal.
- `config.py`: define o diretório base da aplicação (script ou executável), carrega o `.env` e monta `config_usuario` com usuário/senha.
- `requirements.txt`: lista de dependências do projeto Python.
- `setup.py`: configuração de build com `cx_Freeze` para gerar executável Windows, incluindo assets e `.env`.
- `.env`: credenciais e variáveis de ambiente locais (arquivo sensível, não deve ser versionado com dados reais).

### Pasta `assets`

- `assets/alerta.mp3`: áudio usado no alerta sonoro durante intervenção humana.
- `assets/valida.ico`: ícone da aplicação.

### Pasta `core`

- `core/gerenciador_dados.py`: leitura do CSV de entrada, criação/manutenção do CSV de resultado, cálculo de progresso e finalização do processo.
- `core/portal_gmp.py`: encapsula a navegação no portal GMP (login, busca por PN, extração de resultados, logout).
- `core/tratador_erros.py`: detecta estados de erro no portal e define exceções de controle (`PortalIntervencaoHumana`, `ErroRecuperavelPortal`).
- `core/validador.py`: orquestra o fluxo completo de validação (driver, login, loop de linhas, retry/intervenção, callbacks de progresso).

### Pasta `interface`

- `interface/__init__.py`: inicializador do pacote de interface (arquivo vazio).
- `interface/interface_principal.py`: janela principal da aplicação (seleção de planilha, botões de controle, progresso e logs).
- `interface/dialogo_intervencao.py`: janela modal para decisões humanas quando a automação entra em estado de exceção.

### Pasta `interface/widgets`

- `interface/widgets/progresso_info.py`: widget com informações textuais de progresso (linha atual, total e percentual).
- `interface/widgets/widget_log.py`: widget de log expansível com níveis (INFO, SUCESSO, AVISO, ERRO).

### Pasta `utils`

- `utils/scraping.py`: utilitários Selenium (instância do driver, esperas, digitação humana, busca segura de elementos).
- `utils/notificador.py`: controle do alerta sonoro em thread separada (disparar/parar).

## Observações importantes

- O login no portal depende de resolução manual de captcha.
- A aplicação foi desenhada para permitir retomada de processamento com base no arquivo `_RESULTADO.csv`.
- O código assume que as colunas `CPF` e `UC` existem no CSV.
- Há mensagens e logs para suporte operacional, mas não há suíte de testes automatizados neste repositório no estado atual.
