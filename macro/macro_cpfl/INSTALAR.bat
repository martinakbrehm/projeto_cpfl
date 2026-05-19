@echo off
REM ============================================================================
REM INSTALAR.bat  -  Configuracao do ambiente para a macro CPFL
REM ============================================================================
REM Executa uma unica vez no computador de destino.
REM Cria um venv local, instala dependencias com versoes pinadas.
REM ============================================================================

echo ======================================================================
echo   INSTALACAO - Macro CPFL (Orquestrador com Banco)
echo ======================================================================
echo.

REM --- Verificar Python ---
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Python nao encontrado no PATH.
    echo        Instale Python 3.12.x de https://www.python.org/downloads/
    echo        Marque "Add Python to PATH" durante a instalacao.
    pause
    exit /b 1
)

echo [OK] Python encontrado:
python --version
echo.

REM --- Criar venv ---
set VENV_DIR=%~dp0venv
if exist "%VENV_DIR%" (
    echo [INFO] venv ja existe em: %VENV_DIR%
    echo        Para reinstalar, delete a pasta venv e rode novamente.
) else (
    echo [INFO] Criando venv em: %VENV_DIR%
    python -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo [ERRO] Falha ao criar venv.
        pause
        exit /b 1
    )
    echo [OK] venv criado.
)
echo.

REM --- Ativar venv e instalar dependencias ---
echo [INFO] Instalando dependencias (versoes pinadas)...
call "%VENV_DIR%\Scripts\activate.bat"

pip install --upgrade pip >nul 2>&1
pip install -r "%~dp0requirements_pinned.txt"

if errorlevel 1 (
    echo.
    echo [ERRO] Falha ao instalar dependencias.
    echo        Verifique sua conexao com a internet.
    pause
    exit /b 1
)

echo.
echo [OK] Todas as dependencias instaladas.
echo.

REM --- Testar ambiente ---
echo ======================================================================
echo   TESTANDO AMBIENTE...
echo ======================================================================
echo.

python "%~dp0testar_ambiente.py"

echo.
echo ======================================================================
echo   INSTALACAO CONCLUIDA
echo ======================================================================
echo.
echo   Para executar a macro:
echo     - Duplo clique em EXECUTAR.bat
echo     - Ou: python executar_automatico.py
echo.

pause
