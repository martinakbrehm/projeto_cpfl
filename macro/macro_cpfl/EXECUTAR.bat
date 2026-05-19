@echo off
chcp 65001 >nul
cd /d "%~dp0"

REM Usa venv local se existir (criado pelo INSTALAR.bat)
if exist "venv\Scripts\python.exe" (
    set PYTHON_EXE=venv\Scripts\python.exe
) else (
    set PYTHON_EXE=python
)

echo Iniciando orquestrador CPFL (modo terminal)...
echo Python: %PYTHON_EXE%
%PYTHON_EXE% executar_automatico.py --continuar %*
pause
