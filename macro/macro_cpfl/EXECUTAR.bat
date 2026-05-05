@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Iniciando orquestrador CPFL (modo terminal)...
python executar_automatico.py --continuar %*
pause
