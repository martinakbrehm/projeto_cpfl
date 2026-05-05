@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Abrindo Painel de Controle CPFL...
python painel.py
pause
