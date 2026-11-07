@echo off
cd /d "%~dp0"
title Hermes Agent
:loop
python hermes.py
echo [Hermes: riavvio in 5 secondi...]
timeout /t 5 /nobreak >nul
goto loop
