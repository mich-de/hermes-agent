@echo off
setlocal
set "XDG_CONFIG_HOME=D:\CentroAI\data\opencode\config"
set "XDG_DATA_HOME=D:\CentroAI\data\opencode\data"
set "XDG_CACHE_HOME=D:\CentroAI\data\opencode\cache"
set "XDG_STATE_HOME=D:\CentroAI\data\opencode\state"
set "OPENCODE_DISABLE_AUTOUPDATE=1"
echo Testo opencode run con deepseek-v4.1-flash-free...
D:\CentroAI\app\opencode-cli\opencode.exe --standalone run --auto --format json -m opencode/deepseek-v4.1-flash-free "Rispondi solo: ciao, funziono."
echo.
echo Exit code: %ERRORLEVEL%
pause
