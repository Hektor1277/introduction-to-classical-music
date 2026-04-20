@echo off
setlocal
cd /d "%~dp0"
"%~dp0recording-retrieval-service.exe" --mode ui %*
set EXIT_CODE=%errorlevel%
if not "%EXIT_CODE%"=="0" (
  echo.
  echo Recording Retrieval Service failed to start.
  echo Review the startup message above. If another instance is already using port 4780, stop it and retry.
  pause
)
exit /b %EXIT_CODE%
