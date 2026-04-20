@echo off
setlocal
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set ROOT_DIR=%%~fI
cd /d "%ROOT_DIR%"

if exist "%SCRIPT_DIR%recording-retrieval-service.exe" (
  "%SCRIPT_DIR%recording-retrieval-service.exe" --mode ui %*
  set EXIT_CODE=%errorlevel%
  goto finish
)

if exist "%ROOT_DIR%\recording-retrieval-service.exe" (
  "%ROOT_DIR%\recording-retrieval-service.exe" --mode ui %*
  set EXIT_CODE=%errorlevel%
  goto finish
)

if exist "%ROOT_DIR%\.venv\Scripts\python.exe" (
  "%ROOT_DIR%\.venv\Scripts\python.exe" -m app.main --mode ui %*
  set EXIT_CODE=%errorlevel%
  goto finish
)

python -m app.main --mode ui %*
set EXIT_CODE=%errorlevel%

:finish
if not "%EXIT_CODE%"=="0" (
  echo.
  echo Recording Retrieval Service failed to start.
  echo Review the startup message above. A stale portable build or an occupied port 4780 are the most common causes.
  pause
)
exit /b %EXIT_CODE%
