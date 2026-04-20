@echo off
setlocal

set "ROOT=%~dp0.."
cd /d "%ROOT%"

node scripts/dev-server.mjs status all
