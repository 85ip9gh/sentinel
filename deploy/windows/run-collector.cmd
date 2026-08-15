@echo off
rem Wrapper for the scheduled task. Task Scheduler has no clean way to set
rem environment variables, so they are set here next to the command.

setlocal
set "REPO=%~dp0..\.."
cd /d "%REPO%"

if "%SENTINEL_ROLE%"=="" set "SENTINEL_ROLE=workstation"
if "%SENTINEL_BOOTSTRAP%"=="" set "SENTINEL_BOOTSTRAP=127.0.0.1:9092"
if "%SENTINEL_SPOOL_DIR%"=="" set "SENTINEL_SPOOL_DIR=%REPO%\var\spool"
if "%SENTINEL_HTTP_TARGETS%"=="" set "SENTINEL_HTTP_TARGETS=https://pesanth.com,https://cubestore.pesanth.com,https://carsale.pesanth.com"

"%REPO%\.venv\Scripts\python.exe" -m collector %*
