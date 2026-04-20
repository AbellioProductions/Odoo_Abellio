@echo off
setlocal enabledelayedexpansion

set REQ_PYTHON=3.11
set PY_CMD=python

py --version >nul 2>&1
if %ERRORLEVEL% equ 0 (
    set PY_CMD=py -%REQ_PYTHON%
) else (
    python --version >nul 2>&1
    if %ERRORLEVEL% neq 0 goto :INSTALL_PYTHON
)

goto :CHECK_DEPS

:INSTALL_PYTHON
echo [SYS] Python %REQ_PYTHON% not found. Starting deployment...
curl -L -o py_inst.exe https://www.python.org/ftp/python/3.11.8/python-3.11.8-amd64.exe

echo [SYS] Running silent installation...
start /wait py_inst.exe /quiet InstallAllUsers=1 PrependPath=1 Include_test=0 Shortcuts=0 Include_launcher=1
del py_inst.exe

echo [WARN] Installation finished. 
echo [WARN] To refresh PATH, you MUST open a new terminal window.
echo [WARN] Trying to continue using Python Launcher...

set PY_CMD=py -%REQ_PYTHON%
%PY_CMD% --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERR] Environment not updated. Please restart console.
    pause
    exit /b
)

:CHECK_DEPS
echo [SYS] Using: %PY_CMD%
%PY_CMD% -m pip install --upgrade pip

echo [SYS] Syncing dependencies (pyads, requests)...
%PY_CMD% -m pip install pyads requests

echo [READY] Industrial IoT environment is active.
pause