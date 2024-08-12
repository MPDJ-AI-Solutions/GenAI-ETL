@echo off
REM Author: @mpHcl
REM Description: Reinstall the virtual environment
REM This script is used to install (reinstall) the virtual environment. 

echo Deleting existing python virtual environment...
rmdir /s /q venv

echo Creating new virtual environment...
python -m venv .\venv

if exist .\venv\Scripts\activate.bat (
    echo Virtual environment created successfully.
    echo Installing dependencies...
    call .\venv\Scripts\activate.bat
    pip install -r requirements.txt
    echo Dependencies installed successfully.
    call .\venv\Scripts\deactivate.bat
) else (
    echo Error creating virtual environment.
)