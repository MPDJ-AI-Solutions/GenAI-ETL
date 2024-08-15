@echo off
REM Author: @mpHcl
REM Description: Run the app
REM This script is used to run the application and check if OPEN_API_KEY is set 

REM Set path to the virtual environment and activate the virtual environment
cd ./venv/Scripts/
call activate.bat
cd ../../Src/App

REM Run the app
python -m main debug

cd ../..