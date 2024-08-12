#!/bin/bash
# Author: @mpHcl
# Description: Reinstall the virtual environment
# This script is used to install (reinstall) the virtual environment.

echo "Deleting existing python virtual environment..."
rm -rf ./venv

echo "Creating new virtual environment..."
python3 -m venv ./venv

if [ -f ./venv/bin/activate ]; then
    echo "Virtual environment created successfully."
    echo "Installing dependencies..."
    source ./venv/bin/activate
    pip install -r requirements.txt
    echo "Dependencies installed successfully."
    deactivate
else
    echo "Error creating virtual environment."
fi