#!/bin/bash
# Author: @mpHcl
# Description: Run the app
# This script is used to run the application and check if OPEN_API_KEY is set

# Set path to the virtual environment and activate the virtual environment
source ./venv/bin/activate
# Create a temporary 'cls' command
echo '#!/bin/bash' > /tmp/cls
echo 'clear' >> /tmp/cls

# Make it executable
chmod +x /tmp/cls

# Add /tmp to PATH temporarily
export PATH="/tmp:$PATH"


# Change directory to the application directory
cd ./Src/App

# Run the app
python -m main debug

cd ../..
rm /tmp/cls