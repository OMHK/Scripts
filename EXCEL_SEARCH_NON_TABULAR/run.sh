#!/bin/bash

# Get the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Activate the virtual environment
source "$DIR/venv/bin/activate"

echo "Starting the data processing pipeline..."

# Run folder monitor and wait for a new file
echo "Monitoring folder for new files..."
NEW_FILE=$(python folder_monitor.py)

if [ -n "$NEW_FILE" ]; then
    echo "New file detected: $NEW_FILE"
    
    # Process the Excel file
    echo "Processing Excel file..."
    python EXCEL_SEARCH_NON_TABULAR.py
    
    # Upload to database
    echo "Uploading to database..."
    python db_uploader.py
    
    echo "Process completed successfully!"
else
    echo "No new files detected."
fi

# Deactivate virtual environment
deactivate