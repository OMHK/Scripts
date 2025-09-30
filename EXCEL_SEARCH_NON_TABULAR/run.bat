@echo off
echo Starting the data processing pipeline...

:: Get the directory of the script
set "DIR=%~dp0"

:: Activate the virtual environment
call "%DIR%venv\Scripts\activate.bat"

:: Run folder monitor and wait for a new file
echo Monitoring folder for new files...
for /f "tokens=*" %%i in ('python folder_monitor.py') do set NEW_FILE=%%i

if defined NEW_FILE (
    echo New file detected: %NEW_FILE%
    
    :: Process the Excel file
    echo Processing Excel file...
    python EXCEL_SEARCH_NON_TABULAR.py
    
    :: Upload to database
    echo Uploading to database...
    python db_uploader.py
    
    echo Process completed successfully!
) else (
    echo No new files detected.
)

:: Deactivate virtual environment
deactivate
pause