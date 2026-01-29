@echo off
REM Run Bloomberg fetch script

REM Check if venv exists
if not exist "venv\Scripts\activate.bat" (
    echo Error: Virtual environment not found!
    echo Please create a virtual environment first or run setup.bat
    exit /b 1
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

echo.
echo Running fetch.py...
python fetch.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Script completed successfully!
) else (
    echo.
    echo Script failed with error code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

echo.
pause
