@echo off
REM Setup script for Bloomberg Python project

REM Check if venv exists
if not exist "venv\Scripts\activate.bat" (
    echo Error: Virtual environment not found!
    echo Please create a virtual environment first: python -m venv venv
    exit /b 1
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

echo.
echo Installing dependencies from requirements.txt...
pip install -r requirements.txt

if %ERRORLEVEL% EQU 0 (
    echo.
    echo SUCCESS: Dependencies installed successfully!
) else (
    echo.
    echo ERROR: Failed to install dependencies
    exit /b 1
)

echo.
echo Installation complete!
echo.
echo Note: blpapi must be installed separately using install_blpapi.bat
echo Run: install_blpapi.bat
echo.
pause
