@echo off
REM Install Bloomberg API in virtual environment

REM Check if venv exists
if not exist "venv\Scripts\activate.bat" (
    echo Error: Virtual environment not found!
    echo Please create a virtual environment first: python -m venv venv
    exit /b 1
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

echo Installing blpapi from Bloomberg repository...
pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple/ blpapi

if %ERRORLEVEL% EQU 0 (
    echo.
    echo SUCCESS: blpapi installed successfully!
) else (
    echo.
    echo ERROR: Failed to install blpapi
    echo Please check your Bloomberg Terminal access and network connection
    exit /b 1
)

echo.
echo Installation complete. You can now use the virtual environment.
pause
