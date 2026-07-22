@echo off
setlocal
cd /d "%~dp0.."

python -m pip install -r .exe\requirements-desktop.txt || exit /b 1
python -m PyInstaller --noconfirm --clean --onefile --windowed ^
  --name SDGun-Market ^
  --specpath packaging ^
  --distpath .exe ^
  --add-data "%CD%\web;web" ^
  --collect-all webview ^
  desktop_app.py

if errorlevel 1 exit /b %errorlevel%
echo.
echo Build complete: %CD%\.exe\SDGun-Market.exe
endlocal
