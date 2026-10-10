@echo off 
chcp 65001 >nul 
title Huddle Launcher 
 
cd /d "%%~dp0" 
 
echo [1/5] Проверка Node.js... 
where node >nul 2>nul 
if errorlevel 1 ( 
  echo Node.js не установлен! 
  pause 
  exit /b 1 
) 
echo [2/5] Проверка зависимостей... 
if not exist "node_modules\" call npm install 
echo [3/5] Проверка портов... 
taskkill /IM node.exe /F >nul 2>nul 
timeout /t 2 /nobreak >nul 
echo [4/5] Запуск сервера... 
start "Huddle Server" cmd /k "node server.js" 
timeout /t 3 /nobreak >nul 
echo [5/5] Запуск serve... 
start "Huddle Frontend" cmd /k "npx serve . -l 3000" 
timeout /t 3 /nobreak >nul 
start "" "http://localhost:3000" 
start "" "http://localhost:3000/admin.html" 
echo. 
echo Huddle запущен! 
pause
