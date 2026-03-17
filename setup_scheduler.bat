@echo off
REM Sets up Windows Task Scheduler to run lead_aggregator.py daily at 12:00 PM
REM Run this file as Administrator once

set TASK_NAME=LeadAggregatorDaily
set SCRIPT_PATH=D:\projects\ProPlusData\lead_aggregator.py
set PYTHON_PATH=C:\Users\gaurav.kashyap\AppData\Local\Programs\Python\Python313\python.exe
set LOG_DIR=D:\projects\ProPlusData

echo Creating scheduled task: %TASK_NAME%

schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "\"%PYTHON_PATH%\" \"%SCRIPT_PATH%\"" ^
  /sc daily ^
  /st 12:00 ^
  /ru "%USERNAME%" ^
  /rl highest ^
  /f

if %ERRORLEVEL% == 0 (
    echo.
    echo SUCCESS: Task "%TASK_NAME%" scheduled to run daily at 12:00 PM
    echo To verify: schtasks /query /tn "%TASK_NAME%"
    echo To run now: schtasks /run /tn "%TASK_NAME%"
    echo To delete:  schtasks /delete /tn "%TASK_NAME%" /f
) else (
    echo FAILED: Could not create task. Try running this file as Administrator.
)

pause
