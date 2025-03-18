REM filepath: c:\Projects\SoftwareLab_PreThesis\kafka_pysubg\mark3\start_project.bat
@echo off

REM Activate the Conda environment in the current shell
echo Activating Conda environment...
call conda.bat activate subgrp-proj
if errorlevel 1 (
    echo Failed to activate Conda environment. Ensure 'subgrp-proj' exists.
    pause
    exit /b 1
)

REM Start the Master Node
echo Starting Master Node...
start "Master Node" cmd /k "call conda.bat activate subgrp-proj && python c:\Projects\SoftwareLab_PreThesis\kafka_pysubg\mark3\master_m3.py"
echo Master Node started.

REM Start Worker Nodes
echo Starting Worker Nodes...
for /L %%i in (1,1,3) do (
    start "Worker Node %%i" cmd /k "call conda.bat activate subgrp-proj && python c:\Projects\SoftwareLab_PreThesis\kafka_pysubg\mark3\worker_m3.py %%i"
    echo Worker Node %%i started.
)

echo Project started successfully.
pause
