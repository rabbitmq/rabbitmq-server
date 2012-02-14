@echo off

setlocal

:: get the name of the node ::
set NODENAME="%RABBITMQ_NODENAME%"
if "%NODENAME%"=="" (
    set NODENAME=rabbit@%COMPUTERNAME%
)

:: check that wmic exists ::
set WMIC_PATH=%SYSTEMROOT%\System32\Wbem\wmic.exe
if not exist "%WMIC_PATH%" (
  REM  echo "%WMIC_PATH%" not found.
  goto :wmic_end
)

:: build node name expression ::
set RABBITMQ_NODENAME_CLI=-sname %RABBITMQ_NODENAME%

FOR /F "usebackq tokens=* skip=1" %%P IN (`%%WMIC_PATH%% process where "name='erl.exe' and commandline like '%%%RABBITMQ_NODENAME_CLI%%%'" get processid 2^>nul`) do (
  SET PID=%%P
  goto :wmic_echopid
)

:wmic_echopid

:: check for pid not found ::
if "%PID%" == "" (
  REM echo Could not find erl.exe pid
  goto :wmic_end
)

:: show pid ::
echo %PID%

:: all done ::
:wmic_end

endlocal
