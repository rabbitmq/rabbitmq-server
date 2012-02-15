@echo off

REM Usage: rabbitmq-echopid.bat <rabbitmq_nodename> [<wait>]
REM
REM <rabbitmq_nodename> sname of the erlang node to connect to (required)
REM <wait>              if specified, causes the script to wait until a pid is detected

setlocal

if "%1"=="" goto wmic_fail
if "%2"=="wait" set WAIT=%2

:: set the node name ::
set NODENAME="%1"

:: check that wmic exists ::
set WMIC_PATH=%SYSTEMROOT%\System32\Wbem\wmic.exe
if not exist "%WMIC_PATH%" (
  goto wmic_fail
)

:: build node name expression ::
set RABBITMQ_NODENAME_CLI=-sname %1

:wmic_getpid
for /F "usebackq tokens=* skip=1" %%P IN (`%%WMIC_PATH%% process where "name='erl.exe' and commandline like '%%%RABBITMQ_NODENAME_CLI%%%'" get processid 2^>nul`) do (
  set PID=%%P
  goto wmic_echopid
)

:wmic_echopid
:: check for pid not found ::
if "%PID%" == "" (
	if not defined WAIT goto wmic_fail
  PING 127.0.0.1 -n 2 > nul
  goto wmic_getpid
)

:: show pid ::
echo %PID%

:: all done ::
:wmic_ok
endlocal
EXIT /B 0

:: something went wrong ::
:wmic_fail
endlocal
EXIT /B 1


