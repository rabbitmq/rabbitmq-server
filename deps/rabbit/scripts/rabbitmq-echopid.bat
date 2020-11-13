@echo off

REM Usage: rabbitmq-echopid.bat <rabbitmq_nodename>
REM
REM <rabbitmq_nodename> (s)name of the erlang node to connect to (required)

setlocal

set TDP0=%~dp0

REM Get default settings with user overrides for (RABBITMQ_)<var_name>
REM Non-empty defaults should be set in rabbitmq-env
call "%TDP0%\rabbitmq-env.bat" %~n0

if "%1"=="" goto argfail

:: set timeout vars ::
set TIMEOUT=10
set TIMER=1

:: check that wmic exists ::
set WMIC_PATH=%SYSTEMROOT%\System32\Wbem\wmic.exe
if not exist "%WMIC_PATH%" (
  goto fail
)

:getpid
for /f "usebackq tokens=* skip=1" %%P IN (`%%WMIC_PATH%% process where "name='erl.exe' and commandline like '%%%RABBITMQ_NAME_TYPE% %1%%'" get processid 2^>nul`) do (
  set PID=%%P
  goto echopid
)

:echopid
:: check for pid not found ::
if "%PID%" == "" (
  PING 127.0.0.1 -n 2 > nul
  set /a TIMER+=1
  if %TIMEOUT%==%TIMER% goto fail
  goto getpid
)

:: show pid ::
echo %PID%

:: all done ::
:ok
endlocal
EXIT /B 0

:: argument is required ::
:argfail
echo Please provide your RabbitMQ node name as the argument to this script.

:: something went wrong ::
:fail
endlocal
EXIT /B 1
