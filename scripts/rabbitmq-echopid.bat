@echo off

REM Usage: rabbitmq-echopid.bat <rabbitmq_nodename>
REM
REM <rabbitmq_nodename> sname of the erlang node to connect to (required)

setlocal

if "%1"=="" goto fail

:: set timeout vars ::
set TIMEOUT=10
set TIMER=1

:: check that wmic exists ::
set WMIC_PATH=%SYSTEMROOT%\System32\Wbem\wmic.exe
if not exist "%WMIC_PATH%" (
  goto fail
)

:getpid
for /f "usebackq tokens=* skip=1" %%P IN (`%%WMIC_PATH%% process where "name='erl.exe' and commandline like '%%-sname %1%%'" get processid 2^>nul`) do (
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

:: something went wrong ::
:fail
endlocal
EXIT /B 1


