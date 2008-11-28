@echo off
REM   The contents of this file are subject to the Mozilla Public License
REM   Version 1.1 (the "License"); you may not use this file except in
REM   compliance with the License. You may obtain a copy of the License at
REM   http://www.mozilla.org/MPL/
REM
REM   Software distributed under the License is distributed on an "AS IS"
REM   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
REM   License for the specific language governing rights and limitations
REM   under the License.
REM
REM   The Original Code is RabbitMQ.
REM
REM   The Initial Developers of the Original Code are LShift Ltd.,
REM   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
REM
REM   Portions created by LShift Ltd., Cohesive Financial Technologies
REM   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
REM   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
REM   Technologies Ltd.;
REM
REM   All Rights Reserved.
REM
REM   Contributor(s): ______________________________________.
REM

if "%SERVICENAME%"=="" (
    set SERVICENAME=RabbitMQ
)

if "%RABBITMQ_BASE%"=="" (
    set RABBITMQ_BASE=%APPDATA%\%SERVICENAME%
)

if "%NODENAME%"=="" (
    set NODENAME=rabbit
)

if "%NODE_IP_ADDRESS%"=="" (
    set NODE_IP_ADDRESS=0.0.0.0
)

if "%NODE_PORT%"=="" (
    set NODE_PORT=5672
)

if "%ERLANG_SERVICE_MANAGER_PATH%"=="" (
    set ERLANG_SERVICE_MANAGER_PATH=C:\Program Files\erl5.5.5\erts-5.5.5\bin
)

set CONSOLE_FLAG=
set CONSOLE_LOG_VALID=
for %%i in (new reuse) do if "%%i" == "%CONSOLE_LOG%" set CONSOLE_LOG_VALID=TRUE
if "%CONSOLE_LOG_VALID%" == "TRUE" (
    set CONSOLE_FLAG=-debugtype %CONSOLE_LOG%
)

rem *** End of configuration ***

if not exist "%ERLANG_SERVICE_MANAGER_PATH%\erlsrv.exe" (
    echo.
    echo **********************************************
    echo ERLANG_SERVICE_MANAGER_PATH not set correctly. 
    echo **********************************************
    echo.
    echo %ERLANG_SERVICE_MANAGER_PATH%\erlsrv.exe not found!
    echo Please set ERLANG_SERVICE_MANAGER_PATH to the folder containing "erlsrv.exe".
    echo.
    exit /B 1
)

rem erlang prefers forwardslash as separator in paths
set RABBITMQ_BASE_UNIX=%RABBITMQ_BASE:\=/%
set MNESIA_BASE=%RABBITMQ_BASE_UNIX%/db
set LOG_BASE=%RABBITMQ_BASE_UNIX%/log


rem We save the previous logs in their respective backup
rem Log management (rotation, filtering based on size...) is left as an exercise for the user.

set BACKUP_EXTENSION=.1

set LOGS="%RABBITMQ_BASE%\log\%NODENAME%.log"
set SASL_LOGS="%RABBITMQ_BASE%\log\%NODENAME%-sasl.log"

set LOGS_BACKUP="%RABBITMQ_BASE%\log\%NODENAME%.log%BACKUP_EXTENSION%"
set SASL_LOGS_BACKUP="%RABBITMQ_BASE%\log\%NODENAME%-sasl.log%BACKUP_EXTENSION%"

if exist %LOGS% (
	type %LOGS% >> %LOGS_BACKUP%
)
if exist %SASL_LOGS% (
	type %SASL_LOGS% >> %SASL_LOGS_BACKUP%
)

rem End of log management


set CLUSTER_CONFIG_FILE=%RABBITMQ_BASE%\rabbitmq_cluster.config
set CLUSTER_CONFIG=
if not exist "%CLUSTER_CONFIG_FILE%" GOTO L1
set CLUSTER_CONFIG=-rabbit cluster_config \""%CLUSTER_CONFIG_FILE:\=/%"\"
:L1

set MNESIA_DIR=%MNESIA_BASE%/%NODENAME%-mnesia


if "%1" == "install" goto INSTALL_SERVICE
for %%i in (start stop disable enable list remove) do if "%%i" == "%1" goto MODIFY_SERVICE 

echo.
echo *********************
echo Service control usage
echo *********************
echo.
echo %~n0 help    - Display this help
echo %~n0 install - Install the %SERVICENAME% service
echo %~n0 remove  - Remove the %SERVICENAME% service
echo.
echo The following actions can also be accomplished by using 
echo Windows Services Management Console (services.msc):
echo.
echo %~n0 start   - Start the %SERVICENAME% service
echo %~n0 stop    - Stop the %SERVICENAME% service
echo %~n0 disable - Disable the %SERVICENAME% service
echo %~n0 enable  - Enable the %SERVICENAME% service
echo.
exit /B


:INSTALL_SERVICE

if not exist "%RABBITMQ_BASE%" (
    echo Creating base directory %RABBITMQ_BASE% & md "%RABBITMQ_BASE%" 
)

"%ERLANG_SERVICE_MANAGER_PATH%\erlsrv" list %SERVICENAME% 2>NUL 1>NUL
if errorlevel 1 (
    "%ERLANG_SERVICE_MANAGER_PATH%\erlsrv" add %SERVICENAME%
) else (
    echo %SERVICENAME% service is already present - only updating service parameters
)

set RABBIT_EBIN=%~dp0..\ebin

set ERLANG_SERVICE_ARGUMENTS= ^
-pa "%RABBIT_EBIN%" ^
-boot start_sasl ^
-s rabbit ^
+W w ^
+A30 ^
-kernel inet_default_listen_options "[{nodelay,true},{sndbuf,16384},{recbuf,4096}]" ^
-kernel inet_default_connect_options "[{nodelay,true}]" ^
-rabbit tcp_listeners "[{\"%NODE_IP_ADDRESS%\",%NODE_PORT%}]" ^
-kernel error_logger {file,\""%LOG_BASE%/%NODENAME%.log"\"} ^
-sasl errlog_type error ^
-sasl sasl_error_logger {file,\""%LOG_BASE%/%NODENAME%-sasl.log"\"} ^
-os_mon start_cpu_sup true ^
-os_mon start_disksup false ^
-os_mon start_memsup true ^
-os_mon start_os_sup false ^
-os_mon memsup_system_only true ^
-os_mon system_memory_high_watermark 0.95 ^
-mnesia dir \""%MNESIA_DIR%"\" ^
%CLUSTER_CONFIG% ^
%RABBIT_ARGS% ^
%*

set ERLANG_SERVICE_ARGUMENTS=%ERLANG_SERVICE_ARGUMENTS:\=\\%
set ERLANG_SERVICE_ARGUMENTS=%ERLANG_SERVICE_ARGUMENTS:"=\"%

"%ERLANG_SERVICE_MANAGER_PATH%\erlsrv" set %SERVICENAME% ^
-machine "%ERLANG_SERVICE_MANAGER_PATH%\erl.exe" ^
-env ERL_CRASH_DUMP="%RABBITMQ_BASE_UNIX%/log" ^
-workdir "%RABBITMQ_BASE%" ^
-stopaction "rabbit:stop_and_halt()." ^
-sname %NODENAME% ^
%CONSOLE_FLAG% ^
-args "%ERLANG_SERVICE_ARGUMENTS%" > NUL
goto END


:MODIFY_SERVICE

"%ERLANG_SERVICE_MANAGER_PATH%\erlsrv" %1 %SERVICENAME%
goto END


:END