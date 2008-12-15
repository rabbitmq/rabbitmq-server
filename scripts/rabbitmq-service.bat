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
REM   The Initial Developers of the Original Code are LShift Ltd,
REM   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
REM
REM   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
REM   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
REM   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
REM   Technologies LLC, and Rabbit Technologies Ltd.
REM
REM   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
REM   Ltd. Portions created by Cohesive Financial Technologies LLC are
REM   Copyright (C) 2007-2009 Cohesive Financial Technologies
REM   LLC. Portions created by Rabbit Technologies Ltd are Copyright
REM   (C) 2007-2009 Rabbit Technologies Ltd.
REM
REM   All Rights Reserved.
REM
REM   Contributor(s): ______________________________________.
REM

if "%RABBITMQ_SERVICENAME%"=="" (
    set RABBITMQ_SERVICENAME=RabbitMQ
)

if "%RABBITMQ_BASE%"=="" (
    set RABBITMQ_BASE=%APPDATA%\%RABBITMQ_SERVICENAME%
)

if "%RABBITMQ_NODENAME%"=="" (
    set RABBITMQ_NODENAME=rabbit
)

if "%RABBITMQ_NODE_IP_ADDRESS%"=="" (
    set RABBITMQ_NODE_IP_ADDRESS=0.0.0.0
)

if "%RABBITMQ_NODE_PORT%"=="" (
    set RABBITMQ_NODE_PORT=5672
)

if "%ERLANG_SERVICE_MANAGER_PATH%"=="" (
    set ERLANG_SERVICE_MANAGER_PATH=C:\Program Files\erl5.5.5\erts-5.5.5\bin
)

set CONSOLE_FLAG=
set CONSOLE_LOG_VALID=
for %%i in (new reuse) do if "%%i" == "%RABBITMQ_CONSOLE_LOG%" set CONSOLE_LOG_VALID=TRUE
if "%CONSOLE_LOG_VALID%" == "TRUE" (
    set CONSOLE_FLAG=-debugtype %RABBITMQ_CONSOLE_LOG%
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

if "%RABBITMQ_MNESIA_BASE%"=="" (
    set RABBITMQ_MNESIA_BASE=%RABBITMQ_BASE_UNIX%/db
)
if "%RABBITMQ_LOG_BASE%"=="" (
    set RABBITMQ_LOG_BASE=%RABBITMQ_BASE_UNIX%/log
)


rem We save the previous logs in their respective backup
rem Log management (rotation, filtering based on size...) is left as an exercise for the user.

set BACKUP_EXTENSION=.1

set LOGS="%RABBITMQ_BASE%\log\%RABBITMQ_NODENAME%.log"
set SASL_LOGS="%RABBITMQ_BASE%\log\%RABBITMQ_NODENAME%-sasl.log"

set LOGS_BACKUP="%RABBITMQ_BASE%\log\%RABBITMQ_NODENAME%.log%BACKUP_EXTENSION%"
set SASL_LOGS_BACKUP="%RABBITMQ_BASE%\log\%RABBITMQ_NODENAME%-sasl.log%BACKUP_EXTENSION%"

if exist %LOGS% (
	type %LOGS% >> %LOGS_BACKUP%
)
if exist %SASL_LOGS% (
	type %SASL_LOGS% >> %SASL_LOGS_BACKUP%
)

rem End of log management


if "%RABBITMQ_CLUSTER_CONFIG_FILE%"=="" (
    set RABBITMQ_CLUSTER_CONFIG_FILE=%RABBITMQ_BASE%\rabbitmq_cluster.config
)
set CLUSTER_CONFIG=
if not exist "%RABBITMQ_CLUSTER_CONFIG_FILE%" GOTO L1
set CLUSTER_CONFIG=-rabbit cluster_config \""%RABBITMQ_CLUSTER_CONFIG_FILE:\=/%"\"
:L1

if "%RABBITMQ_MNESIA_DIR%"=="" (
    set RABBITMQ_MNESIA_DIR=%RABBITMQ_MNESIA_BASE%/%RABBITMQ_NODENAME%-mnesia
)


if "%1" == "install" goto INSTALL_SERVICE
for %%i in (start stop disable enable list remove) do if "%%i" == "%1" goto MODIFY_SERVICE 

echo.
echo *********************
echo Service control usage
echo *********************
echo.
echo %~n0 help    - Display this help
echo %~n0 install - Install the %RABBITMQ_SERVICENAME% service
echo %~n0 remove  - Remove the %RABBITMQ_SERVICENAME% service
echo.
echo The following actions can also be accomplished by using 
echo Windows Services Management Console (services.msc):
echo.
echo %~n0 start   - Start the %RABBITMQ_SERVICENAME% service
echo %~n0 stop    - Stop the %RABBITMQ_SERVICENAME% service
echo %~n0 disable - Disable the %RABBITMQ_SERVICENAME% service
echo %~n0 enable  - Enable the %RABBITMQ_SERVICENAME% service
echo.
exit /B


:INSTALL_SERVICE

if not exist "%RABBITMQ_BASE%" (
    echo Creating base directory %RABBITMQ_BASE% & md "%RABBITMQ_BASE%" 
)

"%ERLANG_SERVICE_MANAGER_PATH%\erlsrv" list %RABBITMQ_SERVICENAME% 2>NUL 1>NUL
if errorlevel 1 (
    "%ERLANG_SERVICE_MANAGER_PATH%\erlsrv" add %RABBITMQ_SERVICENAME%
) else (
    echo %RABBITMQ_SERVICENAME% service is already present - only updating service parameters
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
-rabbit tcp_listeners "[{\"%RABBITMQ_NODE_IP_ADDRESS%\",%RABBITMQ_NODE_PORT%}]" ^
-kernel error_logger {file,\""%RABBITMQ_LOG_BASE%/%RABBITMQ_NODENAME%.log"\"} ^
-sasl errlog_type error ^
-sasl sasl_error_logger {file,\""%RABBITMQ_LOG_BASE%/%RABBITMQ_NODENAME%-sasl.log"\"} ^
-os_mon start_cpu_sup true ^
-os_mon start_disksup false ^
-os_mon start_memsup false ^
-os_mon start_os_sup false ^
-os_mon memsup_system_only true ^
-os_mon system_memory_high_watermark 0.95 ^
-mnesia dir \""%RABBITMQ_MNESIA_DIR%"\" ^
%CLUSTER_CONFIG% ^
%RABBITMQ_SERVER_START_ARGS% ^
%*

set ERLANG_SERVICE_ARGUMENTS=%ERLANG_SERVICE_ARGUMENTS:\=\\%
set ERLANG_SERVICE_ARGUMENTS=%ERLANG_SERVICE_ARGUMENTS:"=\"%

"%ERLANG_SERVICE_MANAGER_PATH%\erlsrv" set %RABBITMQ_SERVICENAME% ^
-machine "%ERLANG_SERVICE_MANAGER_PATH%\erl.exe" ^
-env ERL_CRASH_DUMP="%RABBITMQ_BASE_UNIX%/log" ^
-workdir "%RABBITMQ_BASE%" ^
-stopaction "rabbit:stop_and_halt()." ^
-sname %RABBITMQ_NODENAME% ^
%CONSOLE_FLAG% ^
-args "%ERLANG_SERVICE_ARGUMENTS%" > NUL
goto END


:MODIFY_SERVICE

"%ERLANG_SERVICE_MANAGER_PATH%\erlsrv" %1 %RABBITMQ_SERVICENAME%
goto END


:END
