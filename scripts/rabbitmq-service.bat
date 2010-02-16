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
REM   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
REM   Ltd. Portions created by Cohesive Financial Technologies LLC are
REM   Copyright (C) 2007-2010 Cohesive Financial Technologies
REM   LLC. Portions created by Rabbit Technologies Ltd are Copyright
REM   (C) 2007-2010 Rabbit Technologies Ltd.
REM
REM   All Rights Reserved.
REM
REM   Contributor(s): ______________________________________.
REM

setlocal

rem Preserve values that might contain exclamation marks before
rem enabling delayed expansion
set TN0=%~n0
set TDP0=%~dp0
set P1=%1
set STAR=%*
setlocal enabledelayedexpansion

if "!RABBITMQ_SERVICENAME!"=="" (
    set RABBITMQ_SERVICENAME=RabbitMQ
)

if "!RABBITMQ_BASE!"=="" (
    set RABBITMQ_BASE=!APPDATA!\!RABBITMQ_SERVICENAME!
)

if "!RABBITMQ_NODENAME!"=="" (
    set RABBITMQ_NODENAME=rabbit
)

if "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
    if not "!RABBITMQ_NODE_PORT!"=="" (
       set RABBITMQ_NODE_IP_ADDRESS=0.0.0.0
    )
) else (
    if "!RABBITMQ_NODE_PORT!"=="" (
       set RABBITMQ_NODE_PORT=5672
    )
)

if "!ERLANG_SERVICE_MANAGER_PATH!"=="" (
    if not exist "!ERLANG_HOME!\bin\erl.exe" (
        echo.
        echo ******************************
        echo ERLANG_HOME not set correctly.
        echo ******************************
        echo.
        echo Please either set ERLANG_HOME to point to your Erlang installation or place the
        echo RabbitMQ server distribution in the Erlang lib folder.
        echo.
        exit /B
    )
    for /f "delims=" %%i in ('dir /ad/b "!ERLANG_HOME!"') do if exist "!ERLANG_HOME!\%%i\bin\erlsrv.exe" (
        set ERLANG_SERVICE_MANAGER_PATH=!ERLANG_HOME!\%%i\bin
    )
)

set CONSOLE_FLAG=
set CONSOLE_LOG_VALID=
for %%i in (new reuse) do if "%%i" == "!RABBITMQ_CONSOLE_LOG!" set CONSOLE_LOG_VALID=TRUE
if "!CONSOLE_LOG_VALID!" == "TRUE" (
    set CONSOLE_FLAG=-debugtype !RABBITMQ_CONSOLE_LOG!
)

rem *** End of configuration ***

if not exist "!ERLANG_SERVICE_MANAGER_PATH!\erlsrv.exe" (
    echo.
    echo **********************************************
    echo ERLANG_SERVICE_MANAGER_PATH not set correctly. 
    echo **********************************************
    echo.
    echo "!ERLANG_SERVICE_MANAGER_PATH!\erlsrv.exe" not found
    echo Please set ERLANG_SERVICE_MANAGER_PATH to the folder containing "erlsrv.exe".
    echo.
    exit /B 1
)

rem erlang prefers forwardslash as separator in paths
set RABBITMQ_BASE_UNIX=!RABBITMQ_BASE:\=/!

if "!RABBITMQ_MNESIA_BASE!"=="" (
    set RABBITMQ_MNESIA_BASE=!RABBITMQ_BASE_UNIX!/db
)
if "!RABBITMQ_LOG_BASE!"=="" (
    set RABBITMQ_LOG_BASE=!RABBITMQ_BASE_UNIX!/log
)


rem We save the previous logs in their respective backup
rem Log management (rotation, filtering based on size...) is left as an exercise for the user.

set BACKUP_EXTENSION=.1

set LOGS=!RABBITMQ_BASE!\log\!RABBITMQ_NODENAME!.log
set SASL_LOGS=!RABBITMQ_BASE!\log\!RABBITMQ_NODENAME!-sasl.log

set LOGS_BACKUP=!RABBITMQ_BASE!\log\!RABBITMQ_NODENAME!.log!BACKUP_EXTENSION!
set SASL_LOGS_BACKUP=!RABBITMQ_BASE!\log\!RABBITMQ_NODENAME!-sasl.log!BACKUP_EXTENSION!

if exist "!LOGS!" (
	type "!LOGS!" >> "!LOGS_BACKUP!"
)
if exist "!SASL_LOGS!" (
	type "!SASL_LOGS!" >> "!SASL_LOGS_BACKUP!"
)

rem End of log management


if "!RABBITMQ_CLUSTER_CONFIG_FILE!"=="" (
    set RABBITMQ_CLUSTER_CONFIG_FILE=!RABBITMQ_BASE!\rabbitmq_cluster.config
)
set CLUSTER_CONFIG=
if not exist "!RABBITMQ_CLUSTER_CONFIG_FILE!" GOTO L1
set CLUSTER_CONFIG=-rabbit cluster_config \""!RABBITMQ_CLUSTER_CONFIG_FILE:\=/!"\"
:L1

if "!RABBITMQ_MNESIA_DIR!"=="" (
    set RABBITMQ_MNESIA_DIR=!RABBITMQ_MNESIA_BASE!/!RABBITMQ_NODENAME!-mnesia
)


if "!P1!" == "install" goto INSTALL_SERVICE
for %%i in (start stop disable enable list remove) do if "%%i" == "!P1!" goto MODIFY_SERVICE 

echo.
echo *********************
echo Service control usage
echo *********************
echo.
echo !TN0! help    - Display this help
echo !TN0! install - Install the !RABBITMQ_SERVICENAME! service
echo !TN0! remove  - Remove the !RABBITMQ_SERVICENAME! service
echo.
echo The following actions can also be accomplished by using 
echo Windows Services Management Console (services.msc):
echo.
echo !TN0! start   - Start the !RABBITMQ_SERVICENAME! service
echo !TN0! stop    - Stop the !RABBITMQ_SERVICENAME! service
echo !TN0! disable - Disable the !RABBITMQ_SERVICENAME! service
echo !TN0! enable  - Enable the !RABBITMQ_SERVICENAME! service
echo.
exit /B


:INSTALL_SERVICE

if not exist "!RABBITMQ_BASE!" (
    echo Creating base directory !RABBITMQ_BASE! & md "!RABBITMQ_BASE!" 
)

"!ERLANG_SERVICE_MANAGER_PATH!\erlsrv" list !RABBITMQ_SERVICENAME! 2>NUL 1>NUL
if errorlevel 1 (
    "!ERLANG_SERVICE_MANAGER_PATH!\erlsrv" add !RABBITMQ_SERVICENAME!
) else (
    echo !RABBITMQ_SERVICENAME! service is already present - only updating service parameters
)

set RABBITMQ_EBIN_ROOT=!TDP0!..\ebin
if exist "!RABBITMQ_EBIN_ROOT!\rabbit.boot" (
    echo Using Custom Boot File "!RABBITMQ_EBIN_ROOT!\rabbit.boot"
    set RABBITMQ_BOOT_FILE=!RABBITMQ_EBIN_ROOT!\rabbit
    set RABBITMQ_EBIN_PATH=
) else (
    set RABBITMQ_BOOT_FILE=start_sasl
    set RABBITMQ_EBIN_PATH=-pa "!RABBITMQ_EBIN_ROOT!"
)
if "!RABBITMQ_CONFIG_FILE!"=="" (
    set RABBITMQ_CONFIG_FILE=!RABBITMQ_BASE!\rabbitmq
)
   
if exist "!RABBITMQ_CONFIG_FILE!.config" (
    set RABBITMQ_CONFIG_ARG=-config "!RABBITMQ_CONFIG_FILE!"
) else (
    set RABBITMQ_CONFIG_ARG=
)

set RABBITMQ_LISTEN_ARG=
if not "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
   if not "!RABBITMQ_NODE_PORT!"=="" (
      set RABBITMQ_LISTEN_ARG=-rabbit tcp_listeners "[{\"!RABBITMQ_NODE_IP_ADDRESS!\", !RABBITMQ_NODE_PORT!}]"
   )
)

set ERLANG_SERVICE_ARGUMENTS= ^
!RABBITMQ_EBIN_PATH! ^
-boot "!RABBITMQ_BOOT_FILE!" ^
!RABBITMQ_CONFIG_ARG! ^
-s rabbit ^
+W w ^
+A30 ^
-kernel inet_default_listen_options "[{nodelay,true}]" ^
-kernel inet_default_connect_options "[{nodelay,true}]" ^
!RABBITMQ_LISTEN_ARG! ^
-kernel error_logger {file,\""!RABBITMQ_LOG_BASE!/!RABBITMQ_NODENAME!.log"\"} ^
!RABBITMQ_SERVER_ERL_ARGS! ^
-sasl errlog_type error ^
-sasl sasl_error_logger {file,\""!RABBITMQ_LOG_BASE!/!RABBITMQ_NODENAME!-sasl.log"\"} ^
-os_mon start_cpu_sup true ^
-os_mon start_disksup false ^
-os_mon start_memsup false ^
-mnesia dir \""!RABBITMQ_MNESIA_DIR!"\" ^
!CLUSTER_CONFIG! ^
!RABBITMQ_SERVER_START_ARGS! ^
!STAR!

set ERLANG_SERVICE_ARGUMENTS=!ERLANG_SERVICE_ARGUMENTS:\=\\!
set ERLANG_SERVICE_ARGUMENTS=!ERLANG_SERVICE_ARGUMENTS:"=\"!

"!ERLANG_SERVICE_MANAGER_PATH!\erlsrv" set !RABBITMQ_SERVICENAME! ^
-machine "!ERLANG_SERVICE_MANAGER_PATH!\erl.exe" ^
-env ERL_CRASH_DUMP="!RABBITMQ_BASE_UNIX!/erl_crash.dump" ^
-workdir "!RABBITMQ_BASE!" ^
-stopaction "rabbit:stop_and_halt()." ^
-sname !RABBITMQ_NODENAME! ^
!CONSOLE_FLAG! ^
-args "!ERLANG_SERVICE_ARGUMENTS!" > NUL
goto END


:MODIFY_SERVICE

"!ERLANG_SERVICE_MANAGER_PATH!\erlsrv" !P1! !RABBITMQ_SERVICENAME!
goto END


:END

endlocal
endlocal
