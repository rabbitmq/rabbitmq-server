@echo off
REM  The contents of this file are subject to the Mozilla Public License
REM  Version 1.1 (the "License"); you may not use this file except in
REM  compliance with the License. You may obtain a copy of the License
REM  at http://www.mozilla.org/MPL/
REM
REM  Software distributed under the License is distributed on an "AS IS"
REM  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
REM  the License for the specific language governing rights and
REM  limitations under the License.
REM
REM  The Original Code is RabbitMQ.
REM
REM  The Initial Developer of the Original Code is VMware, Inc.
REM  Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
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

if "!COMPUTERNAME!"=="" (
    set COMPUTERNAME=localhost
)

if "!RABBITMQ_NODENAME!"=="" (
    set RABBITMQ_NODENAME=rabbit@!COMPUTERNAME!
)

if "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
    if not "!RABBITMQ_NODE_PORT!"=="" (
       set RABBITMQ_NODE_IP_ADDRESS=auto
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

if "!RABBITMQ_MNESIA_BASE!"=="" (
    set RABBITMQ_MNESIA_BASE=!RABBITMQ_BASE!/db
)
if "!RABBITMQ_LOG_BASE!"=="" (
    set RABBITMQ_LOG_BASE=!RABBITMQ_BASE!/log
)


rem We save the previous logs in their respective backup
rem Log management (rotation, filtering based on size...) is left as an exercise for the user.

set LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!.log
set SASL_LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!-sasl.log

rem End of log management


if "!RABBITMQ_MNESIA_DIR!"=="" (
    set RABBITMQ_MNESIA_DIR=!RABBITMQ_MNESIA_BASE!/!RABBITMQ_NODENAME!-mnesia
)

if "!RABBITMQ_PLUGINS_EXPAND_DIR!"=="" (
    set RABBITMQ_PLUGINS_EXPAND_DIR=!RABBITMQ_MNESIA_BASE!/!RABBITMQ_NODENAME!-plugins-expand
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
    "!ERLANG_SERVICE_MANAGER_PATH!\erlsrv" add !RABBITMQ_SERVICENAME! -internalservicename !RABBITMQ_SERVICENAME!
) else (
    echo !RABBITMQ_SERVICENAME! service is already present - only updating service parameters
)

if "!RABBITMQ_ENABLED_PLUGINS_FILE!"=="" (
    set RABBITMQ_ENABLED_PLUGINS_FILE=!RABBITMQ_BASE!\enabled_plugins
)

set RABBITMQ_PLUGINS_DIR=!TDP0!..\plugins
set RABBITMQ_EBIN_ROOT=!TDP0!..\ebin

"!ERLANG_HOME!\bin\erl.exe" ^
-pa "!RABBITMQ_EBIN_ROOT!" ^
-noinput -hidden ^
-s rabbit_prelaunch ^
-extra "!RABBITMQ_ENABLED_PLUGINS_FILE:\=/!" ^
       "!RABBITMQ_PLUGINS_DIR:\=/!" ^
       "!RABBITMQ_PLUGINS_EXPAND_DIR:\=/!" ^
       ""

set RABBITMQ_BOOT_FILE=!RABBITMQ_PLUGINS_EXPAND_DIR!\rabbit
if ERRORLEVEL 1 (
    exit /B 1
)

set RABBITMQ_EBIN_PATH=

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
+W w ^
+A30 ^
+P 1048576 ^
-kernel inet_default_connect_options "[{nodelay,true}]" ^
!RABBITMQ_LISTEN_ARG! ^
!RABBITMQ_SERVER_ERL_ARGS! ^
-sasl errlog_type error ^
-sasl sasl_error_logger false ^
-rabbit error_logger {file,\""!LOGS:\=/!"\"} ^
-rabbit sasl_error_logger {file,\""!SASL_LOGS:\=/!"\"} ^
-os_mon start_cpu_sup false ^
-os_mon start_disksup false ^
-os_mon start_memsup false ^
-mnesia dir \""!RABBITMQ_MNESIA_DIR:\=/!"\" ^
!RABBITMQ_SERVER_START_ARGS! ^
!STAR!

set ERLANG_SERVICE_ARGUMENTS=!ERLANG_SERVICE_ARGUMENTS:\=\\!
set ERLANG_SERVICE_ARGUMENTS=!ERLANG_SERVICE_ARGUMENTS:"=\"!

"!ERLANG_SERVICE_MANAGER_PATH!\erlsrv" set !RABBITMQ_SERVICENAME! ^
-machine "!ERLANG_SERVICE_MANAGER_PATH!\erl.exe" ^
-env ERL_CRASH_DUMP="!RABBITMQ_BASE:\=/!/erl_crash.dump" ^
-workdir "!RABBITMQ_BASE!" ^
-stopaction "rabbit:stop_and_halt()." ^
-sname !RABBITMQ_NODENAME! ^
!CONSOLE_FLAG! ^
-comment "A robust and scalable messaging broker" ^
-args "!ERLANG_SERVICE_ARGUMENTS!" > NUL

goto END


:MODIFY_SERVICE

"!ERLANG_SERVICE_MANAGER_PATH!\erlsrv" !P1! !RABBITMQ_SERVICENAME!
goto END


:END

endlocal
endlocal
