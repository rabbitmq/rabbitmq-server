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
set TDP0=%~dp0
set STAR=%*
setlocal enabledelayedexpansion

if "!RABBITMQ_BASE!"=="" (
    set RABBITMQ_BASE=!APPDATA!\RabbitMQ
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

if "!RABBITMQ_MNESIA_BASE!"=="" (
    set RABBITMQ_MNESIA_BASE=!RABBITMQ_BASE!/db
)
if "!RABBITMQ_LOG_BASE!"=="" (
    set RABBITMQ_LOG_BASE=!RABBITMQ_BASE!/log
)


rem We save the previous logs in their respective backup
rem Log management (rotation, filtering based of size...) is left as an exercice for the user.

set LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!.log
set SASL_LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!-sasl.log

rem End of log management


if "!RABBITMQ_MNESIA_DIR!"=="" (
    set RABBITMQ_MNESIA_DIR=!RABBITMQ_MNESIA_BASE!/!RABBITMQ_NODENAME!-mnesia
)

if "!RABBITMQ_PLUGINS_EXPAND_DIR!"=="" (
    set RABBITMQ_PLUGINS_EXPAND_DIR=!RABBITMQ_MNESIA_BASE!/!RABBITMQ_NODENAME!-plugins-expand
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
-sname rabbitmqprelaunch!RANDOM! ^
-extra "!RABBITMQ_ENABLED_PLUGINS_FILE:\=/!" ^
       "!RABBITMQ_PLUGINS_DIR:\=/!" ^
       "!RABBITMQ_PLUGINS_EXPAND_DIR:\=/!" ^
       "!RABBITMQ_NODENAME!"

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
      set RABBITMQ_LISTEN_ARG=-rabbit tcp_listeners [{\""!RABBITMQ_NODE_IP_ADDRESS!"\","!RABBITMQ_NODE_PORT!"}]
   )
)

"!ERLANG_HOME!\bin\erl.exe" ^
!RABBITMQ_EBIN_PATH! ^
-noinput ^
-boot "!RABBITMQ_BOOT_FILE!" ^
!RABBITMQ_CONFIG_ARG! ^
-sname !RABBITMQ_NODENAME! ^
+W w ^
+A30 ^
+P 1048576 ^
-kernel inet_default_connect_options "[{nodelay, true}]" ^
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

endlocal
endlocal
