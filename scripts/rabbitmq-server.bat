@echo off
REM  The contents of this file are subject to the Mozilla Public License
REM  Version 1.1 (the "License"); you may not use this file except in
REM  compliance with the License. You may obtain a copy of the License
REM  at https://www.mozilla.org/MPL/
REM
REM  Software distributed under the License is distributed on an "AS IS"
REM  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
REM  the License for the specific language governing rights and
REM  limitations under the License.
REM
REM  The Original Code is RabbitMQ.
REM
REM  The Initial Developer of the Original Code is GoPivotal, Inc.
REM  Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
REM

setlocal

rem Preserve values that might contain exclamation marks before
rem enabling delayed expansion
set TDP0=%~dp0
set STAR=%*
set CONF_SCRIPT_DIR=%~dp0
setlocal enabledelayedexpansion
setlocal enableextensions

if ERRORLEVEL 1 (
    echo "Failed to enable command extensions!"
    exit /B 1
)

REM Get default settings with user overrides for (RABBITMQ_)<var_name>
REM Non-empty defaults should be set in rabbitmq-env
call "%TDP0%\rabbitmq-env.bat" %~n0

if not exist "!ERLANG_HOME!\bin\erl.exe" (
    echo.
    echo ******************************
    echo ERLANG_HOME not set correctly.
    echo ******************************
    echo.
    echo Please either set ERLANG_HOME to point to your Erlang installation or place the
    echo RabbitMQ server distribution in the Erlang lib folder.
    echo.
    exit /B 1
)

set RABBITMQ_DEFAULT_ALLOC_ARGS=+MBas ageffcbf +MHas ageffcbf +MBlmbcs 512 +MHlmbcs 512 +MMmcs 30

set RABBITMQ_START_RABBIT=
if "!RABBITMQ_ALLOW_INPUT!"=="" (
    set RABBITMQ_START_RABBIT=!RABBITMQ_START_RABBIT! -noinput
)
if "!RABBITMQ_NODE_ONLY!"=="" (
    set RABBITMQ_START_RABBIT=!RABBITMQ_START_RABBIT! -s "!RABBITMQ_BOOT_MODULE!" boot
)

if "!RABBITMQ_IO_THREAD_POOL_SIZE!"=="" (
    set RABBITMQ_IO_THREAD_POOL_SIZE=64
)

set ENV_OK=true
CALL :check_not_empty "RABBITMQ_BOOT_MODULE" !RABBITMQ_BOOT_MODULE!

if "!ENV_OK!"=="false" (
    EXIT /b 78
)

if "!RABBITMQ_ALLOW_INPUT!"=="" (
    set ERL_CMD=erl.exe
) else (
    set ERL_CMD=werl.exe
)

"!ERLANG_HOME!\bin\!ERL_CMD!" ^
!RABBITMQ_START_RABBIT! ^
-boot "!SASL_BOOT_FILE!" ^
+W w ^
+A "!RABBITMQ_IO_THREAD_POOL_SIZE!" ^
!RABBITMQ_DEFAULT_ALLOC_ARGS! ^
!RABBITMQ_SERVER_ERL_ARGS! ^
!RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS! ^
!RABBITMQ_SERVER_START_ARGS! ^
-lager crash_log false ^
-lager handlers "[]" ^
!STAR!

if ERRORLEVEL 1 (
    exit /B %ERRORLEVEL%
)

EXIT /B 0

:check_not_empty
if "%~2"=="" (
    ECHO "Error: ENV variable should be defined: %1. Please check rabbitmq-env and rabbitmq-defaults, and !RABBITMQ_CONF_ENV_FILE! script files. Check also your Environment Variables settings"
    set ENV_OK=false
    EXIT /B 78
    )
EXIT /B 0

endlocal
endlocal
endlocal
