@echo off
REM  This Source Code Form is subject to the terms of the Mozilla Public
REM  License, v. 2.0. If a copy of the MPL was not distributed with this
REM  file, You can obtain one at https://mozilla.org/MPL/2.0/.
REM
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
!RABBITMQ_DEFAULT_ALLOC_ARGS! ^
!RABBITMQ_SERVER_ERL_ARGS! ^
!RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS! ^
!RABBITMQ_SERVER_START_ARGS! ^
-syslog logger [] ^
-syslog syslog_error_logger false ^
-kernel prevent_overlapping_partitions false ^
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
