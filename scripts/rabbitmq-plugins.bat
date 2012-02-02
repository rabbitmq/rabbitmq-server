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

if "!RABBITMQ_ENABLED_PLUGINS_FILE!"=="" (
    set RABBITMQ_ENABLED_PLUGINS_FILE=!RABBITMQ_BASE!\enabled_plugins
)

set RABBITMQ_PLUGINS_DIR=!TDP0!..\plugins

"!ERLANG_HOME!\bin\erl.exe" -pa "!TDP0!..\ebin" -noinput -hidden -sname rabbitmq-plugins!RANDOM! -s rabbit_plugins -enabled_plugins_file "!RABBITMQ_ENABLED_PLUGINS_FILE!" -plugins_dist_dir "!RABBITMQ_PLUGINS_DIR:\=/!" -extra !STAR!

endlocal
endlocal
