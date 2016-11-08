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
REM  The Initial Developer of the Original Code is GoPivotal, Inc.
REM  Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
REM

setlocal

rem Preserve values that might contain exclamation marks before
rem enabling delayed expansion
set TDP0=%~dp0
set STAR=%*
setlocal enabledelayedexpansion

REM Get default settings with user overrides for (RABBITMQ_)<var_name>
REM Non-empty defaults should be set in rabbitmq-env
call "!TDP0!\rabbitmq-env.bat" %~n0

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

REM Disable erl_crash.dump by default for control scripts.
if not defined ERL_CRASH_DUMP_SECONDS (
    set ERL_CRASH_DUMP_SECONDS=0
)

"!ERLANG_HOME!\bin\escript.exe" ^
"%RABBITMQ_HOME%\escript\rabbitmq-plugins" --formatter=plugins !STAR!
rem -pa "!RABBITMQ_HOME!\ebin" ^
rem -noinput ^
rem -hidden ^
rem !RABBITMQ_CTL_ERL_ARGS! ^
rem -s rabbit_plugins_main ^
rem -enabled_plugins_file "!RABBITMQ_ENABLED_PLUGINS_FILE!" ^
rem -plugins_dist_dir "!RABBITMQ_PLUGINS_DIR:\=/!" ^
rem -nodename !RABBITMQ_NODENAME! ^
rem -extra !STAR!

endlocal
endlocal
