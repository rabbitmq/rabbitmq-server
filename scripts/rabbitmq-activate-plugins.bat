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
set TDP0=%~dp0
set STAR=%*
setlocal enabledelayedexpansion

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

set RABBITMQ_PLUGINS_DIR=!TDP0!..\plugins
set RABBITMQ_PLUGINS_EXPAND_DIR=!TDP0!..\priv\plugins
set RABBITMQ_EBIN_DIR=!TDP0!..\ebin

"!ERLANG_HOME!\bin\erl.exe" ^
-pa "!RABBITMQ_EBIN_DIR!" ^
-noinput -hidden ^
-s rabbit_plugin_activator ^
-rabbit plugins_dir \""!RABBITMQ_PLUGINS_DIR:\=/!"\" ^
-rabbit plugins_expand_dir \""!RABBITMQ_PLUGINS_EXPAND_DIR:\=/!"\" ^
-rabbit rabbit_ebin  \""!RABBITMQ_EBIN_DIR:\=/!"\" ^
-extra !STAR!

endlocal
endlocal
