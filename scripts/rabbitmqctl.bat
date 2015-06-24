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
REM  Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
REM

REM Scopes the variables to the current batch file
setlocal

rem Preserve values that might contain exclamation marks before
rem enabling delayed expansion
set TDP0=%~dp0
set STAR=%*
setlocal enabledelayedexpansion

REM Get default settings with user overrides for (RABBITMQ_)<var_name>
REM Non-empty defaults should be set in rabbitmq-env
call "%cd%\rabbitmq-env.bat"

REM Uncomment this later, just for testing now
"!ERLANG_HOME!\bin\erl.exe" ^
-pa "!TDP0!..\ebin" ^
-noinput ^
-hidden ^
!RABBITMQ_CTL_ERL_ARGS! ^
-sasl errlog_type error ^
-mnesia dir \""!RABBITMQ_MNESIA_DIR:\=/!"\" ^
-s rabbit_control_main ^
-nodename !RABBITMQ_NODENAME! ^
-extra !STAR!

endlocal
endlocal