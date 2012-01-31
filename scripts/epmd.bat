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
REM  Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
REM

for /f "delims=" %%i in ('dir /ad/b "!ERLANG_HOME!"') do if exist "!ERLANG_HOME!\%%i\bin\epmd.exe" (
    call "!ERLANG_HOME!\%%i\bin\epmd.exe"
    if ERRORLEVEL 1 (
       exit /B 1
    )
)
