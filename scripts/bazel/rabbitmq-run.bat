@echo off
setLocal enableDelayedExpansion
setlocal enableextensions

set ORIGINAL_ARGS=%*

if not defined TEST_SRCDIR (
    set BASE_DIR=%cd%
) else (
    set BASE_DIR=%TEST_SRCDIR%/%TEST_WORKSPACE%
    set BASE_DIR=%BASE_DIR:/=\\%
)

if "%1" == "-C" (
    cd %2
    shift 2
)

:loop-args
if "%1" == "" goto :loop-args-end
if "%1" == "run-broker" (
    set CMD=%1
    shift
    goto :loop-args
)
if "%1" == "start-background-broker" (
    set CMD=%1
    shift
    goto :loop-args
)
if "%1" == "stop-node" (
    set CMD=%1
    shift
    goto :loop-args
)
if "%1" == "set-resource-alarm" (
    set CMD=%1
    shift
    goto :loop-args
)
if "%1" == "clean-resource-alarm" (
    set CMD=%1
    shift
    goto :loop-args
)
for /F "tokens=1,3 delims=. " %%a in ("%1") do (
   set %%a=%%b
)
shift
goto :loop-args
:loop-args-end

set DEFAULT_PLUGINS_DIR=%BASE_DIR%\{RABBITMQ_HOME}\plugins
if defined EXTRA_PLUGINS_DIR (
    set DEFAULT_PLUGINS_DIR=%DEFAULT_PLUGINS_DIR%;%EXTRA_PLUGINS_DIR%
)

if not defined TEST_TMPDIR (
    set TEST_TMPDIR=%TEMP%\rabbitmq-test-instances
)
set RABBITMQ_SCRIPTS_DIR=%BASE_DIR%\{RABBITMQ_HOME}\sbin
set RABBITMQ_PLUGINS=%RABBITMQ_SCRIPTS_DIR%\rabbitmq-plugins.bat
set RABBITMQ_SERVER=%RABBITMQ_SCRIPTS_DIR%\rabbitmq-server.bat
set RABBITMQCTL=%RABBITMQ_SCRIPTS_DIR%\rabbitmqctl.bat

set HOSTNAME=%COMPUTERNAME%

if not defined RABBITMQ_NODENAME set RABBITMQ_NODENAME=rabbit@%HOSTNAME%
if not defined RABBITMQ_NODENAME_FOR_PATHS set RABBITMQ_NODENAME_FOR_PATHS=%RABBITMQ_NODENAME%
set NODE_TMPDIR=%TEST_TMPDIR%\%RABBITMQ_NODENAME_FOR_PATHS%

set RABBITMQ_BASE=%NODE_TMPDIR%
set RABBITMQ_PID_FILE=%NODE_TMPDIR%\%{RABBITMQ_NODENAME_FOR_PATHS%.pid
set RABBITMQ_LOG_BASE=%NODE_TMPDIR%\log
set RABBITMQ_MNESIA_BASE=%NODE_TMPDIR%\mnesia
set RABBITMQ_MNESIA_DIR=%RABBITMQ_MNESIA_BASE%\%RABBITMQ_NODENAME_FOR_PATHS%
set RABBITMQ_QUORUM_DIR=%RABBITMQ_MNESIA_DIR%\quorum
set RABBITMQ_STREAM_DIR=%RABBITMQ_MNESIA_DIR%\stream
if not defined RABBITMQ_PLUGINS_DIR set RABBITMQ_PLUGINS_DIR=%DEFAULT_PLUGINS_DIR%
set RABBITMQ_PLUGINS_EXPAND_DIR=%NODE_TMPDIR%\plugins
set RABBITMQ_FEATURE_FLAGS_FILE=%NODE_TMPDIR%\feature_flags
set RABBITMQ_ENABLED_PLUGINS_FILE=%NODE_TMPDIR%\enabled_plugins

if not defined RABBITMQ_SERVER_START_ARGS (
    set RABBITMQ_SERVER_START_ARGS=-ra wal_sync_method sync
)

if not defined RABBITMQ_LOG (
    set RABBITMQ_LOG=debug,+color
)

if defined LEAVE_PLUGINS_DISABLED (
    set RABBITMQ_ENABLED_PLUGINS=
) else (
    set RABBITMQ_ENABLED_PLUGINS=ALL
)

if not exist "%TEST_TMPDIR%" mkdir %TEST_TMPDIR%

if not exist "%RABBITMQ_LOG_BASE%" mkdir %RABBITMQ_LOG_BASE%
if not exist "%RABBITMQ_MNESIA_BASE%" mkdir %RABBITMQ_MNESIA_BASE%
if not exist "%RABBITMQ_PLUGINS_EXPAND_DIR%" mkdir %RABBITMQ_PLUGINS_EXPAND_DIR%

if "%CMD%" == "run-broker" (
    set RABBITMQ_ALLOW_INPUT=true
    set RABBITMQ_CONFIG_FILE=%TEST_TMPDIR%\test.config

    > !RABBITMQ_CONFIG_FILE! (
        @echo [
        @echo   {rabbit, [
        @echo     {loopback_users, []}
        @echo   ]},
        @echo   {rabbitmq_management, []},
        @echo   {rabbitmq_mqtt, []},
        @echo   {rabbitmq_stomp, []},
        @echo   {ra, [
        @echo     {data_dir, "!RABBITMQ_QUORUM_DIR:\=\\!"},
        @echo     {wal_sync_method, sync}
        @echo   ]},
        @echo   {osiris, [
        @echo     {data_dir, "!RABBITMQ_STREAM_DIR:\=\\!"}
        @echo   ]}
        @echo ].
    )

    call %RABBITMQ_SCRIPTS_DIR%\rabbitmq-server.bat

    if ERRORLEVEL 1 (
        exit /B %ERRORLEVEL%
    )

    exit /B 0
)

if "%CMD%" == "start-background-broker" (
    echo ERROR: not implemented by rabbitmq-run.bat
    exit /b 1
)

if "%CMD%" == "stop-node" (
    echo ERROR: not implemented by rabbitmq-run.bat
    exit /b 1
)

if "%CMD%" == "set-resource-alarm" (
    echo ERROR: not implemented by rabbitmq-run.bat
    exit /b 1
)

if "%CMD%" == "clear-resource-alarm" (
    echo ERROR: not implemented by rabbitmq-run.bat
    exit /b 1
)

echo ERROR: unrecognized rabbitmq-run.bat args: "%ORIGINAL_ARGS%"
exit /b 1
