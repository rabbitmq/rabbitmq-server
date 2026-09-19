@echo off

REM $RABBITMQ_CONF_ENV_FILE (or its default, rabbitmq-env-conf.bat) is
REM called into this same cmd.exe process further below, so a plain
REM assignment in it would otherwise silently overwrite any RABBITMQ_*
REM variable already inherited from the environment, reversing the
REM documented precedence (environment, then rabbitmq-env-conf.bat,
REM then built-in defaults). Save every RABBITMQ_* variable already
REM defined here so it can be restored afterwards if the file tries to
REM change it. This must run before this script computes anything itself.
REM
REM This relies on delayed expansion (`setlocal enabledelayedexpansion`)
REM already being active in the calling process; every current caller
REM enables it before calling into this file.
REM
REM RABBITMQ_HOME is always recomputed a few lines down, so it is
REM excluded here rather than preserved: a pre-existing value would
REM otherwise look like a conflict against the freshly computed one and
REM get reverted, even with no conf file involved at all. RABBITMQ_BASE
REM and RABBITMQ_CONF_ENV_FILE are likewise normalized (quotes
REM stripped, or defaulted) by rabbitmq-defaults.bat below rather than
REM up front, so they would trip the same false conflict if preserved
REM here; RABBITMQ_BASE is saved separately below, right after that
REM normalization runs, so it is still protected against the conf file.
REM
REM Unlike the Unix version of this same mechanism, there is no
REM separate handling needed here for a variable explicitly set to an
REM empty string (e.g. RABBITMQ_FEATURE_FLAGS=""): "set VAR=" clears a
REM variable "as if it isn't there" on Windows, so an empty value and
REM an unset variable are the same, indistinguishable state here.
REM
REM Known gap, accepted rather than guarded against, flagged for
REM whoever validates this on real Windows: a value containing an
REM embedded newline could still forge a second candidate name if it
REM arrived from a non-cmd.exe source (PowerShell, a service manager, a
REM container runtime) that this script just inherits from.
REM
REM The value is read back via !%%A! rather than through the %%B token
REM `set` itself produced, because a delayed-expansion read is a single,
REM final substitution that is not rescanned -- unlike %%B, which would
REM be rescanned for "!" once substituted into the "set" command below,
REM silently corrupting a value such as an Erlang cookie that contains
REM a "!".
set "_RMQ_ENV_PRESERVED_VARS="
for /f "tokens=1 delims==" %%A in ('set RABBITMQ_ 2^>nul') do (
    if /i not "%%A"=="RABBITMQ_HOME" if /i not "%%A"=="RABBITMQ_BASE" if /i not "%%A"=="RABBITMQ_CONF_ENV_FILE" (
        set "_RMQ_ENV_SAVED_%%A=!%%A!"
        set "_RMQ_ENV_PRESERVED_VARS=!_RMQ_ENV_PRESERVED_VARS! %%A"
    )
)

REM Scopes the variables to the current batch file
REM setlocal

rem Preserve values that might contain exclamation marks before
rem enabling delayed expansion
set TDP0=%~dp0
REM setlocal enabledelayedexpansion

REM SCRIPT_DIR=`dirname $SCRIPT_PATH`
REM RABBITMQ_HOME="${SCRIPT_DIR}/.."
set SCRIPT_DIR=%TDP0%
set SCRIPT_NAME=%1
for /f "delims=" %%F in ("%SCRIPT_DIR%..") do set RABBITMQ_HOME=%%~dpF%%~nF%%~xF

if defined ERL_LIBS (
    set "ERL_LIBS=%RABBITMQ_HOME%\plugins;%ERL_LIBS%"
) else (
    set "ERL_LIBS=%RABBITMQ_HOME%\plugins"
)

REM If ERLANG_HOME is not defined, check if "erl.exe" is available in
REM the path and use that.
if not defined ERLANG_HOME (
    for /f "delims=" %%F in ('powershell.exe -NoLogo -NoProfile -NonInteractive -Command "(Get-Command erl.exe).Definition"') do @set ERL_PATH=%%F
    if exist "!ERL_PATH!" (
        for /f "delims=" %%F in ("!ERL_PATH!") do set ERL_DIRNAME=%%~dpF
        for /f "delims=" %%F in ("!ERL_DIRNAME!\..") do @set ERLANG_HOME=%%~dpF%%~nF%%~xF
    )
    set ERL_PATH=
    set ERL_DIRNAME=
)

REM ## Set defaults
call "%SCRIPT_DIR%\rabbitmq-defaults.bat"

REM RABBITMQ_BASE was excluded from the preserve loop above because it
REM is normalized by the call just above (quotes stripped, or defaulted
REM to %APPDATA%\RabbitMQ) rather than up front; it is preserved here
REM instead, after that normalization, so the conf file still cannot
REM silently override an inherited value.
set "_RMQ_ENV_SAVED_RABBITMQ_BASE=!RABBITMQ_BASE!"
set "_RMQ_ENV_PRESERVED_VARS=!_RMQ_ENV_PRESERVED_VARS! RABBITMQ_BASE"

set RABBITMQ_CONF_ENV_FILE=!RABBITMQ_CONF_ENV_FILE:"=!

REM RABBITMQ_CONF_ENV_FILE is excluded from the preserve loop above for
REM the same reason RABBITMQ_BASE is (its quotes are unconditionally
REM stripped just above), so it is preserved here too, right after that
REM normalization: rabbit_env.erl reads this variable directly from the
REM OS environment again when the Erlang node boots, so a conf file
REM that reassigns it would otherwise silently change which file the
REM node itself loads.
set "_RMQ_ENV_SAVED_RABBITMQ_CONF_ENV_FILE=!RABBITMQ_CONF_ENV_FILE!"
set "_RMQ_ENV_PRESERVED_VARS=!_RMQ_ENV_PRESERVED_VARS! RABBITMQ_CONF_ENV_FILE"

if exist "!RABBITMQ_CONF_ENV_FILE!" (
    call "!RABBITMQ_CONF_ENV_FILE!"
)

REM Restore whatever was preserved at the top of this script, warning
REM for any RABBITMQ_* variable the file just tried to change.
REM
REM The comparison strips quotes from both sides first: a value
REM compared as-is inside "..." can contain a literal '"' (for example
REM RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-ssl_dist_optfile "C:\..."),
REM which "if" cannot parse as a single quoted operand, unlike "set".
REM The tradeoff is that any change consisting only of quote characters
REM goes undetected, which is preferable to a syntax error.
for %%A in (!_RMQ_ENV_PRESERVED_VARS!) do (
    set "_RMQ_ENV_CHANGED="
    if not defined %%A set "_RMQ_ENV_CHANGED=1"
    if defined %%A if not "!%%A:"=!"=="!_RMQ_ENV_SAVED_%%A:"=!" set "_RMQ_ENV_CHANGED=1"
    if defined _RMQ_ENV_CHANGED (
        echo [warning] %%A is already set in the environment. 1>&2
        echo           The value set in rabbitmq-env-conf.bat is ignored. 1>&2
        set "%%A=!_RMQ_ENV_SAVED_%%A!"
    )
    set "_RMQ_ENV_SAVED_%%A="
)
set "_RMQ_ENV_PRESERVED_VARS="
set "_RMQ_ENV_CHANGED="

rem Bump ETS table limit to 50000
if "!ERL_MAX_ETS_TABLES!"=="" (
    set ERL_MAX_ETS_TABLES=50000
)

rem Default is defined here:
rem https://github.com/erlang/otp/blob/master/erts/emulator/beam/erl_port.h
if "!ERL_MAX_PORTS!"=="" (
    set ERL_MAX_PORTS=65536
)

set DEFAULT_SCHEDULER_BIND_TYPE=db
if "!RABBITMQ_SCHEDULER_BIND_TYPE!"=="" (
    set RABBITMQ_SCHEDULER_BIND_TYPE=!DEFAULT_SCHEDULER_BIND_TYPE!
)

set DEFAULT_DISTRIBUTION_BUFFER_SIZE=128000
if "!RABBITMQ_DISTRIBUTION_BUFFER_SIZE!"=="" (
    set RABBITMQ_DISTRIBUTION_BUFFER_SIZE=!DEFAULT_DISTRIBUTION_BUFFER_SIZE!
)

set DEFAULT_MAX_NUMBER_OF_PROCESSES=1048576
if "!RABBITMQ_MAX_NUMBER_OF_PROCESSES!"=="" (
    set RABBITMQ_MAX_NUMBER_OF_PROCESSES=!DEFAULT_MAX_NUMBER_OF_PROCESSES!
)

set DEFAULT_MAX_NUMBER_OF_ATOMS=5000000
if "!RABBITMQ_MAX_NUMBER_OF_ATOMS!"=="" (
    set RABBITMQ_MAX_NUMBER_OF_ATOMS=!DEFAULT_MAX_NUMBER_OF_ATOMS!
)

set DEFAULT_SCHEDULER_BUSY_WAIT_THRESHOLD=none
if "!RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD!"=="" (
    set RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD=!DEFAULT_SCHEDULER_BUSY_WAIT_THRESHOLD!
)

REM Common server defaults
set SERVER_ERL_ARGS=+pc unicode +P !RABBITMQ_MAX_NUMBER_OF_PROCESSES! +t !RABBITMQ_MAX_NUMBER_OF_ATOMS! +stbt !RABBITMQ_SCHEDULER_BIND_TYPE! +zdbbl !RABBITMQ_DISTRIBUTION_BUFFER_SIZE! +sbwt !RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD! +sbwtdcpu !RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD! +sbwtdio !RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD!

REM ##--- Set environment vars RABBITMQ_<var_name> to defaults if not set

REM [ "x" = "x$RABBITMQ_SERVER_ERL_ARGS" ] && RABBITMQ_SERVER_ERL_ARGS=${SERVER_ERL_ARGS}
if "!RABBITMQ_SERVER_ERL_ARGS!"=="" (
    set RABBITMQ_SERVER_ERL_ARGS=!SERVER_ERL_ARGS!
)

REM [ -n "$RABBITMQ_BOOT_MODULE" ] || RABBITMQ_BOOT_MODULE="rabbit"
if "!RABBITMQ_BOOT_MODULE!"=="" (
    set RABBITMQ_BOOT_MODULE=rabbit
)

if "!RABBITMQ_CTL_DIST_PORT_MIN!"=="" (
    set RABBITMQ_CTL_DIST_PORT_MIN=35672
)
if "!RABBITMQ_CTL_DIST_PORT_MAX!"=="" (
    set /a RABBITMQ_CTL_DIST_PORT_MAX=10+!RABBITMQ_CTL_DIST_PORT_MIN!
)

REM ADDITIONAL WINDOWS ONLY CONFIG ITEMS

if "!RABBITMQ_SERVICENAME!"=="" (
    set RABBITMQ_SERVICENAME=RabbitMQ
)

REM Environment cleanup
set SCRIPT_DIR=
set SCRIPT_NAME=
set TDP0=

REM ##--- End of overridden <var_name> variables

REM # Since we source this elsewhere, don't accidentally stop execution
REM true
