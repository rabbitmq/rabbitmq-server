@echo off

REM ### next line potentially updated in package install steps
REM set SYS_PREFIX=

REM ### next line will be updated when generating a standalone release
REM ERL_DIR=
set ERL_DIR=

REM These boot files don't appear to be referenced in the batch scripts
REM set CLEAN_BOOT_FILE=start_clean
REM set SASL_BOOT_FILE=start_sasl

if exist "%RABBITMQ_HOME%\erlang.mk" (
    REM RabbitMQ is executed from its source directory. The plugins
    REM directory and ERL_LIBS are tuned based on this.
    set RABBITMQ_DEV_ENV=1
)

REM ## Set default values

if "!RABBITMQ_BASE!"=="" (
    set RABBITMQ_BASE=!APPDATA!\RabbitMQ
)

REM Make sure $RABBITMQ_BASE contains no non-ASCII characters. We create
REM the directory first so we don't end up creating it later in its "short
REM filename" version.
if not exist "!RABBITMQ_BASE!" (
    mkdir "!RABBITMQ_BASE!"
)
for /f "delims=" %%F in ("!RABBITMQ_BASE!") do set RABBITMQ_BASE=%%~sF

REM BOOT_MODULE="rabbit"
REM CONFIG_FILE=${SYS_PREFIX}/etc/rabbitmq/rabbitmq
REM LOG_BASE=${SYS_PREFIX}/var/log/rabbitmq
REM MNESIA_BASE=${SYS_PREFIX}/var/lib/rabbitmq/mnesia
REM ENABLED_PLUGINS_FILE=${SYS_PREFIX}/etc/rabbitmq/enabled_plugins
set BOOT_MODULE=rabbit
set CONFIG_FILE=!RABBITMQ_BASE!\rabbitmq
set LOG_BASE=!RABBITMQ_BASE!\log
set MNESIA_BASE=!RABBITMQ_BASE!\db
set ENABLED_PLUGINS_FILE=!RABBITMQ_BASE!\enabled_plugins

REM PLUGINS_DIR="${RABBITMQ_HOME}/plugins"
for /f "delims=" %%F in ("!TDP0!..\plugins") do set PLUGINS_DIR=%%~dpsF%%~nF%%~xF

REM CONF_ENV_FILE=${SYS_PREFIX}/etc/rabbitmq/rabbitmq-env.conf
if "!RABBITMQ_CONF_ENV_FILE!"=="" (
    set RABBITMQ_CONF_ENV_FILE=!RABBITMQ_BASE!\rabbitmq-env-conf.bat
)
