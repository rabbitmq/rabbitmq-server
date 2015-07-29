@echo off

REM ### next line potentially updated in package install steps
REM set SYS_PREFIX=

REM ### next line will be updated when generating a standalone release
REM ERL_DIR=
set ERL_DIR=

REM These boot files don't appear to be referenced in the batch scripts
REM set CLEAN_BOOT_FILE=start_clean
REM set SASL_BOOT_FILE=start_sasl

REM ## Set default values

if "!RABBITMQ_BASE!"=="" (
    set RABBITMQ_BASE=!APPDATA!\RabbitMQ
)

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
set PLUGINS_DIR=!TDP0!..\plugins

REM CONF_ENV_FILE=${SYS_PREFIX}/etc/rabbitmq/rabbitmq-env.conf
if "!RABBITMQ_CONF_ENV_FILE!"=="" (
    set RABBITMQ_CONF_ENV_FILE=!RABBITMQ_BASE!\rabbitmq-env-conf.bat
)
