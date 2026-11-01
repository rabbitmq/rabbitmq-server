@echo off

set SASL_BOOT_FILE=start_sasl
set CLEAN_BOOT_FILE=start_clean

if "!RABBITMQ_BASE!"=="" (
    set RABBITMQ_BASE=!APPDATA!\RabbitMQ
) else (
    set RABBITMQ_BASE=!RABBITMQ_BASE:"=!
)

if not exist "!RABBITMQ_BASE!" (
    mkdir "!RABBITMQ_BASE!"
)

if "!RABBITMQ_CONF_ENV_FILE!"=="" (
    set RABBITMQ_CONF_ENV_FILE=!RABBITMQ_BASE!\rabbitmq-env-conf.bat
)
