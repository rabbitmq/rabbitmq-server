@echo off

set SASL_BOOT_FILE=start_sasl
set CLEAN_BOOT_FILE=start_clean
set BOOT_MODULE=rabbit

if "!RABBITMQ_BASE!"=="" (
    set RABBITMQ_BASE=!APPDATA!\RabbitMQ
) else (
    set RABBITMQ_BASE=!RABBITMQ_BASE:"=!
)

if not exist "!RABBITMQ_BASE!" (
    mkdir "!RABBITMQ_BASE!"
)

if "!RABBITMQ_CONF_ENV_FILE!"=="" (
    if "!CONF_ENV_FILE!"=="" (
        set CONF_ENV_FILE=!RABBITMQ_BASE!\rabbitmq-env-conf.bat
    )
)
