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
REM  Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
REM

setlocal

rem Preserve values that might contain exclamation marks before
rem enabling delayed expansion
set TDP0=%~dp0
set STAR=%*
set CONF_SCRIPT_DIR="%~dp0"
setlocal enabledelayedexpansion
setlocal enableextensions

if ERRORLEVEL 1 (
    echo "Failed to enable command extensions!"
    exit /B 1
)

REM Get default settings with user overrides for (RABBITMQ_)<var_name>
REM Non-empty defaults should be set in rabbitmq-env
call "%TDP0%\rabbitmq-env.bat" %~n0

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

set RABBITMQ_EBIN_ROOT=!RABBITMQ_HOME!\ebin

CALL :get_noex !RABBITMQ_ADVANCED_CONFIG_FILE! RABBITMQ_ADVANCED_CONFIG_FILE_NOEX

if "!RABBITMQ_ADVANCED_CONFIG_FILE!" == "!RABBITMQ_ADVANCED_CONFIG_FILE_NOEX!" (
    set RABBITMQ_ADVANCED_CONFIG_FILE=!RABBITMQ_ADVANCED_CONFIG_FILE_NOEX!.config
    REM Try to create advanced config file, if it doesn't exist
    REM It still can fail to be created, but at least not for default install
    if not exist "!RABBITMQ_ADVANCED_CONFIG_FILE!" (
        echo []. > !RABBITMQ_ADVANCED_CONFIG_FILE!
    )
)

CALL :get_noex !RABBITMQ_CONFIG_FILE! RABBITMQ_CONFIG_FILE_NOEX

if "!RABBITMQ_CONFIG_FILE!" == "!RABBITMQ_CONFIG_FILE_NOEX!" (
    if exist "!RABBITMQ_CONFIG_FILE_NOEX!.config" (
        if exist "!RABBITMQ_CONFIG_FILE_NOEX!.conf" (
            rem Both files exist. Print a warning
            echo "WARNING: Both old (.config) and new (.conf) format config files exist."
            echo "WARNING: Using the old format config file: !RABBITMQ_CONFIG_FILE_NOEX!.config"
            echo "WARNING: Please update your config files to the new format and remove the old file"
        )
        set RABBITMQ_CONFIG_FILE=!RABBITMQ_CONFIG_FILE_NOEX!.config
    ) else if exist "!RABBITMQ_CONFIG_FILE_NOEX!.conf" (
        set RABBITMQ_CONFIG_FILE=!RABBITMQ_CONFIG_FILE_NOEX!.conf
    ) else (
        rem No config file exist. Use advanced config for -config arg.
        if exist "!RABBITMQ_ADVANCED_CONFIG_FILE!" (
            echo "WARNING: Using RABBITMQ_ADVANCED_CONFIG_FILE: !RABBITMQ_ADVANCED_CONFIG_FILE!"
        )
        set RABBITMQ_CONFIG_ARG_FILE=!RABBITMQ_ADVANCED_CONFIG_FILE!
    )
)

rem Set the -config argument.
rem The -config argument should not have extension.
rem the file should exist
rem the file should be a valid erlang term file

rem Config file extension is .config
if "!RABBITMQ_CONFIG_FILE_NOEX!.config" == "!RABBITMQ_CONFIG_FILE!" (
    set RABBITMQ_CONFIG_ARG_FILE=!RABBITMQ_CONFIG_FILE!
) else if "!RABBITMQ_CONFIG_FILE_NOEX!.conf" == "!RABBITMQ_CONFIG_FILE!" (
    set RABBITMQ_CONFIG_ARG_FILE=!RABBITMQ_ADVANCED_CONFIG_FILE!
) else if not "" == "!RABBITMQ_CONFIG_FILE!" (
    if not "!RABBITMQ_CONFIG_FILE_NOEX!" == "!RABBITMQ_CONFIG_FILE!" (
        rem Config file has an extension, but it's neither .conf or .config
        echo "ERROR: Wrong extension for RABBITMQ_CONFIG_FILE: !RABBITMQ_CONFIG_FILE!"
        echo "ERROR: extension should be either .conf or .config"
        exit /B 1
    )
)

CALL :get_noex !RABBITMQ_CONFIG_ARG_FILE! RABBITMQ_CONFIG_ARG_FILE_NOEX

if not "!RABBITMQ_CONFIG_ARG_FILE_NOEX!.config" == "!RABBITMQ_CONFIG_ARG_FILE!" (
    if "!RABBITMQ_CONFIG_ARG_FILE!" == "!RABBITMQ_ADVANCED_CONFIG_FILE!" (
        echo "ERROR: Wrong extension for RABBITMQ_ADVANCED_CONFIG_FILE: !RABBITMQ_ADVANCED_CONFIG_FILE!"
        echo "ERROR: extension should be .config"
        exit /B 1
    ) else (
        rem We should never got here, but still there should be some explanation
        echo "ERROR: Wrong extension for !RABBITMQ_CONFIG_ARG_FILE!"
        echo "ERROR: extension should be .config"
        exit /B 1
    )
)

rem Set -config if the file exists
if exist !RABBITMQ_CONFIG_ARG_FILE! (
    set RABBITMQ_CONFIG_ARG=-config "!RABBITMQ_CONFIG_ARG_FILE_NOEX!"
)

rem Set -conf and other generated config parameters
if "!RABBITMQ_CONFIG_FILE_NOEX!.conf" == "!RABBITMQ_CONFIG_FILE!" (
    if not exist "!RABBITMQ_SCHEMA_DIR!" (
        mkdir "!RABBITMQ_SCHEMA_DIR!"
    )

    if not exist "!RABBITMQ_GENERATED_CONFIG_DIR!" (
        mkdir "!RABBITMQ_GENERATED_CONFIG_DIR!"
    )

    copy /Y "!RABBITMQ_HOME!\priv\schema\rabbit.schema" "!RABBITMQ_SCHEMA_DIR!\rabbit.schema"

    set RABBITMQ_GENERATED_CONFIG_ARG=-conf "!RABBITMQ_CONFIG_FILE!" ^
                                      -conf_dir "!RABBITMQ_GENERATED_CONFIG_DIR!" ^
                                      -conf_script_dir !CONF_SCRIPT_DIR:\=/! ^
                                      -conf_schema_dir "!RABBITMQ_SCHEMA_DIR!" ^
                                      -conf_advanced "!RABBITMQ_ADVANCED_CONFIG_FILE!"
)

"!ERLANG_HOME!\bin\erl.exe" ^
        -pa "!RABBITMQ_EBIN_ROOT!" ^
        -boot !CLEAN_BOOT_FILE! ^
        -noinput -hidden ^
        -s rabbit_prelaunch ^
        !RABBITMQ_NAME_TYPE! rabbitmqprelaunch!RANDOM!!TIME:~9!@localhost ^
        -conf_advanced "!RABBITMQ_ADVANCED_CONFIG_FILE!" ^
        -rabbit feature_flags_file "!RABBITMQ_FEATURE_FLAGS_FILE!" ^
        -rabbit enabled_plugins_file "!RABBITMQ_ENABLED_PLUGINS_FILE!" ^
        -rabbit plugins_dir "!RABBITMQ_PLUGINS_DIR!" ^
        -extra "!RABBITMQ_NODENAME!"

if ERRORLEVEL 2 (
    rem dist port mentioned in config, do not attempt to set it
) else if ERRORLEVEL 1 (
    exit /B 1
) else (
    set RABBITMQ_DIST_ARG=-kernel inet_dist_listen_min !RABBITMQ_DIST_PORT! -kernel inet_dist_listen_max !RABBITMQ_DIST_PORT!
)

rem The default allocation strategy RabbitMQ is using was introduced
rem in Erlang/OTP 20.2.3. Earlier Erlang versions fail to start with
rem this configuration. We therefore need to ensure that erl accepts
rem these values before we can use them.
rem
rem The defaults are meant to reduce RabbitMQ's memory usage and help
rem it reclaim memory at the cost of a slight decrease in performance
rem (due to an increase in memory operations). These defaults can be
rem overridden using the RABBITMQ_SERVER_ERL_ARGS variable.

set RABBITMQ_DEFAULT_ALLOC_ARGS=+MBas ageffcbf +MHas ageffcbf +MBlmbcs 512 +MHlmbcs 512 +MMmcs 30

"!ERLANG_HOME!\bin\erl.exe" ^
    !RABBITMQ_DEFAULT_ALLOC_ARGS! ^
    -boot !CLEAN_BOOT_FILE! ^
    -noinput -eval "halt(0)"

if ERRORLEVEL 1 (
    set RABBITMQ_DEFAULT_ALLOC_ARGS=
)

set RABBITMQ_EBIN_PATH="-pa !RABBITMQ_EBIN_ROOT!"

set RABBITMQ_LISTEN_ARG=
if not "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
   if not "!RABBITMQ_NODE_PORT!"=="" (
      set RABBITMQ_LISTEN_ARG=-rabbit tcp_listeners [{\""!RABBITMQ_NODE_IP_ADDRESS!"\","!RABBITMQ_NODE_PORT!"}]
   )
)

REM If !RABBITMQ_LOGS! is '-', send all log messages to stdout. This is
REM particularly useful for Docker images.

if "!RABBITMQ_LOGS!" == "-" (
    set SASL_ERROR_LOGGER=tty
    set RABBIT_LAGER_HANDLER=tty
    set RABBITMQ_LAGER_HANDLER_UPGRADE=tty
) else (
    set SASL_ERROR_LOGGER=false
    set RABBIT_LAGER_HANDLER=\""!RABBITMQ_LOGS:\=/!"\"
    set RABBITMQ_LAGER_HANDLER_UPGRADE=\""!RABBITMQ_UPGRADE_LOG:\=/!"\"
)

set RABBITMQ_START_RABBIT=
if "!RABBITMQ_ALLOW_INPUT!"=="" (
    set RABBITMQ_START_RABBIT=!RABBITMQ_START_RABBIT! -noinput
)
if "!RABBITMQ_NODE_ONLY!"=="" (
    set RABBITMQ_START_RABBIT=!RABBITMQ_START_RABBIT! -s "!RABBITMQ_BOOT_MODULE!" boot
)

if "!RABBITMQ_IO_THREAD_POOL_SIZE!"=="" (
    set RABBITMQ_IO_THREAD_POOL_SIZE=64
)

rem Bump ETS table limit to 50000
if "!ERL_MAX_ETS_TABLES!"=="" (
    set ERL_MAX_ETS_TABLES=50000
)

set ENV_OK=true
CALL :check_not_empty "RABBITMQ_BOOT_MODULE" !RABBITMQ_BOOT_MODULE!
CALL :check_not_empty "RABBITMQ_NAME_TYPE" !RABBITMQ_NAME_TYPE!
CALL :check_not_empty "RABBITMQ_NODENAME" !RABBITMQ_NODENAME!


if "!ENV_OK!"=="false" (
    EXIT /b 78
)

"!ERLANG_HOME!\bin\erl.exe" ^
-pa "!RABBITMQ_EBIN_ROOT!" ^
-boot start_sasl ^
!RABBITMQ_START_RABBIT! ^
!RABBITMQ_CONFIG_ARG! ^
!RABBITMQ_GENERATED_CONFIG_ARG! ^
!RABBITMQ_NAME_TYPE! !RABBITMQ_NODENAME! ^
+W w ^
+A "!RABBITMQ_IO_THREAD_POOL_SIZE!" ^
!RABBITMQ_DEFAULT_ALLOC_ARGS! ^
!RABBITMQ_SERVER_ERL_ARGS! ^
!RABBITMQ_LISTEN_ARG! ^
-kernel inet_default_connect_options "[{nodelay, true}]" ^
!RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS! ^
-sasl errlog_type error ^
-sasl sasl_error_logger !SASL_ERROR_LOGGER! ^
-rabbit lager_log_root \""!RABBITMQ_LOG_BASE:\=/!"\" ^
-rabbit lager_default_file !RABBIT_LAGER_HANDLER! ^
-rabbit lager_upgrade_file !RABBITMQ_LAGER_HANDLER_UPGRADE! ^
-rabbit feature_flags_file \""!RABBITMQ_FEATURE_FLAGS_FILE:\=/!"\" ^
-rabbit enabled_plugins_file \""!RABBITMQ_ENABLED_PLUGINS_FILE:\=/!"\" ^
-rabbit plugins_dir \""!RABBITMQ_PLUGINS_DIR:\=/!"\" ^
-rabbit plugins_expand_dir \""!RABBITMQ_PLUGINS_EXPAND_DIR:\=/!"\" ^
-os_mon start_cpu_sup false ^
-os_mon start_disksup false ^
-os_mon start_memsup false ^
-mnesia dir \""!RABBITMQ_MNESIA_DIR:\=/!"\" ^
-ra data_dir \""!RABBITMQ_QUORUM_DIR:\=/!"\" ^
!RABBITMQ_SERVER_START_ARGS! ^
!RABBITMQ_DIST_ARG! ^
!STAR!

if ERRORLEVEL 1 (
    exit /B 1
)

EXIT /B 0

:check_not_empty
if "%~2"=="" (
    ECHO "Error: ENV variable should be defined: %1. Please check rabbitmq-env and rabbitmq-defaults, and !RABBITMQ_CONF_ENV_FILE! script files. Check also your Environment Variables settings"
    set ENV_OK=false
    EXIT /B 78
    )
EXIT /B 0

:get_noex
set "%~2=%~dpn1"
EXIT /B 0

endlocal
endlocal
endlocal
