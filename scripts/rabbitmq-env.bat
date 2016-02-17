@echo off

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
for /f "delims=" %%F in ("%SCRIPT_DIR%..") do set RABBITMQ_HOME=%%~dpsF%%~nF%%~xF

REM If ERLANG_HOME is not defined, check if "erl.exe" is available in
REM the path and use that.
if not defined ERLANG_HOME (
    for /f "delims=" %%F in ('where.exe erl.exe') do @set ERL_PATH=%%F
    if exist "!ERL_PATH!" (
        for /f "delims=" %%F in ("!ERL_PATH!") do set ERL_DIRNAME=%%~dpF
        for /f "delims=" %%F in ("!ERL_DIRNAME!\..") do @set ERLANG_HOME=%%~dpsF%%~nF%%~xF
    )
    set ERL_PATH=
    set ERL_DIRNAME=
)

REM ## Set defaults
REM . ${SCRIPT_DIR}/rabbitmq-defaults
call "%SCRIPT_DIR%\rabbitmq-defaults.bat"

REM These common defaults aren't referenced in the batch scripts
REM ## Common defaults
REM SERVER_ERL_ARGS="+P 1048576"
REM
REM # warn about old rabbitmq.conf file, if no new one
REM if [ -f /etc/rabbitmq/rabbitmq.conf ] && \
REM    [ ! -f ${CONF_ENV_FILE} ] ; then
REM     echo -n "WARNING: ignoring /etc/rabbitmq/rabbitmq.conf -- "
REM     echo "location has moved to ${CONF_ENV_FILE}"
REM fi

REM ERL_ARGS aren't referenced in the batch scripts
REM Common defaults
REM set SERVER_ERL_ARGS=+P 1048576

REM ## Get configuration variables from the configure environment file
REM [ -f ${CONF_ENV_FILE} ] && . ${CONF_ENV_FILE} || true
if exist "!RABBITMQ_CONF_ENV_FILE!" (
    call "!RABBITMQ_CONF_ENV_FILE!"
)

REM Make sure $RABBITMQ_BASE contains no non-ASCII characters.
if not exist "!RABBITMQ_BASE!" (
    mkdir "!RABBITMQ_BASE!"
)
for /f "delims=" %%F in ("!RABBITMQ_BASE!") do set RABBITMQ_BASE=%%~sF

REM Check for the short names here too
if "!RABBITMQ_USE_LONGNAME!"=="" (
    if "!USE_LONGNAME!"=="" (
        set RABBITMQ_NAME_TYPE="-sname"
        set NAMETYPE=shortnames
    )
)

if "!RABBITMQ_USE_LONGNAME!"=="true" (
    if "!USE_LONGNAME!"=="true" (
        set RABBITMQ_NAME_TYPE="-name"
        set NAMETYPE=longnames
    )
)

REM [ "x" = "x$RABBITMQ_NODENAME" ] && RABBITMQ_NODENAME=${NODENAME}
if "!RABBITMQ_NODENAME!"=="" (
    if "!NODENAME!"=="" (
        REM We use Erlang to query the local hostname because
        REM !COMPUTERNAME! and Erlang may return different results.
	REM Start erl with -sname to make sure epmd is started.
	call "%ERLANG_HOME%\bin\erl.exe" -A0 -noinput -boot start_clean -sname rabbit-prelaunch-epmd -eval "init:stop()." >nul 2>&1
        for /f "delims=" %%F in ('call "%ERLANG_HOME%\bin\erl.exe" -A0 -noinput -boot start_clean -eval "net_kernel:start([list_to_atom(""rabbit-gethostname-"" ++ os:getpid()), %NAMETYPE%]), [_, H] = string:tokens(atom_to_list(node()), ""@""), io:format(""~s~n"", [H]), init:stop()."') do @set HOSTNAME=%%F
        set RABBITMQ_NODENAME=rabbit@!HOSTNAME!
        set HOSTNAME=
    ) else (
        set RABBITMQ_NODENAME=!NODENAME!
    )
)
set NAMETYPE=

REM
REM ##--- Set environment vars RABBITMQ_<var_name> to defaults if not set
REM
REM DEFAULT_NODE_IP_ADDRESS=auto
REM DEFAULT_NODE_PORT=5672
REM [ "x" = "x$RABBITMQ_NODE_IP_ADDRESS" ] && RABBITMQ_NODE_IP_ADDRESS=${NODE_IP_ADDRESS}
REM [ "x" = "x$RABBITMQ_NODE_PORT" ] && RABBITMQ_NODE_PORT=${NODE_PORT}
REM [ "x" = "x$RABBITMQ_NODE_IP_ADDRESS" ] && [ "x" != "x$RABBITMQ_NODE_PORT" ] && RABBITMQ_NODE_IP_ADDRESS=${DEFAULT_NODE_IP_ADDRESS}
REM [ "x" != "x$RABBITMQ_NODE_IP_ADDRESS" ] && [ "x" = "x$RABBITMQ_NODE_PORT" ] && RABBITMQ_NODE_PORT=${DEFAULT_NODE_PORT}

REM if "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
REM    if not "!RABBITMQ_NODE_PORT!"=="" (
REM       set RABBITMQ_NODE_IP_ADDRESS=auto
REM    )
REM ) else (
REM    if "!RABBITMQ_NODE_PORT!"=="" (
REM       set RABBITMQ_NODE_PORT=5672
REM    )
REM )

if "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
    if not "!NODE_IP_ADDRESS!"=="" (
        set RABBITMQ_NODE_IP_ADDRESS=!NODE_IP_ADDRESS!
    )
)

if "!RABBITMQ_NODE_PORT!"=="" (
    if not "!NODE_PORT!"=="" (
        set RABBITMQ_NODE_PORT=!NODE_PORT!
    )
)

if "!RABBITMQ_NODE_IP_ADDRESS!"=="" (
    if not "!RABBITMQ_NODE_PORT!"=="" (
       set RABBITMQ_NODE_IP_ADDRESS=auto
    )
) else (
    if "!RABBITMQ_NODE_PORT!"=="" (
       set RABBITMQ_NODE_PORT=5672
    )
)

REM [ "x" = "x$RABBITMQ_DIST_PORT" ] && RABBITMQ_DIST_PORT=${DIST_PORT}
REM [ "x" = "x$RABBITMQ_DIST_PORT" ] && [ "x" = "x$RABBITMQ_NODE_PORT" ] && RABBITMQ_DIST_PORT=$((${DEFAULT_NODE_PORT} + 20000))
REM [ "x" = "x$RABBITMQ_DIST_PORT" ] && [ "x" != "x$RABBITMQ_NODE_PORT" ] && RABBITMQ_DIST_PORT=$((${RABBITMQ_NODE_PORT} + 20000))

if "!RABBITMQ_DIST_PORT!"=="" (
    if "!DIST_PORT!"=="" (
        if "!RABBITMQ_NODE_PORT!"=="" (
            set RABBITMQ_DIST_PORT=25672
        ) else (
            set /a RABBITMQ_DIST_PORT=20000+!RABBITMQ_NODE_PORT!
        )
    ) else (
        set RABBITMQ_DIST_PORT=!DIST_PORT!
    )
)

REM [ "x" = "x$RABBITMQ_SERVER_ERL_ARGS" ] && RABBITMQ_SERVER_ERL_ARGS=${SERVER_ERL_ARGS}
REM No Windows equivalent

REM [ "x" = "x$RABBITMQ_CONFIG_FILE" ] && RABBITMQ_CONFIG_FILE=${CONFIG_FILE}
if "!RABBITMQ_CONFIG_FILE!"=="" (
    if "!CONFIG_FILE!"=="" (
        set RABBITMQ_CONFIG_FILE=!RABBITMQ_BASE!\rabbitmq
    ) else (
        set RABBITMQ_CONFIG_FILE=!CONFIG_FILE!
    )
)

REM [ "x" = "x$RABBITMQ_LOG_BASE" ] && RABBITMQ_LOG_BASE=${LOG_BASE}
if "!RABBITMQ_LOG_BASE!"=="" (
    if "!LOG_BASE!"=="" (
        set RABBITMQ_LOG_BASE=!RABBITMQ_BASE!\log
    ) else (
        set RABBITMQ_LOG_BASE=!LOG_BASE!
    )
)
if not exist "!RABBITMQ_LOG_BASE!" (
    mkdir "!RABBITMQ_LOG_BASE!"
)
for /f "delims=" %%F in ("!RABBITMQ_LOG_BASE!") do set RABBITMQ_LOG_BASE=%%~sF

REM [ "x" = "x$RABBITMQ_MNESIA_BASE" ] && RABBITMQ_MNESIA_BASE=${MNESIA_BASE}
if "!RABBITMQ_MNESIA_BASE!"=="" (
    if "!MNESIA_BASE!"=="" (
        set RABBITMQ_MNESIA_BASE=!RABBITMQ_BASE!\db
    ) else (
        set RABBITMQ_MNESIA_BASE=!MNESIA_BASE!
    )
)
if not exist "!RABBITMQ_MNESIA_BASE!" (
    mkdir "!RABBITMQ_MNESIA_BASE!"
)
for /f "delims=" %%F in ("!RABBITMQ_MNESIA_BASE!") do set RABBITMQ_MNESIA_BASE=%%~sF

REM [ "x" = "x$RABBITMQ_SERVER_START_ARGS" ] && RABBITMQ_SERVER_START_ARGS=${SERVER_START_ARGS}
REM No Windows equivalent

REM [ "x" = "x$RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS" ] && RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=${SERVER_ADDITIONAL_ERL_ARGS}
REM No Windows equivalent

REM [ "x" = "x$RABBITMQ_MNESIA_DIR" ] && RABBITMQ_MNESIA_DIR=${MNESIA_DIR}
REM [ "x" = "x$RABBITMQ_MNESIA_DIR" ] && RABBITMQ_MNESIA_DIR=${RABBITMQ_MNESIA_BASE}/${RABBITMQ_NODENAME}
if "!RABBITMQ_MNESIA_DIR!"=="" (
    if "!MNESIA_DIR!"=="" (
        set RABBITMQ_MNESIA_DIR=!RABBITMQ_MNESIA_BASE!\!RABBITMQ_NODENAME!-mnesia
    ) else (
        set RABBITMQ_MNESIA_DIR=!MNESIA_DIR!
    )
)
if not exist "!RABBITMQ_MNESIA_DIR!" (
    mkdir "!RABBITMQ_MNESIA_DIR!"
)
for /f "delims=" %%F in ("!RABBITMQ_MNESIA_DIR!") do set RABBITMQ_MNESIA_DIR=%%~sF

REM [ "x" = "x$RABBITMQ_PID_FILE" ] && RABBITMQ_PID_FILE=${PID_FILE}
REM [ "x" = "x$RABBITMQ_PID_FILE" ] && RABBITMQ_PID_FILE=${RABBITMQ_MNESIA_DIR}.pid
REM No Windows equivalent

REM [ "x" = "x$RABBITMQ_BOOT_MODULE" ] && RABBITMQ_BOOT_MODULE=${BOOT_MODULE}
if "!RABBITMQ_BOOT_MODULE!"=="" (
    if "!BOOT_MODULE!"=="" (
        set RABBITMQ_BOOT_MODULE=rabbit
    ) else (
        set RABBITMQ_BOOT_MODULE=!BOOT_MODULE!
    )
)

REM [ "x" = "x$RABBITMQ_PLUGINS_EXPAND_DIR" ] && RABBITMQ_PLUGINS_EXPAND_DIR=${PLUGINS_EXPAND_DIR}
REM [ "x" = "x$RABBITMQ_PLUGINS_EXPAND_DIR" ] && RABBITMQ_PLUGINS_EXPAND_DIR=${RABBITMQ_MNESIA_BASE}/${RABBITMQ_NODENAME}-plugins-expand
if "!RABBITMQ_PLUGINS_EXPAND_DIR!"=="" (
    if "!PLUGINS_EXPAND_DIR!"=="" (
        set RABBITMQ_PLUGINS_EXPAND_DIR=!RABBITMQ_MNESIA_BASE!\!RABBITMQ_NODENAME!-plugins-expand
    ) else (
        set RABBITMQ_PLUGINS_EXPAND_DIR=!PLUGINS_EXPAND_DIR!
    )
)
REM FIXME: RabbitMQ removes and recreates RABBITMQ_PLUGINS_EXPAND_DIR
REM itself. Therefore we can't create it here in advance and escape the
REM directory name, and RABBITMQ_PLUGINS_EXPAND_DIR must not contain
REM non-US-ASCII characters.

REM [ "x" = "x$RABBITMQ_ENABLED_PLUGINS_FILE" ] && RABBITMQ_ENABLED_PLUGINS_FILE=${ENABLED_PLUGINS_FILE}
if "!RABBITMQ_ENABLED_PLUGINS_FILE!"=="" (
    if "!ENABLED_PLUGINS_FILE!"=="" (
        set RABBITMQ_ENABLED_PLUGINS_FILE=!RABBITMQ_BASE!\enabled_plugins
    ) else (
        set RABBITMQ_ENABLED_PLUGINS_FILE=!ENABLED_PLUGINS_FILE!
    )
) else (
    set RABBITMQ_ENABLED_PLUGINS_FILE_source=environment
)
if not exist "!RABBITMQ_ENABLED_PLUGINS_FILE!" (
    for /f "delims=" %%F in ("!RABBITMQ_ENABLED_PLUGINS_FILE!") do mkdir %%~dpF 2>NUL
    copy /y NUL "!RABBITMQ_ENABLED_PLUGINS_FILE!" >NUL
)
for /f "delims=" %%F in ("!RABBITMQ_ENABLED_PLUGINS_FILE!") do set RABBITMQ_ENABLED_PLUGINS_FILE=%%~sF

REM [ "x" = "x$RABBITMQ_PLUGINS_DIR" ] && RABBITMQ_PLUGINS_DIR=${PLUGINS_DIR}
if "!RABBITMQ_PLUGINS_DIR!"=="" (
    if "!PLUGINS_DIR!"=="" (
        set RABBITMQ_PLUGINS_DIR=!RABBITMQ_HOME!\plugins
    ) else (
        set RABBITMQ_PLUGINS_DIR=!PLUGINS_DIR!
    )
) else (
    set RABBITMQ_PLUGINS_DIR_source=environment
)
if not exist "!RABBITMQ_PLUGINS_DIR!" (
    mkdir "!RABBITMQ_PLUGINS_DIR!"
)
for /f "delims=" %%F in ("!RABBITMQ_PLUGINS_DIR!") do set RABBITMQ_PLUGINS_DIR=%%~sF

REM ## Log rotation
REM [ "x" = "x$RABBITMQ_LOGS" ] && RABBITMQ_LOGS=${LOGS}
REM [ "x" = "x$RABBITMQ_LOGS" ] && RABBITMQ_LOGS="${RABBITMQ_LOG_BASE}/${RABBITMQ_NODENAME}.log"
if "!RABBITMQ_LOGS!"=="" (
    if "!LOGS!"=="" (
        set RABBITMQ_LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!.log
    ) else (
        set RABBITMQ_LOGS=!LOGS!
    )
)
if not "!RABBITMQ_LOGS" == "-" (
    if not exist "!RABBITMQ_LOGS!" (
        for /f "delims=" %%F in ("!RABBITMQ_LOGS!") do mkdir %%~dpF 2>NUL
        copy /y NUL "!RABBITMQ_LOGS!" >NUL
    )
    for /f "delims=" %%F in ("!RABBITMQ_LOGS!") do set RABBITMQ_LOGS=%%~sF
)

REM [ "x" = "x$RABBITMQ_SASL_LOGS" ] && RABBITMQ_SASL_LOGS=${SASL_LOGS}
REM [ "x" = "x$RABBITMQ_SASL_LOGS" ] && RABBITMQ_SASL_LOGS="${RABBITMQ_LOG_BASE}/${RABBITMQ_NODENAME}-sasl.log"
if "!RABBITMQ_SASL_LOGS!"=="" (
    if "!SASL_LOGS!"=="" (
        set RABBITMQ_SASL_LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!-sasl.log
    ) else (
        set RABBITMQ_SASL_LOGS=!SASL_LOGS!
    )
)
if not "!RABBITMQ_SASL_LOGS" == "-" (
    if not exist "!RABBITMQ_SASL_LOGS!" (
        for /f "delims=" %%F in ("!RABBITMQ_SASL_LOGS!") do mkdir %%~dpF 2>NUL
        copy /y NUL "!RABBITMQ_SASL_LOGS!" >NUL
    )
    for /f "delims=" %%F in ("!RABBITMQ_SASL_LOGS!") do set RABBITMQ_SASL_LOGS=%%~sF
)

REM [ "x" = "x$RABBITMQ_CTL_ERL_ARGS" ] && RABBITMQ_CTL_ERL_ARGS=${CTL_ERL_ARGS}
if "!$RABBITMQ_CTL_ERL_ARGS!"=="" (
    if not "!CTL_ERL_ARGS!"=="" (
        set RABBITMQ_CTL_ERL_ARGS=!CTL_ERL_ARGS!
    )
)

REM ADDITIONAL WINDOWS ONLY CONFIG ITEMS
REM rabbitmq-plugins.bat
REM if "!RABBITMQ_SERVICENAME!"=="" (
REM     set RABBITMQ_SERVICENAME=RabbitMQ
REM )

if "!RABBITMQ_SERVICENAME!"=="" (
    if "!SERVICENAME!"=="" (
        set RABBITMQ_SERVICENAME=RabbitMQ
    ) else (
        set RABBITMQ_SERVICENAME=!SERVICENAME!
    )
)

REM Development-specific environment.
if defined RABBITMQ_DEV_ENV (
    if "!SCRIPT_NAME!" == "rabbitmq-plugins" (
        REM We may need to query the running node for the plugins directory
        REM and the "enabled plugins" file.
        if not "%RABBITMQ_PLUGINS_DIR_source%" == "environment" (
            for /f "delims=" %%F in ('!SCRIPT_DIR!\rabbitmqctl eval "{ok, P} = application:get_env(rabbit, plugins_dir), io:format(""~s~n"", [P])."') do @set plugins_dir=%%F
            if exist "!plugins_dir!" (
                set RABBITMQ_PLUGINS_DIR=!plugins_dir!
            )
            REM set plugins_dir=
        )
        if not "%RABBITMQ_ENABLED_PLUGINS_FILE_source%" == "environment" (
            for /f "delims=" %%F in ('!SCRIPT_DIR!\rabbitmqctl eval "{ok, P} = application:get_env(rabbit, enabled_plugins_file), io:format(""~s~n"", [P])."') do @set enabled_plugins_file=%%F
            if exist "!enabled_plugins_file!" (
                set RABBITMQ_ENABLED_PLUGINS_FILE=!enabled_plugins_file!
            )
            REM set enabled_plugins_file=
        )
    )

    if exist "!RABBITMQ_PLUGINS_DIR!" (
        REM RabbitMQ was started with "make run-broker" from its own
        REM source tree. Take rabbit_common from the plugins directory.
        set ERL_LIBS=!RABBITMQ_PLUGINS_DIR!;!ERL_LIBS!
    ) else (
        REM RabbitMQ runs from a testsuite or a plugin. The .ez files are
        REM not available under RabbitMQ source tree. We need to look at
        REM $DEPS_DIR and default locations.

        if "!DEPS_DIR!" == "" (
            if exist "!RABBITMQ_HOME!\..\..\deps\rabbit_common\erlang.mk" (
                REM Dependencies in the Umbrella or a plugin.
                set DEPS_DIR_norm="!RABBITMQ_HOME!\..\..\deps"
            ) else (
                if exist "!RABBITMQ_HOME!\deps\rabbit_common\erlang.mk" (
                    REM Dependencies in the broker.
                    set DEPS_DIR_norm="!RABBITMQ_HOME!\deps"
                )
            )
        ) else (
            for /f "delims=" %%F in ("!DEPS_DIR!") do @set DEPS_DIR_norm=%%~dpsF%%~nF%%~xF
        )

        set ERL_LIBS=!DEPS_DIR_norm!;!ERL_LIBS!
    )
) else (
    if exist "!RABBITMQ_PLUGINS_DIR!" (
        REM RabbitMQ was started from its install directory. Take
        REM rabbit_common from the plugins directory.
        set ERL_LIBS=!RABBITMQ_PLUGINS_DIR!;!ERL_LIBS!
    )
)

REM Ensure all paths in ERL_LIBS do not contains non-ASCII characters.
set ERL_LIBS_orig=%ERL_LIBS%
set ERL_LIBS=
call :filter_paths "%ERL_LIBS_orig%"
goto :filter_paths_done

:filter_paths
set paths=%1
set paths=%paths:"=%
for /f "tokens=1* delims=;" %%a in ("%paths%") do (
    if not "%%a" == "" call :filter_path %%a
    if not "%%b" == "" call :filter_paths %%b
)
set paths=
exit /b

:filter_path
set ERL_LIBS=%ERL_LIBS%;%~dps1%~n1%~x1
exit /b

:filter_paths_done

REM Environment cleanup
set BOOT_MODULE=
set CONFIG_FILE=
set ENABLED_PLUGINS_FILE=
set LOG_BASE=
set MNESIA_BASE=
set PLUGINS_DIR=
set SCRIPT_DIR=
set SCRIPT_NAME=
set TDP0=

REM ##--- End of overridden <var_name> variables
REM
REM # Since we source this elsewhere, don't accidentally stop execution
REM true
