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
set RABBITMQ_HOME=%SCRIPT_DIR%..

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

REM Check for the short names here too
if "!RABBITMQ_USE_LONGNAME!"=="" (
    if "!USE_LONGNAME!"=="" (
	    set RABBITMQ_NAME_TYPE="-sname"
	)
)

if "!RABBITMQ_USE_LONGNAME!"=="true" (
    if "!USE_LONGNAME!"=="true" (
        set RABBITMQ_NAME_TYPE="-name"
	)
)

if "!COMPUTERNAME!"=="" (
    set COMPUTERNAME=localhost
)

REM [ "x" = "x$RABBITMQ_NODENAME" ] && RABBITMQ_NODENAME=${NODENAME}
if "!RABBITMQ_NODENAME!"=="" (
    if "!NODENAME!"=="" (
        set RABBITMQ_NODENAME=rabbit@!COMPUTERNAME!
    ) else (
        set RABBITMQ_NODENAME=!NODENAME!
    )
)

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

REM [ "x" = "x$RABBITMQ_MNESIA_BASE" ] && RABBITMQ_MNESIA_BASE=${MNESIA_BASE}
if "!RABBITMQ_MNESIA_BASE!"=="" (
	if "!MNESIA_BASE!"=="" (
		set RABBITMQ_MNESIA_BASE=!RABBITMQ_BASE!\db
	) else (
		set RABBITMQ_MNESIA_BASE=!MNESIA_BASE!
	)
)

REM [ "x" = "x$RABBITMQ_SERVER_START_ARGS" ] && RABBITMQ_SERVER_START_ARGS=${SERVER_START_ARGS}
REM No Windows equivalent 

REM [ "x" = "x$RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS" ] && RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=${SERVER_ADDITIONAL_ERL_ARGS}
REM No Windows equivalent

REM [ "x" = "x$RABBITMQ_MNESIA_DIR" ] && RABBITMQ_MNESIA_DIR=${MNESIA_DIR}
REM [ "x" = "x$RABBITMQ_MNESIA_DIR" ] && RABBITMQ_MNESIA_DIR=${RABBITMQ_MNESIA_BASE}/${RABBITMQ_NODENAME}
if "!RABBITMQ_MNESIA_DIR!"=="" (
	if "!MNESIA_DIR!"=="" (
		set RABBITMQ_MNESIA_DIR=!RABBITMQ_MNESIA_BASE!/!RABBITMQ_NODENAME!-mnesia
	) else (
		set RABBITMQ_MNESIA_DIR=!MNESIA_DIR!
	)
)

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
		set RABBITMQ_PLUGINS_EXPAND_DIR=!RABBITMQ_MNESIA_BASE!/!RABBITMQ_NODENAME!-plugins-expand
	) else (
		set RABBITMQ_PLUGINS_EXPAND_DIR=!PLUGINS_EXPAND_DIR!
	)
)

REM [ "x" = "x$RABBITMQ_ENABLED_PLUGINS_FILE" ] && RABBITMQ_ENABLED_PLUGINS_FILE=${ENABLED_PLUGINS_FILE}
if "!RABBITMQ_ENABLED_PLUGINS_FILE!"=="" (
	if "!ENABLED_PLUGINS_FILE!"=="" (
		set RABBITMQ_ENABLED_PLUGINS_FILE=!RABBITMQ_BASE!\enabled_plugins
	) else (
		set RABBITMQ_ENABLED_PLUGINS_FILE=!ENABLED_PLUGINS_FILE!
	)
)

REM [ "x" = "x$RABBITMQ_PLUGINS_DIR" ] && RABBITMQ_PLUGINS_DIR=${PLUGINS_DIR}
if "!RABBITMQ_PLUGINS_DIR!"=="" (
	if "!PLUGINS_DIR!"=="" (
		set RABBITMQ_PLUGINS_DIR=!RABBITMQ_BASE!\plugins
	) else (
		set RABBITMQ_PLUGINS_DIR=!PLUGINS_DIR!
	)
)

REM ## Log rotation
REM [ "x" = "x$RABBITMQ_LOGS" ] && RABBITMQ_LOGS=${LOGS}
REM [ "x" = "x$RABBITMQ_LOGS" ] && RABBITMQ_LOGS="${RABBITMQ_LOG_BASE}/${RABBITMQ_NODENAME}.log"
if "!RABBITMQ_LOGS!"=="" (
	if "!LOGS!"=="" (
		set LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!.log
	) else (
		set LOGS=!LOGS!
	)
)

REM [ "x" = "x$RABBITMQ_SASL_LOGS" ] && RABBITMQ_SASL_LOGS=${SASL_LOGS}
REM [ "x" = "x$RABBITMQ_SASL_LOGS" ] && RABBITMQ_SASL_LOGS="${RABBITMQ_LOG_BASE}/${RABBITMQ_NODENAME}-sasl.log"
if "!RABBITMQ_SASL_LOGS!"=="" (
	if "!SASL_LOGS!"=="" (
		set SASL_LOGS=!RABBITMQ_LOG_BASE!\!RABBITMQ_NODENAME!-sasl.log
	) else (
		set SASL_LOGS=!SASL_LOGS!
	)
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
 
REM ##--- End of overridden <var_name> variables
REM 
REM # Since we source this elsewhere, don't accidentally stop execution
REM true
