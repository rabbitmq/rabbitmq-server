#!/bin/sh
#
# rabbitmq-server RabbitMQ broker
#
# chkconfig: - 80 05
# description: Enable AMQP service provided by RabbitMQ
#

### BEGIN INIT INFO
# Provides:          rabbitmq-server
# Required-Start:    $remote_fs $network
# Required-Stop:     $remote_fs $network
# Description:       RabbitMQ broker
# Short-Description: Enable AMQP service provided by RabbitMQ broker
### END INIT INFO

PATH=/sbin:/usr/sbin:/bin:/usr/bin
DAEMON=/usr/sbin/rabbitmq-multi
NAME=rabbitmq-server
DESC=rabbitmq-server
USER=rabbitmq
NODE_COUNT=1
ROTATE_SUFFIX=

LOCK_FILE=/var/lock/subsys/$NAME

test -x $DAEMON || exit 0

# Include rabbitmq defaults if available
if [ -f /etc/sysconfig/rabbitmq ] ; then
	. /etc/sysconfig/rabbitmq
fi

RETVAL=0
set -e

start_rabbitmq () {
    set +e
    $DAEMON start_all ${NODE_COUNT} > /var/log/rabbitmq/startup_log 2> /var/log/rabbitmq/startup_err
    case "$?" in
      0)
        echo SUCCESS && touch $LOCK_FILE
        RETVAL=0
        ;;
      1)
        echo TIMEOUT - check /var/log/rabbitmq/startup_\{log,err\}
        RETVAL=1
        ;;
      *)
        echo FAILED - check /var/log/rabbitmq/startup_log, _err
        RETVAL=1
        ;;
    esac 
    set -e
}

stop_rabbitmq () {
    set +e
    status_rabbitmq quiet
    if [ $RETVAL = 0 ] ; then
        $DAEMON stop_all > /var/log/rabbitmq/shutdown_log 2> /var/log/rabbitmq/shutdown_err
        RETVAL=$?
        if [ $RETVAL != 0 ] ; then
            echo FAILED - check /var/log/rabbitmq/shutdown_log, _err
        else
            rm -rf $LOCK_FILE
        fi
    else
        echo No nodes running 
        RETVAL=0
    fi
    set -e
}

status_rabbitmq() {
    set +e
    if [ "$1" != "quiet" ] ; then
        $DAEMON status 2>&1
    else
        $DAEMON status > /dev/null 2>&1
    fi
    if [ $? != 0 ] ; then
        RETVAL=1
    fi
    set -e
}

rotate_logs_rabbitmq() {
    set +e
    $DAEMON rotate_logs ${ROTATE_SUFFIX}
    if [ $? != 0 ] ; then
        RETVAL=1
    fi
    set -e
}

restart_rabbitmq() {
    stop_rabbitmq	    
    start_rabbitmq
}

case "$1" in
    start)
        echo -n "Starting $DESC: "
        start_rabbitmq
        echo "$NAME."
        ;;
    stop)
        echo -n "Stopping $DESC: "
        stop_rabbitmq
        echo "$NAME."
        ;;
    status)
        status_rabbitmq
        ;;
    rotate-logs)
        echo -n "Rotating log files for $DESC: "
        rotate_logs_rabbitmq
        ;;
    force-reload|reload|restart)
        echo -n "Restarting $DESC: "
        restart_rabbitmq
        echo "$NAME."
        ;;
    condrestart|try-restart)
        echo -n "Restarting $DESC: "
        restart_rabbitmq
        echo "$NAME."
        ;;
    *)
        echo "Usage: $0 {start|stop|status|rotate-logs|restart|condrestart|try-restart|reload|force-reload}" >&2
        RETVAL=2
        ;;
esac

exit $RETVAL
