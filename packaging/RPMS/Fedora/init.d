#!/bin/sh
#
# rabbitmq-server RabbitMQ broker
#
#chkconfig: 2345 80 05
#description: Enable AMQP service provided by RabbitMQ
#

### BEGIN INIT INFO
# Provides:          rabbitmq
# Required-Start:    $remote_fs $network
# Required-Stop:     $remote_fs $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Description:       RabbitMQ broker
# Short-Description: Enable AMQP service provided by RabbitMQ broker
### END INIT INFO

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
DAEMON_NAME=rabbitmq-multi
DAEMON=/usr/sbin/$DAEMON_NAME
NAME=rabbitmq-server
DESC=rabbitmq-server
USER=rabbitmq
NODE_COUNT=1
ROTATE_SUFFIX=

LOCK_FILE=/var/lock/subsys/$NAME

test -x $DAEMON || exit 0

# source function library
. /etc/rc.d/init.d/functions

# Include rabbitmq defaults if available
if [ -f /etc/default/rabbitmq ] ; then
	. /etc/default/rabbitmq
fi

RETVAL=0
set -e
cd /

start_rabbitmq () {
    set +e
    su $USER -s /bin/sh -c "$DAEMON start_all ${NODE_COUNT}" > /var/log/rabbitmq/startup.log 2> /var/log/rabbitmq/startup.err
    case "$?" in
      0)
        echo SUCCESS && touch $LOCK_FILE
        ;;
      1)
        echo TIMEOUT - check /var/log/rabbitmq/startup.\{log,err\}
        ;;
      *)
        echo FAILED - check /var/log/rabbitmq/startup.log, .err
        RETVAL=1;;
    esac 
    set -e
}

stop_rabbitmq () {
    set +e
    su $USER -s /bin/sh -c "$DAEMON stop_all" > /var/log/rabbitmq/shutdown.log 2> /var/log/rabbitmq/shutdown.err

    if [ $? != 0 ] ; then
        echo FAILED - check /var/log/rabbitmq/shutdown.log, .err
        RETVAL=$?
    else
        rm -rf $LOCK_FILE
        RETVAL=0
    fi
    set -e
}

status_rabbitmq () {
    status $NAME
}

restart_rabbitmq () {
    echo -n "Restarting $DESC: "
    stop_rabbitmq
    start_rabbitmq
    echo "$NAME."
}

rotate_logs_rabbitmq() {
    set +e
    su $USER -s /bin/sh -c "$DAEMON rotate_logs_all ${ROTATE_SUFFIX}" 2>&1
    RETVAL=$?
    set -e
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
  rotate-logs)
	echo -n "Rotating log files for $DESC: "
	rotate_logs_rabbitmq
	;;
  force-reload|reload|restart)
        restart_rabbitmq
	;;
  status)
	echo "Status of $DESC: "
	status_rabbitmq
	RETVAL=$?
	;;
  condrestart|try-restart)
        status_rabbitmq >/dev/null 2>&1 || exit 0
        restart_rabbitmq
        ;;
  *)
	echo "Usage: $0 {start|stop|rotate-logs|status|restart|condrestart|try-restart|reload|force-reload}" >&2
	RETVAL=1
	;;
esac

exit $RETVAL
