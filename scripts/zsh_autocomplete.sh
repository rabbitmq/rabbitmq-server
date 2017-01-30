_rabbitmqctl_complete() {
    if [ -x /usr/lib/rabbitmq/bin/rabbitmqctl ]; then
        local word completions a
        local LANG=en_US.UTF-8
        read -cl a
        word="$1"
        completions="$(export LANG=en_US.UTF-8; export LC_CTYPE=en_US.UTF-8; /usr/lib/rabbitmq/bin/rabbitmqctl --auto-complete ${=a})"
        reply=( "${(ps:\n:)completions}" )
    fi
}

compctl -f -K _rabbitmqctl_complete rabbitmqctl

compctl -f -K _rabbitmqctl_complete rabbitmq-plugins

compctl -f -K _rabbitmqctl_complete rabbitmq-diagnostics