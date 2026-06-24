_rabbitmqctl_complete() {
    if [ -x /usr/lib/rabbitmq/bin/rabbitmqctl ]; then
        local word
        local LANG=en_US.UTF-8
        word="$1"
        local -a completions
        completions=("${(@f)$(export LANG=en_US.UTF-8; export LC_CTYPE=en_US.UTF-8; /usr/lib/rabbitmq/bin/rabbitmqctl autocomplete -- "$word")}")
        reply=("${(@)completions:#--}")
    fi
}

compctl -f -K _rabbitmqctl_complete rabbitmqctl

compctl -f -K _rabbitmqctl_complete rabbitmq-plugins

compctl -f -K _rabbitmqctl_complete rabbitmq-diagnostics
