_rabbitmqctl_complete() {
    if [ -x /usr/lib/rabbitmq/bin/rabbitmqctl ]; then
        COMPREPLY=()
        local LANG=en_US.UTF-8
        local word="${COMP_WORDS[COMP_CWORD]}"
        local completions="$(export LANG=en_US.UTF-8; export LC_CTYPE=en_US.UTF-8; /usr/lib/rabbitmq/bin/rabbitmqctl --auto-complete $COMP_LINE)"
        COMPREPLY=( $(compgen -W "$completions" -- "$word") )
    fi
}

complete -f -F _rabbitmqctl_complete rabbitmqctl

complete -f -F _rabbitmqctl_complete rabbitmq-plugins

complete -f -F _rabbitmqctl_complete rabbitmq-diagnostics