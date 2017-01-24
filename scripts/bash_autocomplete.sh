_rabbitmqctl_complete() {
    COMPREPLY=()
    local LANG=en_US.UTF-8
    local word="${COMP_WORDS[COMP_CWORD]}"
    local completions="$(/usr/lib/rabbitmq/bin/rabbitmqctl --auto-complete $COMP_LINE)"
    COMPREPLY=( $(compgen -W "$completions" -- "$word") )
}

complete -f -F _rabbitmqctl_complete rabbitmqctl
