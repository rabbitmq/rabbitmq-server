if which rabbitmqctl; then
    if [ -n "$BASH_VERSION" ]; then
      _rabbitmqctl_complete() {
          COMPREPLY=()
          local word="${COMP_WORDS[COMP_CWORD]}"
          local completions="$(/usr/lib/rabbitmq/bin/rabbitmqctl --auto-complete "$COMP_LINE")"
          COMPREPLY=( $(compgen -W "$completions" -- "$word") )
        }

        complete -f -F _rabbitmqctl_complete rabbitmqctl
    elif [ -n "$ZSH_VERSION" ]; then
      _rabbitmqctl_complete() {
          local word completions a
          read -cl a
          word="$1"
          completions="$(/usr/lib/rabbitmq/bin/rabbitmqctl --auto-complete "${a}")"
          reply=( "${(ps:\n:)completions}" )
        }

        compctl -f -K _rabbitmqctl_complete rabbitmqctl
    fi
fi