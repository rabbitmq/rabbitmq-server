if which rabbitmqctl > /dev/null; then
    if [ -n "$BASH_VERSION" ]; then
        . /usr/lib/rabbitmq/autocomplete/bash_autocomplete.sh
    fi
fi