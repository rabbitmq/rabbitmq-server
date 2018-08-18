#!/usr/bin/env bats

export RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
export _rabbitmq_env_load='false'

setup() {
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default Erlang maximum number of atoms" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_atom_limit_arg

    echo "expected RABBITMQ_MAX_NUMBER_OF_ATOMS to be 5000000, but got: \"$RABBITMQ_MAX_NUMBER_OF_ATOMS\""
    (( RABBITMQ_MAX_NUMBER_OF_ATOMS == 5000000 ))
}

@test "can configure Erlang maximum number of atoms via rabbitmq-env.conf file" {
    echo 'MAX_NUMBER_OF_ATOMS=1000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_atom_limit_arg

    echo "expected RABBITMQ_MAX_NUMBER_OF_ATOMS to be 1000000, but got: \"$RABBITMQ_MAX_NUMBER_OF_ATOMS\""
    (( RABBITMQ_MAX_NUMBER_OF_ATOMS == 1000000 ))
}

@test "can configure Erlang maximum number of atoms via env" {
    RABBITMQ_MAX_NUMBER_OF_ATOMS=2000000
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_atom_limit_arg

    echo "expected RABBITMQ_MAX_NUMBER_OF_ATOMS to be 2000000, but got: \"$RABBITMQ_MAX_NUMBER_OF_ATOMS\""
    (( RABBITMQ_MAX_NUMBER_OF_ATOMS == 2000000 ))
}

@test "Erlang maximum number of atoms env takes precedence over rabbitmq-env.conf file" {
    echo 'MAX_NUMBER_OF_ATOMS=3000000' > "$RABBITMQ_CONF_ENV_FILE"
    RABBITMQ_MAX_NUMBER_OF_ATOMS=4000000
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_atom_limit_arg

    echo "expected RABBITMQ_MAX_NUMBER_OF_ATOMS to be 4000000, but got: \"$RABBITMQ_MAX_NUMBER_OF_ATOMS\""
    (( RABBITMQ_MAX_NUMBER_OF_ATOMS == 4000000 ))
}
