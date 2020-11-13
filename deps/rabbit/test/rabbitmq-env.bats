#!/usr/bin/env bats

export RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"

setup() {
  export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.$BATS_TEST_NAME.conf"
}

@test "default Erlang scheduler bind type" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    echo $RABBITMQ_SCHEDULER_BIND_TYPE

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +stbt db ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +stbt db "* ]]
}

@test "can configure Erlang scheduler bind type via conf file" {
    echo 'SCHEDULER_BIND_TYPE=u' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +stbt u ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +stbt u "* ]]
}

@test "can configure Erlang scheduler bind type via env" {
    RABBITMQ_SCHEDULER_BIND_TYPE=tnnps source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +stbt tnnps ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +stbt tnnps "* ]]
}

@test "Erlang scheduler bind type env takes precedence over conf file" {
    echo 'SCHEDULER_BIND_TYPE=s' > "$RABBITMQ_CONF_ENV_FILE"
    RABBITMQ_SCHEDULER_BIND_TYPE=nnps source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +stbt nnps ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +stbt nnps "* ]]
}

@test "default Erlang distribution buffer size" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +zdbbl 128000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +zdbbl 128000 "* ]]
}

@test "can configure Erlang distribution buffer size via conf file" {
    echo 'DISTRIBUTION_BUFFER_SIZE=123123' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +zdbbl 123123 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +zdbbl 123123 "* ]]
}

@test "can configure Erlang distribution buffer size via env" {
    RABBITMQ_DISTRIBUTION_BUFFER_SIZE=2000000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +zdbbl 2000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +zdbbl 2000000 "* ]]
}

@test "Erlang distribution buffer size env takes precedence over conf file" {
    echo 'DISTRIBUTION_BUFFER_SIZE=3000000' > "$RABBITMQ_CONF_ENV_FILE"
    RABBITMQ_DISTRIBUTION_BUFFER_SIZE=4000000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +zdbbl 4000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +zdbbl 4000000 "* ]]
}

@test "default Erlang maximum number of processes" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +P 1048576 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +P 1048576 "* ]]
}

@test "can configure Erlang maximum number of processes via conf file" {
    echo 'MAX_NUMBER_OF_PROCESSES=2000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +P 2000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +P 2000000 "* ]]
}

@test "can configure Erlang maximum number of processes via env" {
    RABBITMQ_MAX_NUMBER_OF_PROCESSES=3000000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +P 3000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +P 3000000 "* ]]
}

@test "Erlang maximum number of processes env takes precedence over conf file" {
    echo 'MAX_NUMBER_OF_PROCESSES=4000000' > "$RABBITMQ_CONF_ENV_FILE"
    RABBITMQ_MAX_NUMBER_OF_PROCESSES=5000000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +P 5000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +P 5000000 "* ]]
}

@test "default Erlang maximum number of atoms" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +t 5000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 5000000 "* ]]
}

@test "can configure Erlang maximum number of atoms via conf file" {
    echo 'MAX_NUMBER_OF_ATOMS=1000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +t 1000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 1000000 "* ]]
}

@test "can configure Erlang maximum number of atoms via env" {
    RABBITMQ_MAX_NUMBER_OF_ATOMS=2000000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +t 2000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 2000000 "* ]]
}

@test "Erlang maximum number of atoms env takes precedence over conf file" {
    echo 'MAX_NUMBER_OF_ATOMS=3000000' > "$RABBITMQ_CONF_ENV_FILE"
    RABBITMQ_MAX_NUMBER_OF_ATOMS=4000000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +t 4000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 4000000 "* ]]
}
