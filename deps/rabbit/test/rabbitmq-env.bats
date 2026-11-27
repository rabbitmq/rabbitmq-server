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

@test "atom limit jitter includes zero offset" {
    od() { echo 0; }
    RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=500000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [ "$RABBITMQ_MAX_NUMBER_OF_ATOMS" -eq 5000000 ]
}

@test "atom limit jitter includes its upper bound" {
    od() { echo 500000; }
    RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=500000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [ "$RABBITMQ_MAX_NUMBER_OF_ATOMS" -eq 5500000 ]
}

@test "atom limit jitter can be configured via conf file" {
    echo 'MAX_NUMBER_OF_ATOMS_JITTER=500000' > "$RABBITMQ_CONF_ENV_FILE"
    od() { echo 100000; }
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [ "$RABBITMQ_MAX_NUMBER_OF_ATOMS" -eq 5100000 ]
}

@test "atom limit jitter env takes precedence over conf file" {
    echo 'MAX_NUMBER_OF_ATOMS_JITTER=500000' > "$RABBITMQ_CONF_ENV_FILE"
    od() { echo 500000; }
    RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=100000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [ "$RABBITMQ_MAX_NUMBER_OF_ATOMS" -eq 5099996 ]
}

@test "zero atom limit jitter does not read randomness" {
    od() { return 1; }
    RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=0 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [ "$RABBITMQ_MAX_NUMBER_OF_ATOMS" -eq 5000000 ]
}

@test "explicit atom limit in conf file disables jitter" {
    echo 'MAX_NUMBER_OF_ATOMS=1000000' > "$RABBITMQ_CONF_ENV_FILE"
    od() { return 1; }
    RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=500000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [ "$RABBITMQ_MAX_NUMBER_OF_ATOMS" -eq 1000000 ]
}

@test "explicit atom limit in env disables jitter" {
    od() { return 1; }
    RABBITMQ_MAX_NUMBER_OF_ATOMS=2000000 RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=500000 \
        source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 2000000 "* ]]
}

@test "atom limit jitter rejects malformed and excessive ranges" {
    for value in -1 invalid 1.5 010 129217728 999999999999999999999; do
        run env RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER="$value" \
            sh -c '. "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"'
        [ "$status" -eq 78 ]
        [[ $output == *"MAX_NUMBER_OF_ATOMS_JITTER must be an integer"* ]]
    done
}

@test "atom limit jitter fails if randomness is unavailable" {
    od() { return 1; }
    export RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=500000
    run source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [ "$status" -eq 78 ]
    [[ $output == *"Could not read randomness"* ]]
}

@test "sourcing the environment twice preserves the selected atom limit" {
    od() { echo 123456; }
    RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=500000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    limit=$RABBITMQ_MAX_NUMBER_OF_ATOMS
    od() { return 1; }
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [ "$RABBITMQ_MAX_NUMBER_OF_ATOMS" -eq "$limit" ]
}

@test "Erlang VM uses the randomized atom limit" {
    RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=500000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    [ "$RABBITMQ_MAX_NUMBER_OF_ATOMS" -ge 5000000 ]
    [ "$RABBITMQ_MAX_NUMBER_OF_ATOMS" -le 5500000 ]
    run erl $RABBITMQ_SERVER_ERL_ARGS +S 1:1 -noshell \
        -eval 'io:format("~b", [erlang:system_info(atom_limit)]), halt().'
    [ "$status" -eq 0 ]
    [ "$output" -eq "$RABBITMQ_MAX_NUMBER_OF_ATOMS" ]
}

@test "explicit additional Erlang atom limit overrides jitter" {
    export RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='+t 6000000'
    RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=500000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    run erl $RABBITMQ_SERVER_ERL_ARGS $RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS +S 1:1 -noshell \
        -eval 'io:format("~b", [erlang:system_info(atom_limit)]), halt().'
    [ "$status" -eq 0 ]
    [ "$output" -eq 6000000 ]
}

@test "explicit Erlang server arguments override jitter" {
    export RABBITMQ_SERVER_ERL_ARGS='+t 6000000'
    RABBITMQ_MAX_NUMBER_OF_ATOMS_JITTER=500000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    run erl $RABBITMQ_SERVER_ERL_ARGS +S 1:1 -noshell \
        -eval 'io:format("~b", [erlang:system_info(atom_limit)]), halt().'
    [ "$status" -eq 0 ]
    [ "$output" -eq 6000000 ]
}

@test "default Erlang scheduler busy wait threshold" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    echo $RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +sbwt none ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +sbwt none "* ]]
}

@test "can configure Erlang scheduler busy wait threshold via conf file" {
    echo 'SCHEDULER_BUSY_WAIT_THRESHOLD=medium' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +sbwt medium ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +sbwt medium "* ]]
}

@test "can configure Erlang scheduler busy wait threshold via env" {
    RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD=long source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +sbwt long ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +sbwt long "* ]]
}

@test "Erlang scheduler busy wait threshold env takes precedence over conf file" {
    echo 'SCHEDULER_BIND_TYPE=s' > "$RABBITMQ_CONF_ENV_FILE"
    RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD=short source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +sbwt short ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +sbwt short "* ]]
}
