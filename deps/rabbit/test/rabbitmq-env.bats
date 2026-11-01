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
    echo 'RABBITMQ_SCHEDULER_BIND_TYPE=u' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +stbt u ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +stbt u "* ]]
}

@test "bare (non-RABBITMQ_-prefixed) scheduler bind type in conf file is ignored" {
    echo 'SCHEDULER_BIND_TYPE=u' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain the default ' +stbt db ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +stbt db "* ]]
}

@test "can configure Erlang scheduler bind type via env" {
    RABBITMQ_SCHEDULER_BIND_TYPE=tnnps source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +stbt tnnps ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +stbt tnnps "* ]]
}

@test "Erlang scheduler bind type env takes precedence over conf file" {
    echo 'RABBITMQ_SCHEDULER_BIND_TYPE=s' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_SCHEDULER_BIND_TYPE=nnps
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +stbt nnps ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +stbt nnps "* ]]
}

@test "default Erlang distribution buffer size" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +zdbbl 128000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +zdbbl 128000 "* ]]
}

@test "can configure Erlang distribution buffer size via conf file" {
    echo 'RABBITMQ_DISTRIBUTION_BUFFER_SIZE=123123' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +zdbbl 123123 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +zdbbl 123123 "* ]]
}

@test "bare (non-RABBITMQ_-prefixed) distribution buffer size in conf file is ignored" {
    echo 'DISTRIBUTION_BUFFER_SIZE=123123' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain the default ' +zdbbl 128000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +zdbbl 128000 "* ]]
}

@test "can configure Erlang distribution buffer size via env" {
    RABBITMQ_DISTRIBUTION_BUFFER_SIZE=2000000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +zdbbl 2000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +zdbbl 2000000 "* ]]
}

@test "Erlang distribution buffer size env takes precedence over conf file" {
    echo 'RABBITMQ_DISTRIBUTION_BUFFER_SIZE=3000000' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_DISTRIBUTION_BUFFER_SIZE=4000000
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +zdbbl 4000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +zdbbl 4000000 "* ]]
}

@test "default Erlang maximum number of processes" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +P 1048576 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +P 1048576 "* ]]
}

@test "can configure Erlang maximum number of processes via conf file" {
    echo 'RABBITMQ_MAX_NUMBER_OF_PROCESSES=2000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +P 2000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +P 2000000 "* ]]
}

@test "bare (non-RABBITMQ_-prefixed) maximum number of processes in conf file is ignored" {
    echo 'MAX_NUMBER_OF_PROCESSES=2000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain the default ' +P 1048576 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +P 1048576 "* ]]
}

@test "can configure Erlang maximum number of processes via env" {
    RABBITMQ_MAX_NUMBER_OF_PROCESSES=3000000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +P 3000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +P 3000000 "* ]]
}

@test "Erlang maximum number of processes env takes precedence over conf file" {
    echo 'RABBITMQ_MAX_NUMBER_OF_PROCESSES=4000000' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_MAX_NUMBER_OF_PROCESSES=5000000
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +P 5000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +P 5000000 "* ]]
}

@test "default Erlang maximum number of atoms" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +t 5000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 5000000 "* ]]
}

@test "can configure Erlang maximum number of atoms via conf file" {
    echo 'RABBITMQ_MAX_NUMBER_OF_ATOMS=1000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +t 1000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 1000000 "* ]]
}

@test "bare (non-RABBITMQ_-prefixed) maximum number of atoms in conf file is ignored" {
    echo 'MAX_NUMBER_OF_ATOMS=1000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain the default ' +t 5000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 5000000 "* ]]
}

@test "can configure Erlang maximum number of atoms via env" {
    RABBITMQ_MAX_NUMBER_OF_ATOMS=2000000 source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +t 2000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 2000000 "* ]]
}

@test "Erlang maximum number of atoms env takes precedence over conf file" {
    echo 'RABBITMQ_MAX_NUMBER_OF_ATOMS=3000000' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_MAX_NUMBER_OF_ATOMS=4000000
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +t 4000000 ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +t 4000000 "* ]]
}

@test "default Erlang scheduler busy wait threshold" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    echo $RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +sbwt none ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +sbwt none "* ]]
}

@test "can configure Erlang scheduler busy wait threshold via conf file" {
    echo 'RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD=medium' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +sbwt medium ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +sbwt medium "* ]]
}

@test "bare (non-RABBITMQ_-prefixed) scheduler busy wait threshold in conf file is ignored" {
    echo 'SCHEDULER_BUSY_WAIT_THRESHOLD=medium' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain the default ' +sbwt none ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +sbwt none "* ]]
}

@test "can configure Erlang scheduler busy wait threshold via env" {
    RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD=long source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +sbwt long ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +sbwt long "* ]]
}

@test "Erlang scheduler busy wait threshold env takes precedence over conf file" {
    echo 'RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD=medium' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_SCHEDULER_BUSY_WAIT_THRESHOLD=short
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain ' +sbwt short ', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *" +sbwt short "* ]]
}

@test "default Erlang boot module" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_BOOT_MODULE to be 'rabbit', but got: $RABBITMQ_BOOT_MODULE"
    [ "$RABBITMQ_BOOT_MODULE" = "rabbit" ]
}

@test "can configure Erlang boot module via conf file" {
    echo 'RABBITMQ_BOOT_MODULE=my_boot_module' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_BOOT_MODULE to be 'my_boot_module', but got: $RABBITMQ_BOOT_MODULE"
    [ "$RABBITMQ_BOOT_MODULE" = "my_boot_module" ]
}

@test "bare (non-RABBITMQ_-prefixed) boot module in conf file is ignored" {
    echo 'BOOT_MODULE=my_boot_module' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_BOOT_MODULE to default to 'rabbit', but got: $RABBITMQ_BOOT_MODULE"
    [ "$RABBITMQ_BOOT_MODULE" = "rabbit" ]
}

@test "can configure Erlang boot module via env" {
    export RABBITMQ_BOOT_MODULE=my_boot_module
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_BOOT_MODULE to be 'my_boot_module', but got: $RABBITMQ_BOOT_MODULE"
    [ "$RABBITMQ_BOOT_MODULE" = "my_boot_module" ]
}

@test "Erlang boot module env takes precedence over conf file" {
    echo 'RABBITMQ_BOOT_MODULE=from_conf_file' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_BOOT_MODULE=from_env
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_BOOT_MODULE to be 'from_env', but got: $RABBITMQ_BOOT_MODULE"
    [ "$RABBITMQ_BOOT_MODULE" = "from_env" ]
}

@test "an exported but empty env var does not take precedence over conf file" {
    echo 'RABBITMQ_BOOT_MODULE=from_conf_file' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_BOOT_MODULE=""
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_BOOT_MODULE to be 'from_conf_file', but got: $RABBITMQ_BOOT_MODULE"
    [ "$RABBITMQ_BOOT_MODULE" = "from_conf_file" ]
}

@test "a precedence conflict prints a warning" {
    echo 'RABBITMQ_BOOT_MODULE=from_conf_file' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_BOOT_MODULE=from_env
    run source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected output to mention RABBITMQ_BOOT_MODULE was already set, but got: $output"
    [[ $output == *"RABBITMQ_BOOT_MODULE is already set in the environment"* ]]
}

@test "no precedence conflict means no warning" {
    echo 'RABBITMQ_BOOT_MODULE=from_conf_file' > "$RABBITMQ_CONF_ENV_FILE"
    run source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected no warning, but got: $output"
    [[ $output != *"is already set in the environment"* ]]
}

@test "an embedded newline in one variable's value does not corrupt another" {
    export RABBITMQ_SOME_VAR="$(printf 'line one\nRABBITMQ_NODENAME=forged')"
    export RABBITMQ_NODENAME=real
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_NODENAME to remain 'real', but got: $RABBITMQ_NODENAME"
    [ "$RABBITMQ_NODENAME" = "real" ]
}

@test "a precedence conflict never prints the value, not just for named secrets" {
    # The warning must not print a value at all: RABBITMQ_* is an
    # open-ended namespace, not limited to the names rabbit_env.erl
    # itself treats as secret (e.g. RABBITMQ_CTL_ERL_ARGS can carry
    # "-setcookie ..."), and this warning isn't gated behind debug
    # logging. Checked here on an arbitrary, non-allowlisted variable.
    echo 'RABBITMQ_CTL_ERL_ARGS=-proto_dist_from_conf_file' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_CTL_ERL_ARGS=-setcookie_super_secret_value
    run source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    # A single compound assertion: bats 0.4 only checks the exit status
    # of a test's last statement, so two separate `[[ ]]` lines here
    # would let a passing second one mask a failing first one.
    echo "expected the warning to mention the variable but not the secret value, but got: $output"
    [[ $output == *"RABBITMQ_CTL_ERL_ARGS is already set in the environment"* && $output != *"super_secret_value"* ]]
}

@test "set -a auto-exporting RABBITMQ_HOME does not block a conf file override" {
    echo 'RABBITMQ_HOME=/custom/home' > "$RABBITMQ_CONF_ENV_FILE"
    set -a
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    set +a

    echo "expected RABBITMQ_HOME to be '/custom/home', but got: $RABBITMQ_HOME"
    [ "$RABBITMQ_HOME" = "/custom/home" ]
}

@test "a pre-set RABBITMQ_HOME does not block its own recomputation" {
    export RABBITMQ_HOME=/should/be/ignored
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_HOME to have been recomputed, but got: $RABBITMQ_HOME"
    [ "$RABBITMQ_HOME" != "/should/be/ignored" ]
}

@test "RABBITMQ_SCRIPTS_DIR env takes precedence over conf file" {
    local expected="$RABBITMQ_SCRIPTS_DIR"
    echo 'RABBITMQ_SCRIPTS_DIR=/from/conf/file' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_SCRIPTS_DIR to remain '$expected', but got: $RABBITMQ_SCRIPTS_DIR"
    [ "$RABBITMQ_SCRIPTS_DIR" = "$expected" ]
}

@test "a precedence conflict on the erlang cookie never prints the value" {
    echo 'RABBITMQ_ERLANG_COOKIE=from-conf-file' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_ERLANG_COOKIE=super-secret-cookie
    run source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected the warning to mention the variable but not the secret value, but got: $output"
    [[ $output == *"RABBITMQ_ERLANG_COOKIE is already set in the environment"* && $output != *"super-secret-cookie"* ]]
}

@test "an exported empty RABBITMQ_FEATURE_FLAGS takes precedence over conf file" {
    # rabbit_env.erl reads this with keep_empty_string_as_is: an
    # empty value means "force zero feature flags", a real directive,
    # not the same as unset.
    echo 'RABBITMQ_FEATURE_FLAGS=some_flag' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_FEATURE_FLAGS=""
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_FEATURE_FLAGS to remain empty, but got: $RABBITMQ_FEATURE_FLAGS"
    [ "$RABBITMQ_FEATURE_FLAGS" = "" ]
}

@test "an exported empty RABBITMQ_ENABLED_PLUGINS takes precedence over conf file" {
    # Same as RABBITMQ_FEATURE_FLAGS: an empty value here means
    # "enable no plugins", not unset.
    echo 'RABBITMQ_ENABLED_PLUGINS=rabbitmq_management' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_ENABLED_PLUGINS=""
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_ENABLED_PLUGINS to remain empty, but got: $RABBITMQ_ENABLED_PLUGINS"
    [ "$RABBITMQ_ENABLED_PLUGINS" = "" ]
}

@test "conf file unsetting RABBITMQ_FEATURE_FLAGS does not defeat an exported empty value" {
    # A value comparison alone can't tell "conf file left this alone"
    # apart from "conf file ran `unset VAR`" when the preserved value
    # is itself empty: both look like "" == "" to a plain compare.
    echo 'unset RABBITMQ_FEATURE_FLAGS' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_FEATURE_FLAGS=""
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_FEATURE_FLAGS to still be set (to empty), but it is unset"
    [ -n "${RABBITMQ_FEATURE_FLAGS+x}" ]
}

@test "a value restored after an unset conf file directive is exported to child processes" {
    # `unset` in the conf file strips the export attribute along with
    # the value; the restore must re-export, not just reassign, or
    # the restored value never reaches the erl process this script
    # eventually execs.
    echo 'unset RABBITMQ_FEATURE_FLAGS' > "$RABBITMQ_CONF_ENV_FILE"
    export RABBITMQ_FEATURE_FLAGS=""
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    run sh -c 'echo "${RABBITMQ_FEATURE_FLAGS+set}"'

    echo "expected the restored value to be exported to a child process, but got: $output"
    [ "$output" = "set" ]
}

@test "an embedded newline cannot fake RABBITMQ_FEATURE_FLAGS into looking set" {
    # RABBITMQ_FEATURE_FLAGS is preserved unconditionally, even when
    # empty (see above), so it must not be fooled by env's line-based
    # parsing into treating a genuinely-unset variable as if it had
    # been exported empty: that would block a legitimate conf file
    # value the same way a real conflict would.
    unset RABBITMQ_FEATURE_FLAGS
    export RABBITMQ_SOME_VAR="$(printf 'line one\nRABBITMQ_FEATURE_FLAGS=forged')"
    echo 'RABBITMQ_FEATURE_FLAGS=legit_operator_value' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected RABBITMQ_FEATURE_FLAGS to be 'legit_operator_value', but got: $RABBITMQ_FEATURE_FLAGS"
    [ "$RABBITMQ_FEATURE_FLAGS" = "legit_operator_value" ]
}
