#!/usr/bin/env bash

set -o nounset
set -o errexit

declare -r tmp_file="$(mktemp)"
declare -r script_arg="${1:-unset}"

function onexit
{
    rm -vf "$tmp_file"
}

trap onexit EXIT

function main
{
    # Note: if script_arg is kiex_cleanup,
    # this function exits early
    kiex_cleanup

    # Note: if script_arg is tests,
    # this function exits early
    maybe_run_tests "$@"

    ensure_directories
    ensure_kerl
    ensure_kiex
    ensure_make
    ensure_otp
}

function test_group_0
{
    make ct-backing_queue
    make ct-channel_interceptor
    make ct-channel_operation_timeout
    make ct-cluster_formation_locking
}

function test_group_1
{
    make ct-clustering_management
    make ct-cluster_rename
    make ct-cluster
    make ct-config_schema
}

function test_group_2
{
    make ct-crashing_queues
    make ct-credential_validation
    make ct-disconnect_detected_during_alarm
    make ct-dynamic_ha
}

function test_group_3
{
    make ct-eager_sync
    make ct-gm
    make ct-health_check
    make ct-lazy_queue
}

function test_group_4
{
    make ct-list_consumers_sanity_check
    make ct-list_queues_online_and_offline
    make ct-many_node_ha
    make ct-metrics
}

function test_group_5
{
    make ct-mirrored_supervisor
    make ct-msg_store
    # TODO FUTURE HACK
    # This suite fails frequently on Travis CI
    # make ct-partitions
    make ct-peer_discovery_dns
}

function test_group_6
{
    make ct-per_user_connection_tracking
    make ct-per_vhost_connection_limit_partitions
    make ct-per_vhost_connection_limit
    make ct-per_vhost_msg_store
}

function test_group_7
{
    make ct-per_vhost_queue_limit
    make ct-plugin_versioning
    make ct-policy
    make ct-priority_queue_recovery
}

function test_group_8
{
    make ct-priority_queue
    make ct-proxy_protocol
    make ct-queue_master_location
    make ct-rabbit_core_metrics_gc
}

function test_group_9
{
    make ct-rabbitmqctl_integration
    make ct-rabbitmqctl_shutdown
    make ct-simple_ha
    make ct-sup_delayed_restart
}

function test_group_10
{
    make ct-sync_detection
    make ct-term_to_binary_compat_prop
    make ct-topic_permission
    make ct-unit_inbroker_non_parallel
}

function test_group_11
{
    make ct-unit_inbroker_parallel
    make ct-unit
    make ct-worker_pool
}

function maybe_run_tests
{
    if [[ $script_arg == 'tests' ]]
    then
        # Note: Travis env specifies test suite number
        local -ri group="${2:-999}"

        local -r test_func="test_group_$group"
        "$test_func"

        # Only doing tests, so early exit
        exit 0
    fi
}

function kiex_cleanup
{
    rm -vf "$HOME"/.kiex/bin/*.bak*
    rm -vf "$HOME"/.kiex/elixirs/.*.old
    rm -vf "$HOME"/.kiex/elixirs/*.old
    rm -vf "$HOME"/.kiex/scripts/*.bak*

    if [[ $script_arg == 'kiex_cleanup' ]]
    then
        # Only doing cleanup, so early exit
        exit 0
    fi
}


function ensure_directories
{
    set +o errexit
    mkdir "$HOME/otp"
    mkdir "$HOME/bin"
    set -o errexit
    export PATH="$HOME/bin:$PATH"
}

function ensure_kerl
{
    curl -Lo "$HOME/bin/kerl"  https://raw.githubusercontent.com/kerl/kerl/master/kerl
    chmod 755 "$HOME/bin/kerl"
}

function ensure_kiex
{
    curl -sSL https://raw.githubusercontent.com/taylor/kiex/master/install | /usr/bin/env bash -s
    local -r kiex_script="$HOME/.kiex/scripts/kiex"
    if [[ -s $kiex_script ]]
    then
        source "$kiex_script"
        # Note: this produces a lot of output but without running
        # "list known" first, kiex install ... sometimes fails
        kiex list known
        kiex_cleanup
    else
        echo "Did not find kiex at $kiex_script" 1>&2
        exit 1
    fi
}

function ensure_make
{
    # GNU Make build variables
    local -r make_install_dir="$HOME/gmake"
    local -r make_bin_dir="$make_install_dir/bin"

    export PATH="$make_bin_dir:$PATH"

    if [[ -x $make_bin_dir/make ]]
    then
        echo "Found GNU Make installation at $make_install_dir"
    else
        mkdir -p "$make_install_dir"
        curl -sLO http://ftp.gnu.org/gnu/make/make-4.2.1.tar.gz
        tar xf make-4.2.1.tar.gz
        pushd make-4.2.1
        ./configure --prefix="$make_install_dir"
        make
        make install
        popd
    fi
}

function build_ticker
{
    local status

    status=$(< "$tmp_file")
    while [[ $status == 'true' ]]
    do
        echo '------------------------------------------------------------------------------------------------------------------------------------------------'
        echo "$(date) building $otp_tag_name ..."
        if ls "$otp_build_log_dir"/otp_build*.log > /dev/null
        then
            tail "$otp_build_log_dir"/otp_build*.log
        fi
        sleep 10
        status=$(< "$tmp_file")
    done
    echo '.'
}

function ensure_otp
{
    # OTP build variables
    local -r otp_tag_name="$script_arg"
    local -r otp_build_log_dir="$HOME/.kerl/builds/$otp_tag_name"
    local -r otp_install_dir="$HOME/otp/$otp_tag_name"
    if [[ -s $otp_install_dir/activate ]]
    then
        echo "Found OTP installation at $otp_install_dir"
    else
        export KERL_CONFIGURE_OPTIONS='--enable-hipe --enable-smp-support --enable-threads --enable-kernel-poll'
        rm -rf "$otp_install_dir"
        mkdir -p "$otp_install_dir"

        echo -n 'true' > "$tmp_file"
        build_ticker &
        kerl build git https://github.com/erlang/otp.git "$otp_tag_name" "$otp_tag_name"
        echo -n 'false' > "$tmp_file"
        wait

        kerl install "$otp_tag_name" "$otp_install_dir"
    fi
}

main "$@"
