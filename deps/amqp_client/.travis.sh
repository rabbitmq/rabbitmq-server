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

    ensure_directories
    ensure_kerl
    ensure_kiex
    ensure_make
    ensure_otp
}

function kiex_cleanup
{
    rm -vf $HOME/.kiex/bin/*.bak*
    rm -vf $HOME/.kiex/elixirs/.*.old
    rm -vf $HOME/.kiex/elixirs/*.old
    rm -vf $HOME/.kiex/scripts/*.bak*

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
    local status="$(< $tmp_file)"
    while [[ $status == 'true' ]]
    do
        echo '------------------------------------------------------------------------------------------------------------------------------------------------'
        echo "$(date) building $otp_tag_name ..."
        if ls $otp_build_log_dir/otp_build*.log > /dev/null
        then
            tail $otp_build_log_dir/otp_build*.log
        fi
        sleep 10
        status="$(< $tmp_file)"
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

main
