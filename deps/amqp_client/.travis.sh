#!/usr/bin/env bash

set -o nounset
set -o errexit

declare -r script_arg="${1:-unset}"

# GNU Make build variables
declare -r make_install_dir="$HOME/gmake"
declare -r make_bin_dir="$make_install_dir/bin"

# OTP build variables
declare -r build_status="$(mktemp)"
declare -r otp_tag_name="$script_arg"
declare -r otp_build_log_dir="$HOME/.kerl/builds/$otp_tag_name"
declare -r otp_install_dir="$HOME/otp/$otp_tag_name"

function onexit
{
    rm -vf "$build_status"
}

trap onexit EXIT

function build_ticker
{
    local status="$(< $build_status)"
    while [[ $status == 'true' ]]
    do
        echo '------------------------------------------------------------------------------------------------------------------------------------------------'
        echo "$(date) building $otp_tag_name ..."
        if ls $otp_build_log_dir/otp_build*.log > /dev/null
        then
            tail $otp_build_log_dir/otp_build*.log
        fi
        sleep 10
        status="$(< $build_status)"
    done
    echo '.'
}

function kiex_cleanup
{
    rm -vf $HOME/.kiex/bin/*.bak*
    rm -vf $HOME/.kiex/elixirs/.*.old
    rm -vf $HOME/.kiex/elixirs/*.old
    rm -vf $HOME/.kiex/scripts/*.bak*
}

if [[ $script_arg == 'kiex_cleanup' ]]
then
    kiex_cleanup
    exit 0
fi

set +o errexit
mkdir "$HOME/otp"
mkdir "$HOME/bin"
set -o errexit

curl -Lo "$HOME/bin/kerl"  https://raw.githubusercontent.com/kerl/kerl/master/kerl
chmod 755 "$HOME/bin/kerl"
export PATH="$HOME/bin:$PATH"

curl -sSL https://raw.githubusercontent.com/taylor/kiex/master/install | /usr/bin/env bash -s
declare -r kiex_script="$HOME/.kiex/scripts/kiex"
if [[ -s $kiex_script ]]
then
    source "$kiex_script"
    kiex list known
    kiex_cleanup
else
    echo "Did not find kiex at $kiex_script" 1>&2
    exit 1
fi

if [[ -s $make_bin_dir/make ]]
then
    echo "Found GNU Make installation at $make_install_dir"
    export PATH="$make_bin_dir:$PATH"
else
    mkdir -p "$make_install_dir"
    curl -sLO http://ftp.gnu.org/gnu/make/make-4.2.1.tar.gz
    tar xf make-4.2.1.tar.gz
    pushd make-4.2.1
    ./configure --prefix="$make_install_dir"
    make
    make install
    export PATH="$make_bin_dir:$PATH"
    popd
fi

if [[ -s $otp_install_dir/activate ]]
then
    echo "Found OTP installation at $otp_install_dir"
else
    export KERL_CONFIGURE_OPTIONS='--enable-hipe --enable-smp-support --enable-threads --enable-kernel-poll'
    rm -rf "$otp_install_dir"
    mkdir -p "$otp_install_dir"

    echo -n 'true' > "$build_status"
    build_ticker &
    kerl build git https://github.com/erlang/otp.git "$otp_tag_name" "$otp_tag_name"
    echo -n 'false' > "$build_status"
    wait

    kerl install "$otp_tag_name" "$otp_install_dir"
fi

exit 0
