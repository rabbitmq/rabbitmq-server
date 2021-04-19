#!/bin/sh
# vim:sw=2:et:

set -ex

# Execute ourselves as root if we are an unprivileged user.
if test "$(id -u)" != '0'; then
  exec sudo -i "$0" "$@"
fi

HOME=/root
export HOME

DEBIAN_FRONTEND=noninteractive
export DEBIAN_FRONTEND

# shellcheck disable=SC2016
readonly erlang_version='${erlang_version}'
# shellcheck disable=SC2016
erlang_git_ref='${erlang_git_ref}'
# shellcheck disable=SC2016
readonly elixir_version='${elixir_version}'
# shellcheck disable=SC2016
readonly erlang_nodename='${erlang_nodename}'
# shellcheck disable=SC2016
readonly default_user='${default_user}'
# shellcheck disable=SC2016
readonly dirs_archive_url='${dirs_archive_url}'
# shellcheck disable=SC2016
readonly distribution='${distribution}'
# shellcheck disable=SC2016
readonly erlang_cookie='${erlang_cookie}'

readonly debian_codename="$${distribution#debian-*}"

case "$erlang_version" in
  24.*)
    if test -z "$erlang_git_ref"; then
      erlang_git_ref='master'
    fi
    ;;
  23.*|22.*|21.*|20.*|19.3)
    readonly erlang_package_version="1:$erlang_version-1"
    ;;
  R16B03)
    readonly erlang_package_version='1:16.b.3-3'
    ;;
  *)
    echo "[ERROR] unknown erlang version: $erlang_version" 1>&2
    exit 69 # EX_UNAVAILABLE; see sysexists(3)
    ;;
esac

install_essentials() {
  apt-get -qq update
  apt-get -qq install wget curl gnupg
}

setup_backports() {
  # Enable backports.
  cat >/etc/apt/sources.list.d/backports.list << EOF
deb http://cdn-fastly.deb.debian.org/debian $debian_codename-backports main
EOF
  apt-get -qq update
}

# --------------------------------------------------------------------
# Functions to take Erlang and Elixir from Debian packages.
# --------------------------------------------------------------------

determine_version_to_pin() {
  package=$1
  min_version=$2

  apt-cache policy "$package" | \
    awk '
BEGIN {
  version_to_pin = "";
}
/^ (   |\*\*\*) [^ ]/ {
  if ($1 == "***") {
    version = $2;
  } else {
    version = $1;
  }

  if (version_to_pin) {
    exit;
  } else if (match(version, /^'$min_version'([-.]|$)/)) {
    version_to_pin = version;
  }
}
END {
  if (version_to_pin) {
    print version_to_pin;
    exit;
  } else {
    exit 1;
  }
}'
}

setup_erlang_deb_repository() {
  # Setup repository to get Erlang.
  wget -O- https://www.rabbitmq.com/rabbitmq-release-signing-key.asc | apt-key add -
  wget -O- https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/gpg.E495BB49CC4BBE5B.key | apt-key add -
  cat >/etc/apt/sources.list.d/rabbitmq-erlang.list <<EOF
deb https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/deb/debian buster main
EOF

  # Configure Erlang version pinning.
  cat >/etc/apt/preferences.d/erlang <<EOF
Package: erlang*
Pin: version $erlang_package_version
Pin-Priority: 1000
EOF

  apt-get -qq install -y --no-install-recommends apt-transport-https
  apt-get -qq update
}

apt_install_erlang() {
  apt-get -qq install -y --no-install-recommends \
    erlang-base erlang-nox erlang-dev erlang-src erlang-common-test
}

apt_install_elixir() {
  if test "$elixir_version"; then
    # Configure Elixir version pinning.
    elixir_package_version=$(determine_version_to_pin elixir "$elixir_version")

    cat >/etc/apt/preferences.d/elixir <<EOF
Package: elixir
Pin: version $elixir_package_version
Pin-Priority: 1000
EOF
  fi

  apt-get -qq install -y --no-install-recommends elixir
}

apt_install_extra() {
  readonly extra_pkgs='git make rsync vim-nox xz-utils zip'
  readonly extra_backports=''

  # shellcheck disable=SC2086
  test -z "$extra_pkgs" || \
    apt-get -qq install -y --no-install-recommends \
    $extra_pkgs

  # shellcheck disable=SC2086
  test -e "$extra_backports" || \
    apt-get -qq install -y -V --fix-missing --no-install-recommends \
    -t "$debian_codename"-backports \
    $extra_backports
}

# --------------------------------------------------------------------
# Functions to build Erlang and Elixir from sources.
# --------------------------------------------------------------------

install_kerl() {
  apt-get -qq install -y --no-install-recommends \
    curl

  mkdir -p /usr/local/bin
  cd /usr/local/bin
  curl -O https://raw.githubusercontent.com/kerl/kerl/master/kerl
  chmod a+x kerl
}

kerl_install_erlang() {
  apt-get -qq install -y --no-install-recommends \
    git \
    build-essential \
    autoconf automake libtool \
    libssl-dev \
    libncurses5-dev \
    libsctp1 libsctp-dev

  kerl build git https://github.com/erlang/otp.git "$erlang_git_ref" "$erlang_version"
  kerl install "$erlang_version" /usr/local/erlang

  . /usr/local/erlang/activate
  echo '. /usr/local/erlang/activate' > /etc/profile.d/erlang.sh
}

install_kiex() {
  curl -sSL https://raw.githubusercontent.com/taylor/kiex/master/install | bash -s

  mv "$HOME/.kiex" /usr/local/kiex
  sed -E \
    -e 's,\\\$HOME/\.kiex,/usr/local/kiex,' \
    -e 's,\$HOME/\.kiex,/usr/local/kiex,' \
    < /usr/local/kiex/bin/kiex \
    > /usr/local/kiex/bin/kiex.patched
  mv /usr/local/kiex/bin/kiex.patched /usr/local/kiex/bin/kiex
  chmod a+x /usr/local/kiex/bin/kiex
}

kiex_install_elixir() {
  case "$erlang_version" in
    22.*|23.*|24.*)
      url="https://github.com/elixir-lang/elixir/releases/download/v$elixir_version/Precompiled.zip"
      wget -q -O/tmp/elixir.zip "$url"

      apt-get -qq install -y --no-install-recommends unzip

      mkdir -p /usr/local/elixir
      (cd /usr/local/elixir && unzip -q /tmp/elixir.zip)
      export PATH=/usr/local/elixir/bin:$PATH
      ;;
    *)
      export PATH=/usr/local/kiex/bin:$PATH
      latest_elixir_version=$(kiex list releases | tail -n 1 | awk '{print $1}')
      kiex install $latest_elixir_version

      . /usr/local/kiex/elixirs/elixir-$latest_elixir_version.env
      cat >> /etc/profile.d/erlang.sh <<EOF

. /usr/local/kiex/elixirs/elixir-$latest_elixir_version.env
EOF
      ;;
  esac
}

# --------------------------------------------------------------------
# Main.
# --------------------------------------------------------------------

install_essentials
setup_backports

# Install Erlang + various tools.
if test "$erlang_package_version"; then
  setup_erlang_deb_repository
  apt_install_erlang
  apt_install_elixir
elif test "$erlang_git_ref"; then
  install_kerl
  kerl_install_erlang
  install_kiex
  kiex_install_elixir
fi

apt_install_extra

# Store Erlang cookie file for both root and the default user.
for file in ~/.erlang.cookie "/home/$default_user/.erlang.cookie"; do
  echo "$erlang_cookie" > "$file"
  chmod 400 "$file"
done
chown "$default_user" "/home/$default_user/.erlang.cookie"

# Fetch and extract the dirs archive.
dirs_archive="/tmp/$(basename "$dirs_archive_url")"
wget -q -O"$dirs_archive" "$dirs_archive_url"
if test -s "$dirs_archive"; then
  xzcat "$dirs_archive" | tar xf - -P
fi
rm -f "$dirs_archive"

# Start an Erlang node to control the VM from Erlang.
erl -noinput -sname "$erlang_nodename" -hidden -detached
