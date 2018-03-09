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
  20.[12])
    readonly erlang_package_version="1:$erlang_version-1"
    ;;
  19.3)
    readonly erlang_package_version='1:19.3-1'
    ;;
  R16B03)
    readonly erlang_package_version='1:16.b.3-3'
    ;;
  *)
    echo "[ERROR] unknown erlang version: $erlang_version" 1>&2
    exit 69 # EX_UNAVAILABLE; see sysexists(3)
    ;;
esac

# Enable backports.
cat >/etc/apt/sources.list.d/backports.list << EOF
deb http://httpredir.debian.org/debian $debian_codename-backports main
EOF

# Setup repository to get Erlang.
readonly esl_package=/tmp/erlang-solutions_1.0_all.deb
wget -O"$esl_package" https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
dpkg -i "$esl_package"
apt-get -qq update

# Configure Erlang version pinning.
cat >/etc/apt/preferences.d/erlang <<EOF
Package: erlang*
Pin: version $erlang_package_version
Pin-Priority: 1000
EOF

apt_install_erlang() {
  apt-get -qq install -y --no-install-recommends \
    erlang-base-hipe erlang-nox erlang-dev erlang-src erlang-common-test
}

apt_install_extra() {
  case "$debian_codename" in
    wheezy)
      # We don't install Elixir because we only use Debian Wheezy to
      # run Erlang R16B03. This version of Erlang is only useful for
      # RabbitMQ 3.6.x which doesn't depend on Elixir.
      readonly extra_pkgs='make rsync vim-nox zip'
      readonly extra_backports='git'
      ;;
    *)
      readonly extra_pkgs='elixir git make rsync vim-nox zip'
      readonly extra_backports=''
      ;;
  esac

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

# Install Erlang + various tools.
apt_install_erlang
apt_install_extra

# Store Erlang cookie file for both root and the default user.
for file in ~/.erlang.cookie "/home/$default_user/.erlang.cookie"; do
  echo "$erlang_cookie" > "$file"
  chmod 400 "$file"
done
chown "$default_user" "/home/$default_user/.erlang.cookie"

# Fetch and extract the dirs archive.
dirs_archive="/tmp/$(basename "$dirs_archive_url")"
wget -O"$dirs_archive" "$dirs_archive_url"
if test -s "$dirs_archive"; then
  xzcat "$dirs_archive" | tar xf - -P
fi
rm -f "$dirs_archive"

# Start an Erlang node to control the VM from Erlang.
erl -noinput -sname "$erlang_nodename" -hidden -detached
