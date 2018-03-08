#!/bin/sh
# vim:sw=2:et:

set -ex

# Execute ourselves as root if we are an unprivileged user.
if test "$(id -u)" != '0'; then
  exec sudo -i "$0" "$@"
fi

HOME=/root
export HOME

erlang_package_version='1:16.b.3-3'

# shellcheck disable=SC2016
erlang_nodename='${erlang_nodename}'
# shellcheck disable=SC2016
default_user='${default_user}'
# shellcheck disable=SC2016
dirs_archive_url='${dirs_archive_url}'
# shellcheck disable=SC2016
erlang_cookie='${erlang_cookie}'

# Enable backports.
cat >/etc/apt/sources.list.d/backports.list << EOF
deb http://httpredir.debian.org/debian wheezy-backports main
EOF

# Setup repository to get Erlang.
esl_package=/tmp/erlang-solutions_1.0_all.deb
wget -O"$esl_package" https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
dpkg -i "$esl_package"
apt-get -qq update

# Configure Erlang version pinning.
cat >/etc/apt/preferences.d/erlang <<EOF
Package: erlang*
Pin: version $erlang_package_version
Pin-Priority: 1000
EOF

# Install Erlang + various tools.
apt-get -qq install -y --no-install-recommends \
  erlang-base-hipe \
  erlang-nox \
  erlang-dev \
  erlang-src \
  erlang-common-test \
  make \
  rsync \
  vim-nox \
  zip

apt-get install -y -V --fix-missing --no-install-recommends \
  -t wheezy-backports \
  git

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
